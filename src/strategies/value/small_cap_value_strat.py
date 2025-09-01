# trading_strategy/src/strategies/value/small_cap_value_strat.py

# TODO: Verify

"""
Module: small_cap_value_screener.py

Implementation of Joel Tillinghast’s Small-Cap Value Screener strategy.
The strategy targets small-cap stocks exhibiting:
  - Strong free cash flow,
  - Low debt-to-equity,
  - High insider ownership,
  - Undervaluation based on earnings and book value.

This module retrieves historical financial statements (balance sheet, income statement,
and cash flow) for each ticker, aligns the financial statement dates with daily closing
prices, and calculates key metrics. A composite score is generated using default thresholds
and weights (optimized for the Indian market by default) which can be used for ranking.

The strategy class inherits from BaseStrategy so that it conforms to the common interface.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class SmallCapValueScreenerStrategy(BaseStrategy):
    """
    Implementation of Joel Tillinghast's Small-Cap Value Screener strategy.

    This strategy screens small-cap stocks that demonstrate:
      - Strong free cash flow,
      - Low debt-to-equity ratio,
      - High insider ownership,
      - Undervaluation based on earnings and book value.

    It calculates the following metrics for each fundamental date:
      - Market Capitalization (using closing price and shares outstanding)
      - Free Cash Flow Yield = free_cash_flow / market_cap
      - Debt-to-Equity = total_debt / stockholders_equity
      - Price-to-Earnings (P/E) and Price-to-Book (P/B) ratios
      - Average Valuation = (P/E + P/B) / 2 (if earnings > 0; else uses P/B)
      - A final composite score computed as a weighted sum of:
            (free_cash_flow_yield - min_free_cash_flow_yield) +
            (max_debt_to_equity - debt_to_equity) +
            (insider_ownership - min_insider_ownership) +
            (max_average_valuation - average_valuation)
      (Higher composite scores indicate more attractive small-cap value candidates.)

    Attributes:
        params (dict): Strategy parameters with defaults optimized for the Indian market.
            Default keys include:
              - 'max_market_cap': Maximum market capitalization for a small-cap stock (default 5e9).
              - 'min_free_cash_flow_yield': Minimum acceptable FCF yield (default 0.05, i.e., 5%).
              - 'max_debt_to_equity': Maximum acceptable debt-to-equity ratio (default 1.0).
              - 'min_insider_ownership': Minimum required insider ownership (default 0.10 or 10%).
              - 'max_average_valuation': Maximum acceptable average valuation (default 15.0).
              - 'weight_fcf_yield': Weight for FCF yield difference in the final score (default 1.0).
              - 'weight_debt_to_equity': Weight for debt-to-equity difference (default 1.0).
              - 'weight_insider_ownership': Weight for insider ownership difference (default 1.0).
              - 'weight_valuation': Weight for average valuation difference (default 1.0).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Small-Cap Value Screener strategy.

        Args:
            db_config: Database configuration settings.
            params (dict, optional): Strategy parameter overrides.
        """
        super().__init__(db_config, params)
        # Default parameters optimized for the Indian market.
        self.params = params or {
            'max_market_cap': 5e9,              # Maximum market cap (e.g., 5 billion)
            'min_free_cash_flow_yield': 0.05,     # Minimum FCF yield (5%)
            'max_debt_to_equity': 1.0,            # Maximum acceptable debt-to-equity ratio
            'min_insider_ownership': 0.10,        # Minimum insider ownership (10%)
            'max_average_valuation': 15.0,        # Maximum average valuation (e.g., average of P/E and P/B)
            'weight_fcf_yield': 1.0,
            'weight_debt_to_equity': 1.0,
            'weight_insider_ownership': 1.0,
            'weight_valuation': 1.0,
        }

    def generate_signals(
        self,
        tickers: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate historical trading signals based on the Small-Cap Value Screener strategy.

        For each ticker, the method:
          1. Retrieves all available financial statement data (balance sheet, income statement,
             and cash flow).
          2. Merges the three statements on 'ticker' and 'date' (reporting date).
          3. Retrieves and aligns historical closing prices for the financial statement dates.
          4. Computes key metrics:
                - Market Capitalization (close price * ordinary_shares_number)
                - Free Cash Flow Yield (free_cash_flow / market_cap)
                - Debt-to-Equity (total_debt / stockholders_equity)
                - P/E, P/B ratios and an average valuation.
          5. Retrieves insider ownership from company_info and merges it by ticker.
          6. Filters small-cap stocks (market cap <= max_market_cap) and then computes a composite
             final score as described in the class docstring.
          7. Applies optional date range filtering or returns only the latest record per ticker.

        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Filter results from this date (YYYY-MM-DD).
            end_date (str, optional): Filter results up to this date (YYYY-MM-DD).
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent available signal per ticker.

        Returns:
            pd.DataFrame: DataFrame with columns including:
              ['ticker', 'date', 'market_cap', 'free_cash_flow', 'free_cash_flow_yield',
               'total_debt', 'stockholders_equity', 'debt_to_equity',
               'net_income', 'pe_ratio', 'pb_ratio', 'average_valuation',
               'insider_ownership', 'final_score']
        """
        try:
            # Retrieve all available financial statements data.
            bs = self.get_financials(tickers, 'balance_sheet', lookback=None)
            inc = self.get_financials(tickers, 'income_statement', lookback=None)
            cf = self.get_financials(tickers, 'cash_flow', lookback=None)

            # Validate data availability.
            if bs.empty or inc.empty or cf.empty:
                self.logger.warning("Incomplete financial statements for tickers: %s", tickers)
                return pd.DataFrame()

            # Prepare dataframes by resetting index (if they are MultiIndexed by 'ticker' and 'date').
            for df in [bs, inc, cf]:
                if isinstance(df.index, pd.MultiIndex):
                    df.reset_index(inplace=True)

            # Select only required columns from the financial statements.
            bs_required = ['ticker', 'date', 'total_debt', 'stockholders_equity', 'ordinary_shares_number']
            inc_required = ['ticker', 'date', 'net_income']
            cf_required = ['ticker', 'date', 'free_cash_flow']
            bs = bs[bs_required].copy()
            inc = inc[inc_required].copy()
            cf = cf[cf_required].copy()

            # Convert 'date' to datetime for proper merging.
            for df in (bs, inc, cf):
                df['date'] = pd.to_datetime(df['date'])

            # Merge the balance sheet and income statement first (inner join on ticker and date).
            merged = pd.merge(bs, inc, on=['ticker', 'date'], how='inner')
            # Then merge with the cash flow data.
            merged = pd.merge(merged, cf, on=['ticker', 'date'], how='inner')

            if merged.empty:
                self.logger.warning("No overlapping dates found among the financial statements for %s", tickers)
                return pd.DataFrame()

            # Retrieve insider ownership from company_info.
            # Note: company_info typically has one (most recent) row per ticker.
            try:
                comp_info = self.get_company_info(tickers)
            except DataRetrievalError as e:
                self.logger.warning("Company info retrieval error: %s", e)
                comp_info = pd.DataFrame()

            # If company_info was retrieved as a series for a single ticker, convert it to DataFrame.
            if isinstance(comp_info, pd.Series):
                comp_info = comp_info.to_frame().T
            if not comp_info.empty and 'heldpercentinsiders' in comp_info.columns:
                # Keep only the required column.
                insider = comp_info[['ticker', 'heldpercentinsiders']].copy()
            else:
                self.logger.warning("Insider ownership data not found in company_info; setting to NaN")
                insider = pd.DataFrame({'ticker': merged['ticker'].unique(), 'heldpercentinsiders': np.nan})
            # Merge insider ownership into merged dataframe on ticker.
            merged = pd.merge(merged, insider, on='ticker', how='left')
            # Rename for clarity.
            merged.rename(columns={'heldpercentinsiders': 'insider_ownership'}, inplace=True)

            # Retrieve and align price data to the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Historical price data retrieval failed.")
                return pd.DataFrame()

            # Merge the price data with the fundamentals.
            # The merge is performed as of (backward fill) the financial statement date.
            merged.sort_values(['ticker', 'date'], inplace=True)
            price_data.sort_values(['ticker', 'date'], inplace=True)
            aligned = pd.merge_asof(
                merged.sort_values('date'),
                price_data[['ticker', 'date', 'close']],
                on='date',
                by='ticker',
                direction='backward'
            )

            # Calculate market capitalization: closing price multiplied by shares outstanding.
            aligned['market_cap'] = aligned['close'] * aligned['ordinary_shares_number']

            # Compute free cash flow yield: free_cash_flow / market_cap
            aligned['free_cash_flow_yield'] = aligned['free_cash_flow'] / aligned['market_cap']

            # Compute debt-to-equity: total_debt / stockholders_equity
            aligned['debt_to_equity'] = np.where(
                aligned['stockholders_equity'] != 0,
                aligned['total_debt'] / aligned['stockholders_equity'],
                np.nan
            )

            # Compute valuation metrics.
            # Price-to-Book: market_cap / stockholders_equity.
            aligned['pb_ratio'] = np.where(
                aligned['stockholders_equity'] != 0,
                aligned['market_cap'] / aligned['stockholders_equity'],
                np.nan
            )
            # Price-to-Earnings: market_cap / net_income (if net_income > 0).
            aligned['pe_ratio'] = np.where(
                aligned['net_income'] > 0,
                aligned['market_cap'] / aligned['net_income'],
                np.nan
            )
            # Average valuation: (pe_ratio + pb_ratio)/2 if pe_ratio exists; else use pb_ratio.
            aligned['average_valuation'] = np.where(
                aligned['pe_ratio'].notnull(),
                (aligned['pe_ratio'] + aligned['pb_ratio']) / 2,
                aligned['pb_ratio']
            )

            # Filter to include only small-cap stocks (market cap <= max_market_cap).
            aligned = aligned[aligned['market_cap'] <= self.params['max_market_cap']].copy()

            # --- Compute the final composite score ---
            # Score is based on:
            #   free cash flow yield above minimum,
            #   lower debt-to-equity (the lower, the better),
            #   higher insider ownership than the minimum,
            #   and lower average valuation than the maximum.
            #
            # The formula used (each term weighted by a parameter) is:
            #     final_score = (weight_fcf_yield * (free_cash_flow_yield - min_free_cash_flow_yield)) +
            #                   (weight_debt_to_equity * (max_debt_to_equity - debt_to_equity)) +
            #                   (weight_insider_ownership * (insider_ownership - min_insider_ownership)) +
            #                   (weight_valuation * (max_average_valuation - average_valuation))
            #
            # A higher final_score indicates a more attractive candidate.
            aligned['final_score'] = (
                self.params['weight_fcf_yield'] *
                (aligned['free_cash_flow_yield'] - self.params['min_free_cash_flow_yield'])
                +
                self.params['weight_debt_to_equity'] *
                (self.params['max_debt_to_equity'] - aligned['debt_to_equity'])
                +
                self.params['weight_insider_ownership'] *
                (aligned['insider_ownership'] - self.params['min_insider_ownership'])
                +
                self.params['weight_valuation'] *
                (self.params['max_average_valuation'] - aligned['average_valuation'])
            )

            # Apply date range filtering if provided.
            if start_date:
                start_dt = pd.to_datetime(start_date)
                aligned = aligned[aligned['date'] >= start_dt]
            if end_date:
                end_dt = pd.to_datetime(end_date)
                aligned = aligned[aligned['date'] <= end_dt]

            # If only the latest signal is required, then pick the most recent record per ticker.
            if latest_only:
                aligned = aligned.sort_values('date').groupby('ticker').tail(1)

            # Select and order the desired columns.
            result = aligned[
                ['ticker', 'date', 'market_cap', 'free_cash_flow', 'free_cash_flow_yield',
                 'total_debt', 'stockholders_equity', 'debt_to_equity', 'net_income',
                 'pe_ratio', 'pb_ratio', 'average_valuation', 'insider_ownership', 'final_score']
            ].copy()

            return result.reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _get_price_data(self, fundamentals_df: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for the reporting dates provided in fundamentals_df.

        This method uses the base strategy's get_historical_prices to pull price data over the range
        of financial statement dates, and then uses merge_asof to align each statement date with its
        most recent available closing price (backward fill).

        Args:
            fundamentals_df (pd.DataFrame): DataFrame containing at least a 'date' column.
            tickers (Union[str, List[str]]): The ticker or list of tickers.

        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'] containing the aligned prices.
        """
        if fundamentals_df.empty:
            return pd.DataFrame()

        # Determine date range from the fundamental data.
        min_date = fundamentals_df['date'].min().strftime('%Y-%m-%d')
        max_date = fundamentals_df['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data for the specified tickers and date range.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        # Reset index so that 'ticker' and 'date' are available as columns.
        price_df = price_df.reset_index() if 'ticker' not in price_df.columns else price_df.reset_index(drop=True)
        # In the single ticker case, ensure 'ticker' column is present.
        if 'ticker' not in price_df.columns:
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

        # Ensure the 'date' column is in datetime format.
        if not np.issubdtype(price_df['date'].dtype, np.datetime64):
            price_df['date'] = pd.to_datetime(price_df['date'])

        return price_df