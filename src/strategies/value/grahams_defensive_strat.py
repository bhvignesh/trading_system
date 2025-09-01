# trading_system/src/strategies/graham_defensive_strategy.py

import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class GrahamDefensiveStrategy(BaseStrategy):
    """
    Implements Benjamin Graham's Defensive Investor strategy using vectorized operations,
    multi-ticker support, and historical financial statement–driven price alignment.

    The strategy uses the following criteria:
      1. Adequate Size: Market Cap ≥ $2B.
      2. Strong Financial Condition: Current Ratio ≥ 2.
      3. Earnings Stability: At least 5 consecutive periods (years) of positive earnings.
      4. Consistent Dividend Record: Dividend payments are consistently recorded.
      5. Earnings Growth: At least 10% earnings growth over the period.
      6. Moderate P/E: P/E ratio ≤ 15.
      7. Moderate P/B: P/B ratio ≤ 1.5.

    The strategy pulls financial statement data (from the Balance Sheet, Income Statement,
    and Cash Flow) for all available published dates, then retrieves historical prices for these
    dates. Finally, it calculates the corresponding metrics and applies the Graham criteria.
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Graham Defensive Strategy.

        Args:
            db_config: Database configuration object.
            params (Optional[Dict]): A dictionary of strategy parameters.
                Defaults to:
                  {
                    'min_market_cap': 2e9,
                    'min_current_ratio': 2.0,
                    'min_years_positive_earnings': 5,
                    'min_earnings_growth': 0.1,
                    'max_pe_ratio': 15,
                    'max_pb_ratio': 1.5,
                    'min_criteria_met': 5
                  }
        """
        super().__init__(db_config, params)
        self.params = params or {
            'min_market_cap': 2e9,
            'min_current_ratio': 2.0,
            'min_years_positive_earnings': 5,
            'min_earnings_growth': 0.1,
            'max_pe_ratio': 15,
            'max_pb_ratio': 1.5,
            'min_criteria_met': 5
        }
        self.logger = logging.getLogger(self.__class__.__name__)

    def generate_signals(self,
                         tickers: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate historical trading signals for one or more tickers using vectorized operations.

        For each ticker, the method:
          1. Retrieves and aligns the financial statements (Balance Sheet, Income Statement,
             Cash Flow) based on their published dates.
          2. Obtains historical prices for those dates.
          3. Calculates key fundamental metrics (Market Cap, Current Ratio, P/E, P/B, Dividend Record).
          4. Applies the Graham criteria in a vectorized manner.
          5. Outputs a DataFrame with a trend of the ratios, criteria counts, and final signal score.

        Args:
            tickers (Union[str, List[str]]): A single ticker symbol or a list of ticker symbols.
            start_date (str, optional): Start date (YYYY-MM-DD) for signal generation (if applicable).
            end_date (str, optional): End date (YYYY-MM-DD) for signal generation (if applicable).
            initial_position (int): The starting position indicator (e.g., 0, 1, or -1). Not used in this strategy.
            latest_only (bool): If True, only the most recent signal (per ticker) will be returned.

        Returns:
            pd.DataFrame: A DataFrame with the following columns:
              [ticker, date, market_cap, current_ratio, pe_ratio, pb_ratio, dividend_record,
               criteria_met, signal, adequate_size, strong_financial, earnings_stability,
               dividend_record, earnings_growth, moderate_pe, moderate_pb]
        """
        tickers = [tickers] if isinstance(tickers, str) else tickers
        results = []

        for ticker in tickers:
            try:
                df = self._process_ticker(ticker)
                if not df.empty:
                    results.append(df)
            except Exception as e:
                self.logger.error(f"Error processing {ticker}: {str(e)}")

        final_df = pd.concat(results, axis=0) if results else pd.DataFrame()

        if latest_only and not final_df.empty:
            final_df = final_df.groupby('ticker', group_keys=False).last().reset_index()

        return final_df

    def _process_ticker(self, ticker: str) -> pd.DataFrame:
        """
        Process a single ticker by aligning its financial statements and historical prices,
        calculating financial metrics, and applying Graham criteria.

        Args:
            ticker (str): The ticker symbol.

        Returns:
            pd.DataFrame: A DataFrame containing the fundamental metrics, criteria counts, and signal.
        """
        # Retrieve and align financial statements (the union of dates is used with forward fill)
        financials = self._get_all_financials(ticker)
        if financials.empty:
            return pd.DataFrame()

        # Obtain historical prices for the period covering the financial statement dates
        from_date = financials.index.min().strftime('%Y-%m-%d')
        to_date = financials.index.max().strftime('%Y-%m-%d')
        prices = self.get_historical_prices(ticker, from_date=from_date, to_date=to_date)
        if prices.empty:
            return pd.DataFrame()

        # Merge financial data with historical price data using the financial statement dates
        merged = self._merge_financials_with_prices(financials, prices)
        if merged.empty:
            return pd.DataFrame()

        # Add explicit ticker column (since the financials index is date only)
        merged['ticker'] = ticker

        # Calculate key financial metrics (market_cap, current_ratio, pe_ratio, pb_ratio, dividend_record)
        metrics = self._calculate_metrics(merged)

        # Apply Graham Defensive criteria on the calculated metrics
        criteria = self._apply_graham_criteria(metrics)

        return criteria

    def _get_all_financials(self, ticker: str) -> pd.DataFrame:
        """
        Retrieve, align, and merge the financial statements (Balance Sheet, Income Statement,
        Cash Flow) for a given ticker using vectorized operations.

        The method performs the following:
          1. Retrieves each financial statement and adds a statement-specific suffix.
          2. Computes the union of all available dates from each statement.
          3. Reindexes each statement on the union of dates with forward fill to carry the last seen
             value until updated.
          4. Concatenates the reindexed statements along columns.

        Args:
            ticker (str): The ticker symbol.

        Returns:
            pd.DataFrame: A DataFrame indexed by date containing the merged financial statement data.
        """
        dfs = []

        for stmt_type in ['balance_sheet', 'income_statement', 'cash_flow']:
            stmt = self.get_financials(ticker, stmt_type, lookback=None)
            if not stmt.empty:
                # Append a suffix (e.g., _balance_sheet) to column names to avoid collisions
                stmt = stmt.add_suffix(f'_{stmt_type}')
                dfs.append(stmt)

        if not dfs:
            return pd.DataFrame()

        # Compute the union of all dates from the different statements
        all_dates = sorted(set().union(*(df.index for df in dfs)))

        # Reindex each statement to the same set of dates and forward fill to use the last available value
        dfs_filled = [df.reindex(all_dates).ffill() for df in dfs]
        combined = pd.concat(dfs_filled, axis=1)
        combined.index.name = 'date'
        return combined.sort_index()

    def _merge_financials_with_prices(self, financials: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Merge the financial statement data with historical price data.

        The method:
          1. Resamples the price data (daily frequency) and forward fills missing values.
          2. Merges the financial statements with the close price series using the financial dates.
          3. Forward fills any missing price values after the merge.

        Args:
            financials (pd.DataFrame): The DataFrame containing financial statement data.
            prices (pd.DataFrame): The DataFrame containing historical prices.

        Returns:
            pd.DataFrame: The merged DataFrame with a 'close' price column.
        """
        if prices.empty:
            return pd.DataFrame()

        # Get the daily close price and forward fill
        price_series = prices['close'].resample('D').ffill()
        merged = financials.merge(price_series, left_index=True, right_index=True, how='left')
        merged['close'] = merged['close'].ffill()
        return merged.dropna(subset=['close'])

    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate financial metrics needed for the strategy in a vectorized manner.

        The following metrics are computed:
          - Market Cap: Calculated as (ordinary_shares_number × close price), where
            'ordinary_shares_number_balance_sheet' is used as the measure of shares outstanding.
          - Current Ratio: Ratio of current assets to current liabilities from the balance sheet.
          - P/E Ratio: Market Cap divided by net income (from the income statement). Infinite
            values are replaced with NaN.
          - P/B Ratio: Market Cap divided by stockholders' equity (taken from 'stockholders_equity'
            in the balance sheet). Infinite values are replaced with NaN.
          - Dividend Record: A boolean flag indicating the presence of dividend payments
            (derived from the 'cash_dividends_paid' from the cash flow, assuming dividend payments appear as negative values).

        Args:
            df (pd.DataFrame): The merged DataFrame containing financials and price data.

        Returns:
            pd.DataFrame: The DataFrame with added columns: market_cap, current_ratio, pe_ratio, pb_ratio, dividend_record.
        """
        metrics = df.copy()

        # Use 'ordinary_shares_number_balance_sheet' as the shares outstanding measure for market cap.
        metrics['market_cap'] = metrics['ordinary_shares_number_balance_sheet'] * metrics['close']

        # Current ratio from balance sheet: current assets / current liabilities.
        metrics['current_ratio'] = metrics['current_assets_balance_sheet'] / metrics['current_liabilities_balance_sheet']

        # P/E ratio: market cap divided by net income from the income statement.
        metrics['pe_ratio'] = (metrics['market_cap'] / metrics['net_income_income_statement']).replace([np.inf, -np.inf], np.nan)

        # P/B ratio: market cap divided by stockholders' equity (from the balance sheet, using 'stockholders_equity').
        metrics['pb_ratio'] = (metrics['market_cap'] / metrics['stockholders_equity_balance_sheet']).replace([np.inf, -np.inf], np.nan)

        # Dividend record: True if dividends (from cash flow) are negative (i.e., paid out).
        metrics['dividend_record'] = metrics['cash_dividends_paid_cash_flow'] < 0

        return metrics

    def _apply_graham_criteria(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Benjamin Graham's Defensive Investor criteria in a vectorized fashion.

        The following criteria are applied:
          1. Adequate Size: market_cap ≥ min_market_cap.
          2. Strong Financial Condition: current_ratio ≥ min_current_ratio.
          3. Earnings Stability: a rolling window (over 'min_years_positive_earnings' periods)
             where all net incomes (from the income statement) are positive.
          4. Dividend Record: dividend_record is True.
          5. Earnings Growth: percentage change in net income (over 'min_years_positive_earnings' periods)
             is at least min_earnings_growth.
          6. Moderate P/E: pe_ratio falls between 0 and max_pe_ratio.
          7. Moderate P/B: pb_ratio falls between 0 and max_pb_ratio.

        The total number of criteria met is computed, and the final trading signal is set to 1 if
        the count meets or exceeds 'min_criteria_met'; otherwise, 0.

        Args:
            df (pd.DataFrame): DataFrame containing financial metrics.

        Returns:
            pd.DataFrame: DataFrame with columns for each criterion, the total criteria_met, and a binary signal.
        """
        criteria = df.copy()

        # 1. Adequate Size
        criteria['adequate_size'] = criteria['market_cap'] >= self.params['min_market_cap']

        # 2. Strong Financial Condition
        criteria['strong_financial'] = criteria['current_ratio'] >= self.params['min_current_ratio']

        # 3. Earnings Stability: check rolling window (min_years_positive_earnings periods) if all earnings > 0.
        criteria['earnings_stability'] = criteria['net_income_income_statement'].rolling(
            window=self.params['min_years_positive_earnings'], min_periods=1
        ).apply(lambda x: 1 if (x > 0).all() else 0, raw=False)

        # 4. Dividend Record is already computed as a boolean.

        # 5. Earnings Growth: percentage change over 'min_years_positive_earnings' periods.
        criteria['earnings_growth'] = criteria['net_income_income_statement'].pct_change(
            periods=self.params['min_years_positive_earnings']
        ) >= self.params['min_earnings_growth']

        # 6. Moderate P/E: pe_ratio between 0 and max_pe_ratio.
        criteria['moderate_pe'] = criteria['pe_ratio'].between(0, self.params['max_pe_ratio'])

        # 7. Moderate P/B: pb_ratio between 0 and max_pb_ratio.
        criteria['moderate_pb'] = criteria['pb_ratio'].between(0, self.params['max_pb_ratio'])

        # List of criteria flags for later aggregation
        criteria_list = [
            'adequate_size',
            'strong_financial',
            'earnings_stability',
            'dividend_record',
            'earnings_growth',
            'moderate_pe',
            'moderate_pb'
        ]

        # Count the number of criteria met for each record
        criteria['criteria_met'] = criteria[criteria_list].sum(axis=1)

        # Final signal: 1 if the count meets or exceeds the threshold; otherwise 0.
        criteria['signal'] = (criteria['criteria_met'] >= self.params['min_criteria_met']).astype(int)

        # Ensure the ticker is available; reset the index (date) so that ticker and date become individual columns.
        if 'ticker' not in criteria.columns:
            criteria['ticker'] = np.nan

        criteria = criteria.reset_index()

        # Define the final column order for output.
        columns_order = [
            'ticker', 'date', 'market_cap', 'current_ratio', 'pe_ratio',
            'pb_ratio', 'dividend_record', 'criteria_met', 'signal'
        ] + criteria_list

        return criteria[columns_order]