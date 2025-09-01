# trading_system/src/strategies/value/qarp_strategy.py

# TODO: Verify

"""
Implementation of Buffett's Quality at a Reasonable Price (QARP) strategy.
This strategy identifies companies with the following characteristics:
  - High Return on Equity (ROE)
  - High Free Cash Flow (FCF) yield
  - Low Debt-to-Equity ratio
  - A reasonable valuation based on P/E and/or P/B ratio

The strategy uses historical financial statement data (from income_statement,
balance_sheet, and cash_flow tables) and aligns these with historical price data.
It then computes a composite score that can be used to rank or rank the tickers.

Default parameter values (optimized for the Indian market):
  - min_roe:              0.15  (15% minimum return on equity)
  - min_fcf_yield:        0.05  (5% minimum free cash flow yield)
  - max_debt_to_equity:   0.5   (maximum acceptable debt-to-equity ratio)
  - max_pe:               25    (maximum acceptable P/E ratio)
  - max_pb:               3     (maximum acceptable P/B ratio)
  - Weights for composite score: all set to 1.0 by default

The final composite score is calculated from normalized components:
  final_score = weight_roe * ((roe - min_roe) / min_roe)
              + weight_fcf * ((fcf_yield - min_fcf_yield) / min_fcf_yield)
              + weight_debt * ((max_debt_to_equity - debt_to_equity) / max_debt_to_equity)
              + weight_val * max( (max_pe - pe_ratio)/max_pe, (max_pb - pb_ratio)/max_pb )

A higher final score indicates a more attractive opportunity under the QARP criteria.
"""

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy


class BuffettsQualityAtReasonablePriceStrategy(BaseStrategy):
    """
    Implementation of Buffett's Quality at a Reasonable Price (QARP) strategy.

    This strategy screens companies based on:
      - High Return on Equity (ROE)
      - High Free Cash Flow (FCF) yield
      - Low Debt-to-Equity ratio
      - A reasonable valuation via low P/E or P/B ratios

    The strategy retrieves historical financial statement data (from income_statement,
    balance_sheet, and cash_flow tables), aligns these with historical daily closing prices,
    and computes a final composite score for each date. The final score can be used for ranking
    the tickers or generating a trading signal.

    Default parameters (optimized for the Indian market, NSE/BSE):
        min_roe:              0.15
        min_fcf_yield:        0.05
        max_debt_to_equity:   0.5
        max_pe:               25
        max_pb:               3
        weight_roe:           1.0
        weight_fcf:           1.0
        weight_debt:          1.0
        weight_val:           1.0
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Buffett's QARP strategy.

        Args:
            db_config: Database configuration settings.
            params (dict, optional): Strategy-specific parameter overrides.
        """
        super().__init__(db_config, params)
        default_params = {
            'min_roe': 0.15,
            'min_fcf_yield': 0.05,
            'max_debt_to_equity': 0.5,
            'max_pe': 25,
            'max_pb': 3,
            'weight_roe': 1.0,
            'weight_fcf': 1.0,
            'weight_debt': 1.0,
            'weight_val': 1.0,
        }
        default_params.update(params or {})
        self.params = default_params

    def generate_signals(
        self,
        tickers: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate historical trading signals based on the QARP strategy.

        Retrieves financial statement data for the tickers, aligns the data with historical
        closing prices (using financial statement dates), computes key metrics and a final
        composite score.

        Args:
            tickers (Union[str, List[str]]): A single ticker or a list of tickers.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): Not used in this strategy.
            latest_only (bool): If True, returns only the most recent signal per ticker.

        Returns:
            pd.DataFrame: DataFrame with columns:
              ['ticker', 'date', 'roe', 'fcf_yield', 'debt_to_equity', 'pe_ratio',
               'pb_ratio', 'final_score']
        """
        try:
            # Retrieve complete financial data for income statement, balance sheet, and cash flow.
            income_stmt = self.get_financials(tickers, 'income_statement', lookback=None)
            balance_sheet = self.get_financials(tickers, 'balance_sheet', lookback=None)
            cash_flow = self.get_financials(tickers, 'cash_flow', lookback=None)

            # Validate that we have sufficient data.
            if income_stmt.empty or balance_sheet.empty or cash_flow.empty:
                self.logger.warning(f"Incomplete financial data for tickers: {tickers}")
                return pd.DataFrame()

            # Merge financial statement data.
            merged = self._merge_financials(income_stmt, balance_sheet, cash_flow)
            if merged.empty:
                return pd.DataFrame()

            # Retrieve and align closing price data for financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed.")
                return pd.DataFrame()

            # Calculate key metrics and composite QARP score.
            results = self._calculate_metrics(merged, price_data)
            if results.empty:
                return pd.DataFrame()

            # Apply date range filtering if provided.
            if start_date:
                results = results[results['date'] >= pd.to_datetime(start_date)]
            if end_date:
                results = results[results['date'] <= pd.to_datetime(end_date)]

            # If latest_only flag is set, take the most recent record for each ticker.
            if latest_only:
                results = results.sort_values('date').groupby('ticker').tail(1)

            return results.reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _merge_financials(
        self,
        income_stmt: pd.DataFrame,
        balance_sheet: pd.DataFrame,
        cash_flow: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge income statement, balance sheet, and cash flow data on ticker and date.

        For income statement, the column 'net_income_common_stockholders' is used.
        For balance sheet, the columns 'common_stock_equity', 'total_debt' and 'ordinary_shares_number'
        are required, and for cash flow, 'free_cash_flow' is used.

        Args:
            income_stmt (pd.DataFrame): Income statement data.
            balance_sheet (pd.DataFrame): Balance sheet data.
            cash_flow (pd.DataFrame): Cash flow data.

        Returns:
            pd.DataFrame: Merged DataFrame containing the financial data.
        """
        # Ensure each DataFrame has explicit 'ticker' and 'date' columns and convert 'date' to datetime.
        for df in [income_stmt, balance_sheet, cash_flow]:
            if not isinstance(df.index, pd.MultiIndex):
                df.reset_index(inplace=True)
            if 'ticker' not in df.columns or df['ticker'].isnull().all():
                df['ticker'] = 'N/A'
            df['date'] = pd.to_datetime(df['date'])

        # Merge income statement with balance sheet.
        merged = pd.merge(
            income_stmt[['ticker', 'date', 'net_income_common_stockholders']],
            balance_sheet[['ticker', 'date', 'common_stock_equity', 'total_debt', 'ordinary_shares_number']],
            on=['ticker', 'date'],
            how='inner'
        )
        # Merge the result with cash flow.
        merged = pd.merge(
            merged,
            cash_flow[['ticker', 'date', 'free_cash_flow']],
            on=['ticker', 'date'],
            how='inner'
        )

        return merged

    def _get_price_data(
        self,
        merged_data: pd.DataFrame,
        tickers: Union[str, List[str]]
    ) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for financial statement dates.

        Uses the base strategy's get_historical_prices to pull price data over the range
        of financial statement dates and uses merge_asof to align each financial statement date
        to the most recent available closing price.

        Args:
            merged_data (pd.DataFrame): Merged financial data with 'ticker' and 'date'.
            tickers (Union[str, List[str]]): Single ticker or list of tickers.

        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'].
        """
        if merged_data.empty:
            return pd.DataFrame()

        # Determine the minimum and maximum dates from the merged financial data.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data using the base strategy method.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

        if 'date' not in price_df.columns and 'Date' in price_df.columns:
            price_df.rename(columns={'Date': 'date'}, inplace=True)
        price_df['date'] = pd.to_datetime(price_df['date'])
        merged_data['date'] = pd.to_datetime(merged_data['date'])

        # Sort data for merge_asof to work correctly.
        price_df.sort_values(['ticker', 'date'], inplace=True)
        merged_data.sort_values(['ticker', 'date'], inplace=True)

        # Merge asof: for each financial statement date, find the latest available closing price.
        aligned = pd.merge_asof(
            merged_data,
            price_df[['ticker', 'date', 'close']],
            on='date',
            by='ticker',
            direction='backward'
        )
        return aligned[['ticker', 'date', 'close']]

    def _calculate_metrics(
        self,
        merged_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate key metrics and the composite QARP score.

        The following metrics are computed:
          - Market Cap = ordinary_shares_number * close
          - ROE = net_income_common_stockholders / common_stock_equity
          - FCF Yield = free_cash_flow / market_cap
          - Debt-to-Equity = total_debt / common_stock_equity
          - EPS = net_income_common_stockholders / ordinary_shares_number
          - Book Value Per Share (BVPS) = common_stock_equity / ordinary_shares_number
          - P/E Ratio = close / EPS
          - P/B Ratio = close / BVPS

        The final score is the weighted sum of normalized components:
            final_score = weight_roe * ((roe - min_roe) / min_roe)
                        + weight_fcf * ((fcf_yield - min_fcf_yield) / min_fcf_yield)
                        + weight_debt * ((max_debt_to_equity - debt_to_equity) / max_debt_to_equity)
                        + weight_val * max( (max_pe - pe_ratio)/max_pe, (max_pb - pb_ratio)/max_pb )

        Args:
            merged_data (pd.DataFrame): Merged financial data.
            price_data (pd.DataFrame): Aligned price data with closing prices.

        Returns:
            pd.DataFrame: DataFrame with columns:
              ['ticker', 'date', 'roe', 'fcf_yield', 'debt_to_equity', 'pe_ratio',
               'pb_ratio', 'final_score']
        """
        # Merge aligned price data with the financials.
        data = pd.merge(merged_data, price_data, on=['ticker', 'date'], how='left')
        if data['close'].isnull().all():
            self.logger.warning("Price data alignment failed: no matching close prices found.")
            return pd.DataFrame()

        # Calculate Market Cap.
        data['market_cap'] = data['ordinary_shares_number'] * data['close']

        # Compute ROE (Return on Equity).
        data['roe'] = data['net_income_common_stockholders'] / data['common_stock_equity']

        # Compute FCF Yield.
        data['fcf_yield'] = data['free_cash_flow'] / data['market_cap']

        # Compute Debt-to-Equity ratio.
        data['debt_to_equity'] = data['total_debt'] / data['common_stock_equity']

        # Compute EPS and Book Value Per Share.
        data['eps'] = data['net_income_common_stockholders'] / data['ordinary_shares_number'].replace(0, np.nan)
        data['bvps'] = data['common_stock_equity'] / data['ordinary_shares_number'].replace(0, np.nan)

        # Calculate P/E and P/B ratios.
        data['pe_ratio'] = data['close'] / data['eps']
        data['pb_ratio'] = data['close'] / data['bvps']

        # Retrieve threshold parameters.
        min_roe = self.params['min_roe']
        min_fcf_yield = self.params['min_fcf_yield']
        max_debt_to_equity = self.params['max_debt_to_equity']
        max_pe = self.params['max_pe']
        max_pb = self.params['max_pb']

        # Compute normalized components.
        data['roe_component'] = (data['roe'] - min_roe) / min_roe
        data['fcf_component'] = (data['fcf_yield'] - min_fcf_yield) / min_fcf_yield
        data['debt_component'] = (max_debt_to_equity - data['debt_to_equity']) / max_debt_to_equity
        data['pe_component'] = (max_pe - data['pe_ratio']) / max_pe
        data['pb_component'] = (max_pb - data['pb_ratio']) / max_pb
        data['val_component'] = data[['pe_component', 'pb_component']].max(axis=1)

        # Compute final composite QARP score.
        final_score = (
            self.params['weight_roe'] * data['roe_component'] +
            self.params['weight_fcf'] * data['fcf_component'] +
            self.params['weight_debt'] * data['debt_component'] +
            self.params['weight_val'] * data['val_component']
        )
        data['final_score'] = final_score

        return data[['ticker', 'date', 'roe', 'fcf_yield', 'debt_to_equity', 'pe_ratio', 'pb_ratio', 'final_score']]

    def _generate_signal(self, final_score: pd.Series) -> pd.Series:
        """
        (Optional) Generate a binary trading signal based on the final score.

        A signal of 1 is returned if the final_score is non-negative, indicating that
        the company meets or exceeds the quality criteria; otherwise 0.

        Args:
            final_score (pd.Series): Series of computed QARP scores.

        Returns:
            pd.Series: Array of binary signals.
        """
        return (final_score >= 0).astype(int)