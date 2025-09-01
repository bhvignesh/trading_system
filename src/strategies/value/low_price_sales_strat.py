# trading_system/src/strategies/value/low_price_to_sales_strategy.py

# TODO: Verify

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class LowPriceToSalesStrategy(BaseStrategy):
    """
    Implementation of the Low Price-to-Sales (P/S) Screener strategy.

    This strategy identifies stocks with a Price-to-Sales ratio (P/S) < 1,
    which may indicate deep undervaluation. It relies on historical financial
    data from the income statement and balance sheet (with a secondary reliance on
    cash flow only if needed) rather than company info. The strategy pulls the
    available quarterly (or period) financial statement dates and aligns the
    data with historical closing prices to calculate the P/S ratio across these dates.
    The P/S trend (over the available statement dates) can be used for further analysis or ranking.

    Attributes:
        params (dict): Dictionary containing strategy parameters:
            - 'ps_threshold': Price-to-Sales threshold (default: 1.0).
            - 'min_total_revenue': Minimum total revenue required to avoid noisy data (default: 1e6).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Low Price-to-Sales strategy.

        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override the default parameters.
                Defaults are optimized for the Indian market (NSE/BSE).
        """
        super().__init__(db_config, params)
        self.params = params or {
            'ps_threshold': 1.0,      # P/S ratio threshold below which a signal is generated.
            'min_total_revenue': 1e6  # Minimum total revenue (can be tuned for Indian market requirements).
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
        Generate historical trading signals based on the Low Price-to-Sales Screener strategy.

        This method retrieves historical quarterly financials from the income statement and 
        balance sheet, aligns each financial statement date with the most recent closing price, 
        calculates the market capitalization and then computes the P/S ratio as:
        
            price_to_sales = (ordinary_shares_number * close_price) / total_revenue

        A binary signal is created: signal = 1 if the P/S ratio is below the threshold,
        else signal = 0. The output DataFrame includes a trend across the available dates.

        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent available signal per ticker.

        Returns:
            pd.DataFrame: DataFrame containing the columns:
              ['ticker', 'date', 'total_revenue', 'ordinary_shares_number', 'market_cap',
               'price_to_sales', 'signal']
        """
        try:
            # Retrieve full historical financial data for income statement and balance sheet.
            income_stmt = self.get_financials(tickers, 'income_statement', lookback=None)
            balance_sheet = self.get_financials(tickers, 'balance_sheet', lookback=None)

            # Validate that sufficient data was retrieved.
            if income_stmt.empty or balance_sheet.empty:
                self.logger.warning(f"Incomplete financial statements for tickers: {tickers}")
                return pd.DataFrame()

            # Merge financial statement data on common 'ticker' and 'date'.
            merged = self._merge_financials(income_stmt, balance_sheet)
            if merged.empty:
                return pd.DataFrame()

            # Retrieve and align closing price data for the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed.")
                return pd.DataFrame()

            # Calculate market capitalization and the Price-to-Sales ratio.
            results = self._calculate_metrics(merged, price_data)
            if results.empty:
                return pd.DataFrame()

            # Optionally filter the results by date.
            if start_date:
                results = results[results['date'] >= pd.to_datetime(start_date)]
            if end_date:
                results = results[results['date'] <= pd.to_datetime(end_date)]

            # Generate the binary trading signal: 1 if P/S < ps_threshold, else 0.
            results['signal'] = self._generate_signal_column(results)

            # If only the most recent signal is desired, select the last record per ticker.
            if latest_only:
                if 'ticker' in results.columns:
                    results = results.sort_values('date').groupby('ticker').tail(1)
                else:
                    results = results.sort_values('date').tail(1)

            return results.reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _merge_financials(self, income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame) -> pd.DataFrame:
        """
        Merge income statement and balance sheet data on 'ticker' and 'date'.
        
        For the income statement, the method expects the column 'total_revenue'.
        For the balance sheet, it expects the 'ordinary_shares_number' column.
        Both dataframes are prepared to ensure they contain 'ticker' and 'date' for a proper merge.

        Args:
            income_stmt (pd.DataFrame): Income statement data with the 'total_revenue' column.
            balance_sheet (pd.DataFrame): Balance sheet data with the 'ordinary_shares_number' column.

        Returns:
            pd.DataFrame: Merged DataFrame containing ['ticker', 'date', 'total_revenue', 'ordinary_shares_number'].
        """
        # Ensure the dataframes have explicit 'ticker' and 'date' columns.
        for df in [income_stmt, balance_sheet]:
            if not isinstance(df.index, pd.MultiIndex):
                df.reset_index(inplace=True)
            if 'ticker' not in df.columns or df['ticker'].isnull().all():
                df['ticker'] = 'N/A'
            df['date'] = pd.to_datetime(df['date'])

        if 'total_revenue' not in income_stmt.columns:
            self.logger.warning("Missing 'total_revenue' in income statement.")
            return pd.DataFrame()
        if 'ordinary_shares_number' not in balance_sheet.columns:
            self.logger.warning("Missing 'ordinary_shares_number' in balance sheet.")
            return pd.DataFrame()

        # Merge on both 'ticker' and 'date'.
        merged = pd.merge(
            income_stmt[['ticker', 'date', 'total_revenue']],
            balance_sheet[['ticker', 'date', 'ordinary_shares_number']],
            on=['ticker', 'date'],
            how='inner'
        )
        return merged

    def _get_price_data(self, merged_data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data corresponding to the financial statement dates.

        The method determines the date range from the provided financial statement data,
        retrieves historical prices using the base method, and then aligns the financial statement
        dates with the most recent available closing price using a merge_asof.

        Args:
            merged_data (pd.DataFrame): Merged financial statement data that includes 'date' and 'ticker'.
            tickers (Union[str, List[str]]): The ticker symbol or a list of tickers.

        Returns:
            pd.DataFrame: DataFrame with aligned closing prices containing columns:
                          ['ticker', 'date', 'close']
        """
        if merged_data.empty:
            return pd.DataFrame()

        # Determine the overall date range for financial statement data.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        price_df = price_df.reset_index()  # Ensure 'date' is a column
        if 'ticker' not in price_df.columns:
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

        if 'date' not in price_df.columns and 'Date' in price_df.columns:
            price_df.rename(columns={'Date': 'date'}, inplace=True)
        price_df['date'] = pd.to_datetime(price_df['date'])

        # Sort both dataframes for proper merging.
        price_df.sort_values(['ticker', 'date'], inplace=True)
        merged_data.sort_values(['ticker', 'date'], inplace=True)

        # Use merge_asof to align each financial statement date with the latest closing price.
        aligned = pd.merge_asof(
            merged_data,
            price_df[['ticker', 'date', 'close']],
            on='date',
            by='ticker',
            direction='backward'
        )
        return aligned[['ticker', 'date', 'close']]

    def _calculate_metrics(self, merged_data: pd.DataFrame, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate market capitalization and Price-to-Sales (P/S) ratio.

        The market capitalization is calculated as:
            market_cap = ordinary_shares_number * close price.
        The Price-to-Sales ratio is derived as:
            price_to_sales = market_cap / total_revenue.

        Only rows with valid (positive) data for revenue and outstanding shares, and meeting the
        minimum total revenue requirement are kept.

        Args:
            merged_data (pd.DataFrame): Merged financial data containing 'total_revenue' and 'ordinary_shares_number'.
            price_data (pd.DataFrame): DataFrame containing aligned closing prices.

        Returns:
            pd.DataFrame: DataFrame with the following columns:
                        ['ticker', 'date', 'total_revenue', 'ordinary_shares_number',
                         'market_cap', 'price_to_sales']
        """
        # Merge the financial and price data.
        data = pd.merge(merged_data, price_data, on=['ticker', 'date'], how='left')
        if data['close'].isnull().all():
            self.logger.warning("Price data alignment failed: no matching close prices found.")
            return pd.DataFrame()

        # Compute market capitalization.
        data['market_cap'] = data['ordinary_shares_number'] * data['close']
        # Compute Price-to-Sales ratio and avoid division by zero.
        data['price_to_sales'] = data['market_cap'] / data['total_revenue'].replace(0, pd.NA)

        # Filter records: positive revenue, positive shares and above the minimum revenue threshold.
        valid = data[
            (data['total_revenue'] > 0) &
            (data['ordinary_shares_number'] > 0) &
            (data['total_revenue'] >= self.params['min_total_revenue']) &
            (data['price_to_sales'].notnull())
        ].copy()

        return valid[['ticker', 'date', 'total_revenue', 'ordinary_shares_number', 'market_cap', 'price_to_sales']]

    def _generate_signal_column(self, results: pd.DataFrame) -> np.ndarray:
        """
        Generate binary trading signals based on the Price-to-Sales (P/S) ratio.

        A signal of 1 is generated if the P/S ratio is below the threshold defined in
        params (ps_threshold), suggesting undervaluation. Otherwise, the signal is 0.

        Args:
            results (pd.DataFrame): DataFrame containing the 'price_to_sales' column.

        Returns:
            np.ndarray: An array of binary signals.
        """
        return np.where(results['price_to_sales'] < self.params['ps_threshold'], 1, 0).astype(int)