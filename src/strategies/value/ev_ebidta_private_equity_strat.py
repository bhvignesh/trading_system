# trading_system/src/strategies/value/ev_ebidta_private_equity_strat.py

# TODO: Verify

"""
EV/EBITDA Screener (Private Equity Style) Strategy

This strategy computes the Enterprise Value (EV) to EBITDA ratio using historical 
financial data from the income statement and balance sheet. Rather than using P/E, 
the ratio computed using EV/EBITDA accounts for debt and is popular with private 
equity investors and acquirers. A low ratio (default <6x) is indicative of 
undervaluation. The strategy merges quarterly (or periodic) financial statement dates 
with historical price data to produce a trend, and finally outputs a final score (the 
EV/EBITDA ratio) along with a binary signal.

The input tickers can be a single ticker string or a list of tickers.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class EVEBITDAScreenerStrategy(BaseStrategy):
    """
    Implementation of the EV/EBITDA Screener (Private Equity Style) strategy.
    
    This strategy computes the Enterprise Value to EBITDA (EV/EBITDA) ratio using 
    historical financial statement data from the income statement and balance sheet,
    and aligns each fundamental date with historical closing prices. The computed 
    EV/EBITDA ratio is used as a signal, where a ratio below the specified threshold 
    (default < 6.0) suggests undervaluation, a metric favored by private equity investors.

    Attributes:
        params (dict): Dictionary containing strategy parameters:
            - 'min_market_cap': Minimum market capitalization to consider (e.g., in INR).
            - 'max_ev_ebitda': EV/EBITDA threshold; ratios below this are considered undervalued.
            - 'min_ebitda': Minimum EBITDA required for a valid calculation.
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the EV/EBITDA Screener strategy.

        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override default parameters.
                Defaults are optimized for the Indian market (NSE/BSE).
        """
        super().__init__(db_config, params)
        self.params = params or {
            'min_market_cap': 1e8,       # INR 100 million minimum market cap (example value)
            'max_ev_ebitda': 6.0,        # EV/EBITDA threshold (under 6 considered undervalued)
            'min_ebitda': 1e7            # INR 10 million minimum EBITDA
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
        Generate historical trading signals based on the EV/EBITDA Screener strategy.
        
        Retrieves financial statement data (income statement and balance sheet) for the 
        provided ticker(s), aligns the data with historical closing prices at the financial
        statement dates, and calculates key metrics:
            - Market Capitalization = ordinary_shares_number * closing price
            - Enterprise Value (EV) = market_cap + net_debt
            - EV/EBITDA ratio = enterprise_value / EBITDA
        
        A binary signal is generated: 1 if EV/EBITDA is below the specified threshold, 
        indicating potential undervaluation, and 0 otherwise. This ratio is also output
        as the final score to facilitate ranking among various tickers.
        
        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, return only the most recent available signal per ticker.
        
        Returns:
            pd.DataFrame: DataFrame containing columns:
              ['ticker', 'date', 'ebitda', 'enterprise_value', 'market_cap', 'ev_ebitda', 'signal']
        """
        try:
            # Retrieve financial statements for income statement and balance sheet.
            income_stmt = self.get_financials(tickers, 'income_statement', lookback=None)
            balance_sheet = self.get_financials(tickers, 'balance_sheet', lookback=None)

            # Validate that sufficient financial statement data is available.
            if income_stmt.empty or balance_sheet.empty:
                self.logger.warning(f"Incomplete financial statements data for {tickers}.")
                return pd.DataFrame()

            # Merge income statement and balance sheet data on 'ticker' and 'date'.
            merged = self._merge_financials(income_stmt, balance_sheet)
            if merged.empty:
                self.logger.warning("Merged financial data is empty after combining income statement and balance sheet.")
                return pd.DataFrame()

            # Retrieve and align the closing price data for the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Historical price data retrieval failed.")
                return pd.DataFrame()

            # Calculate market cap, enterprise value, and EV/EBITDA ratio.
            results = self._calculate_metrics(merged, price_data)
            if results.empty:
                self.logger.warning("Metrics calculation resulted in empty dataset.")
                return pd.DataFrame()

            # Filter results by specified date range, if provided.
            if start_date:
                results = results[results['date'] >= pd.to_datetime(start_date)]
            if end_date:
                results = results[results['date'] <= pd.to_datetime(end_date)]

            # Generate the binary trading signal.
            results['signal'] = self._generate_signal_column(results)

            # If only the latest signal is desired, select the most recent record for each ticker.
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
        
        For the income statement, the function selects the first available EBITDA measure 
        from the columns ['ebitda', 'normalized_ebitda']. For the balance sheet, it requires 
        the columns: 'ordinary_shares_number' and 'net_debt'.
        
        Args:
            income_stmt (pd.DataFrame): Historical income statement data.
            balance_sheet (pd.DataFrame): Historical balance sheet data.
        
        Returns:
            pd.DataFrame: Merged DataFrame containing columns ['ticker', 'date', 'ebitda',
                        'ordinary_shares_number', 'net_debt'].
        """
        # Ensure that 'ticker' and 'date' are columns (reset index if necessary).
        if not isinstance(income_stmt.index, pd.MultiIndex):
            income_stmt = income_stmt.reset_index()
        if not isinstance(balance_sheet.index, pd.MultiIndex):
            balance_sheet = balance_sheet.reset_index()

        # Select EBITDA measure from the income statement.
        ebitda_col = None
        for col in ['ebitda', 'normalized_ebitda']:
            if col in income_stmt.columns:
                ebitda_col = col
                break
        if not ebitda_col:
            self.logger.warning("EBITDA data not found in income statement.")
            return pd.DataFrame()

        # Ensure that dates are in datetime format.
        income_stmt['date'] = pd.to_datetime(income_stmt['date'])
        balance_sheet['date'] = pd.to_datetime(balance_sheet['date'])

        # Ensure 'ticker' column exists.
        if 'ticker' not in income_stmt.columns:
            income_stmt['ticker'] = 'N/A'
        if 'ticker' not in balance_sheet.columns:
            balance_sheet['ticker'] = 'N/A'

        # Merge the two statements on 'ticker' and 'date'.
        merged = pd.merge(
            income_stmt[['ticker', 'date', ebitda_col]],
            balance_sheet[['ticker', 'date', 'ordinary_shares_number', 'net_debt']],
            on=['ticker', 'date'],
            how='inner'
        )

        # Rename the EBITDA column to a consistent name.
        merged.rename(columns={ebitda_col: 'ebitda'}, inplace=True)
        return merged

    def _get_price_data(self, merged_data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for the given financial statement dates.
        
        Determines the minimum and maximum dates from the merged financial data, retrieves historical 
        price data for the ticker(s) in that date range, and aligns the prices using a backward fill 
        with merge_asof.
        
        Args:
            merged_data (pd.DataFrame): Financial statement data containing at least 'date' and 'ticker'.
            tickers (Union[str, List[str]]): The ticker or list of tickers.
        
        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'] containing the aligned closing prices.
        """
        if merged_data.empty:
            return pd.DataFrame()

        # Determine the date range for price retrieval.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data using the base strategy's method.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        # Reset index to ensure 'date' and 'ticker' are columns.
        price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            # For single ticker scenario.
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

        # Ensure 'date' column is in datetime format.
        price_df['date'] = pd.to_datetime(price_df['date'])
        merged_data['date'] = pd.to_datetime(merged_data['date'])

        # Sort both DataFrames by ticker and date.
        price_df.sort_values(['ticker', 'date'], inplace=True)
        merged_data.sort_values(['ticker', 'date'], inplace=True)

        # Align financial statement dates with the most recent available closing price (backward fill).
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
        Calculate key financial metrics: market capitalization, enterprise value, and EV/EBITDA ratio.
        
        Market Capitalization is computed as: ordinary_shares_number * closing price.
        Enterprise Value (EV) is computed as: market_cap + net_debt.
        EV/EBITDA is then computed by dividing the enterprise value by EBITDA.
        
        Filtering is applied to retain only rows where:
          - enterprise_value > 0
          - EBITDA is at least the minimum threshold defined in parameters.
          - market_cap is at least the minimum market cap defined in parameters.
        
        Args:
            merged_data (pd.DataFrame): Merged financial statement data.
            price_data (pd.DataFrame): DataFrame containing aligned closing prices.
        
        Returns:
            pd.DataFrame: DataFrame with columns:
              ['ticker', 'date', 'ebitda', 'enterprise_value', 'market_cap', 'ev_ebitda']
        """
        if merged_data.empty or price_data.empty:
            return pd.DataFrame()

        # Merge the financial data with price data on ticker and date.
        data = pd.merge(merged_data, price_data, on=['ticker', 'date'], how='left')
        if data['close'].isnull().all():
            self.logger.warning("Failed to align closing prices with financial data.")
            return pd.DataFrame()

        # Vectorized calculation of market capitalization.
        data['market_cap'] = data['ordinary_shares_number'] * data['close']

        # Calculate enterprise value as market_cap + net_debt.
        data['enterprise_value'] = data['market_cap'] + data['net_debt']

        # Compute EV/EBITDA ratio, handling division by zero by replacing zero EBITDA with NaN.
        data['ev_ebitda'] = data['enterprise_value'] / data['ebitda'].replace(0, np.nan)

        # Apply filtering conditions.
        valid = data[
            (data['enterprise_value'] > 0) &
            (data['ebitda'] >= self.params['min_ebitda']) &
            (data['market_cap'] >= self.params['min_market_cap']) &
            (data['ev_ebitda'].notnull())
        ].copy()

        return valid[['ticker', 'date', 'ebitda', 'enterprise_value', 'market_cap', 'ev_ebitda']]

    def _generate_signal_column(self, results: pd.DataFrame) -> np.ndarray:
        """
        Generate a binary signal based on the EV/EBITDA ratio.
        
        A signal of 1 is generated if the EV/EBITDA ratio is less than or equal to the maximum threshold, 
        indicating potential undervaluation, and 0 otherwise.
        
        Args:
            results (pd.DataFrame): DataFrame containing the 'ev_ebitda' column.
        
        Returns:
            np.ndarray: Array of binary signals.
        """
        return np.where(results['ev_ebitda'] <= self.params['max_ev_ebitda'], 1, 0).astype(int)