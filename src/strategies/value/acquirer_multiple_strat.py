# trading_system/src/strategies/acquirers_multiple_strategy.py

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class AcquirersMultipleStrategy(BaseStrategy):
    """
    Implementation of Tobias Carlisle's Acquirer's Multiple strategy.
    
    This strategy computes the Acquirer's Multiple (EV/EBIT) for one or more tickers using
    historical financial statements (income statement and balance sheet) and aligns each 
    fundamental date with historical closing prices. The computed EV/EBIT ratio is used to 
    generate a trading signal: if the ratio is below a given threshold, the signal is positive.
    
    Attributes:
        params (dict): Dictionary containing strategy parameters:
            - 'min_market_cap': Minimum market capitalization (in dollars) to consider.
            - 'max_multiple': Maximum EV/EBIT threshold for a buy signal.
            - 'min_ebit': Minimum operating earnings (EBIT) required.
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Acquirer's Multiple strategy.
        
        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override default parameters.
        """
        super().__init__(db_config, params)
        self.params = params or {
            'min_market_cap': 50e6,  # $50M minimum market cap
            'max_multiple': 8.0,     # EV/EBIT threshold
            'min_ebit': 5e6          # Minimum operating earnings (EBIT)
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
        Generate historical trading signals based on the Acquirer's Multiple strategy.
        
        Retrieves financial statements for the provided ticker(s), aligns the data with historical
        closing prices at the financial statement dates, calculates market capitalization, enterprise value,
        and the EV/EBIT ratio (Acquirer's Multiple). A binary signal is then generated using the specified 
        threshold parameters.
        
        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, the method returns only the most recent available signal per ticker.
        
        Returns:
            pd.DataFrame: DataFrame containing the columns:
              ['ticker', 'date', 'operating_earnings', 'enterprise_value', 
               'market_cap', 'acquirers_multiple', 'signal']
              where acquirers_multiple is the final score.
        """
        try:
            # Retrieve complete financial data (all available historical dates) for income statement and balance sheet.
            income_stmt = self.get_financials(tickers, 'income_statement', lookback=None)
            balance_sheet = self.get_financials(tickers, 'balance_sheet', lookback=None)

            # Validate that we have sufficient data from both statements.
            if income_stmt.empty or balance_sheet.empty:
                self.logger.warning(f"Incomplete financial statements for {tickers}")
                return pd.DataFrame()

            # Merge income statement and balance sheet data on their date (and ticker if provided).
            merged = self._merge_financials(income_stmt, balance_sheet)
            if merged.empty:
                return pd.DataFrame()

            # Retrieve and align closing price data for the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed.")
                return pd.DataFrame()

            # Calculate key metrics including market cap, enterprise value and Acquirer's Multiple.
            results = self._calculate_metrics(merged, price_data)
            if results.empty:
                return pd.DataFrame()

            # Apply date range filtering if provided.
            if start_date:
                results = results[results['date'] >= pd.to_datetime(start_date)]
            if end_date:
                results = results[results['date'] <= pd.to_datetime(end_date)]

            # Generate signal column: 1 if acquirers_multiple is less than or equal to the threshold, else 0.
            results['signal'] = self._generate_signal_column(results)

            # If only the latest signal is desired, pick the most recent record per ticker.
            if latest_only:
                if isinstance(tickers, list) or 'ticker' in results.columns:
                    results = results.sort_values('date').groupby('ticker').tail(1)
                else:
                    results = results.sort_values('date').tail(1)

            return results.reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _merge_financials(self, income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame) -> pd.DataFrame:
        """
        Merge income statement and balance sheet data on the common date (and ticker if applicable).
        
        For the income statement, this function looks for the first available value among the columns
        ['ebit', 'operating_income'] as the measure for operating earnings.
        For the balance sheet, it expects the columns:
          - 'total_debt'
          - 'cash_and_cash_equivalents'
          - 'ordinary_shares_number'
        
        Returns a DataFrame with the merged fundamental data.
        
        Args:
            income_stmt (pd.DataFrame): Historical income statement data.
            balance_sheet (pd.DataFrame): Historical balance sheet data.
        
        Returns:
            pd.DataFrame: Merged DataFrame containing the required financial metrics.
        """
        required_income_cols = ['ebit', 'operating_income']
        ebit_source = next((col for col in required_income_cols if col in income_stmt.columns), None)
        if not ebit_source:
            self.logger.warning("Missing EBIT/operating income data in income statement")
            return pd.DataFrame()

        required_balance_cols = ['total_debt', 'cash_and_cash_equivalents', 'ordinary_shares_number']
        missing_balance = [col for col in required_balance_cols if col not in balance_sheet.columns]
        if missing_balance:
            self.logger.warning(f"Missing balance sheet columns: {missing_balance}")
            return pd.DataFrame()

        # Ensure the dataframes have a 'date' and 'ticker' column for merging.
        for df in [income_stmt, balance_sheet]:
            if not isinstance(df.index, pd.MultiIndex):
                df.reset_index(inplace=True)
            # If 'ticker' is missing, add a dummy ticker column.
            if 'ticker' not in df.columns or df['ticker'].isnull().all():
                df['ticker'] = 'N/A'
            # Ensure the 'date' column is datetime.
            df['date'] = pd.to_datetime(df['date'])

        # Merge the financial statements on 'ticker' and 'date'
        merged = pd.merge(
            income_stmt[['ticker', 'date', ebit_source]],
            balance_sheet[['ticker', 'date'] + required_balance_cols],
            on=['ticker', 'date'],
            how='inner',
            suffixes=('_income', '_balance')
        )

        return merged

    def _get_price_data(self, merged_data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for financial statement dates.
        
        Uses the base strategy's get_historical_prices to pull closing prices over the date range of the
        financial statements, and aligns each financial statement date with the most recent available closing
        price (backward fill) using a merge_asof, grouped by ticker.
        
        Args:
            merged_data (pd.DataFrame): Merged financial statement data containing at least 'date' and 'ticker'.
            tickers (Union[str, List[str]]): The ticker or list of tickers.
        
        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'] containing aligned closing prices.
        """
        if merged_data.empty:
            return pd.DataFrame()

        # Determine the date range from the financial statement dates.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data for the tickers over the determined date range.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        # Reset index so that 'ticker' and 'date' are explicit columns.
        price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            # For single ticker case, assign the ticker value.
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

        # Ensure the date column is named 'date' and in datetime format.
        if 'date' not in price_df.columns and 'Date' in price_df.columns:
            price_df.rename(columns={'Date': 'date'}, inplace=True)
        price_df['date'] = pd.to_datetime(price_df['date'])
        merged_data['date'] = pd.to_datetime(merged_data['date'])

        # Sort both dataframes for merge_asof to work properly.
        price_df.sort_values(['ticker', 'date'], inplace=True)
        merged_data.sort_values(['ticker', 'date'], inplace=True)

        # Use merge_asof to align financial statement dates with the most recent price.
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
        Calculate fundamental metrics: market capitalization, enterprise value, and Acquirer's Multiple.
        
        The market capitalization is computed as the number of outstanding shares (ordinary_shares_number)
        multiplied by the closing price. The enterprise value is derived as:
            enterprise_value = market_cap + total_debt - cash_and_cash_equivalents.
        Finally, the Acquirer's Multiple (EV/EBIT) is computed as enterprise_value divided by EBIT.
        
        Args:
            merged_data (pd.DataFrame): Merged financial statement data.
            price_data (pd.DataFrame): DataFrame containing aligned closing prices.
        
        Returns:
            pd.DataFrame: DataFrame with the calculated metrics and renamed EBIT as operating_earnings. 
                        Columns include: [ticker, date, operating_earnings, enterprise_value, market_cap, acquirers_multiple]
        """
        if merged_data.empty or price_data.empty:
            return pd.DataFrame()

        # Align the closing prices with the financial statement data.
        data = pd.merge(merged_data, price_data, on=['ticker', 'date'], how='left')
        if data['close'].isnull().all():
            self.logger.warning("Price data alignment failed: no matching close prices found.")
            return pd.DataFrame()

        # Determine the EBIT source column: prefers 'ebit' then 'operating_income'.
        ebit_col = None
        for col in ['ebit', 'operating_income']:
            if col in data.columns:
                ebit_col = col
                break
        if not ebit_col:
            self.logger.warning("Missing EBIT/operating income data after merge.")
            return pd.DataFrame()

        # Calculate market capitalization using outstanding shares and closing price.
        data['market_cap'] = data['ordinary_shares_number'] * data['close']

        # Calculate enterprise value: Market Cap + Total Debt - Cash & Cash Equivalents.
        data['enterprise_value'] = data['market_cap'] + data['total_debt'] - data['cash_and_cash_equivalents']

        # Compute Acquirer's Multiple (EV/EBIT) while avoiding division by zero.
        data['acquirers_multiple'] = data['enterprise_value'] / data[ebit_col].replace(0, np.nan)

        # Filter to keep only rows with valid calculations.
        valid = data[
            (data['enterprise_value'] > 0) &
            (data[ebit_col] >= self.params['min_ebit']) &
            (data['market_cap'] >= self.params['min_market_cap']) &
            (data['acquirers_multiple'].notnull())
        ].copy()

        # Rename EBIT column to operating_earnings for clarity.
        valid.rename(columns={ebit_col: 'operating_earnings'}, inplace=True)
        return valid[['ticker', 'date', 'operating_earnings', 'enterprise_value', 'market_cap', 'acquirers_multiple']]

    def _generate_signal_column(self, results: pd.DataFrame) -> np.ndarray:
        """
        Generate binary trading signals based on the Acquirer's Multiple threshold.
        
        A signal of 1 is generated if the Acquirer's Multiple is less than or equal to the maximum allowed multiple,
        and 0 otherwise.
        
        Args:
            results (pd.DataFrame): DataFrame containing 'acquirers_multiple' and relevant metrics.
        
        Returns:
            np.ndarray: Array of binary signals.
        """
        return np.where(results['acquirers_multiple'] <= self.params['max_multiple'], 1, 0).astype(int)