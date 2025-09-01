# trading_system/src/strategy/value/greenblatts_earnings_yield_strat.py

# TODO: Verify

"""
This module implements Greenblatt's Earnings Yield & Return on Capital strategy.
The strategy identifies quality businesses trading at cheap valuations by evaluating:
  - High Earnings Yield (EBIT / Enterprise Value)
  - High Return on Capital (EBIT / Invested Capital)

It retrieves historical financials (income statement and balance sheet), aligns the
financial statement dates with historical closing prices, computes key metrics and 
a final score for each record so that the tickers can be ranked later on.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class GreenblattEarningsYieldReturnOnCapitalStrategy(BaseStrategy):
    """
    Implementation of Greenblatt's Earnings Yield & Return on Capital strategy.
    
    This strategy identifies quality businesses trading at cheap valuations by evaluating:
      - High Earnings Yield (EBIT / Enterprise Value)
      - High Return on Capital (EBIT / Invested Capital)
    
    The strategy retrieves historical financial data from the income_statement and balance_sheet
    tables, aligns the data with historical closing prices extracted from the daily_prices table,
    computes key metrics, and then produces a final score. The final score is calculated by 
    ranking the companies (on each financial date) on both ratios and summing the two ranks.
    A lower final score indicates better relative quality.
    
    Attributes:
        params (dict): Dictionary containing strategy parameters:
            - 'min_market_cap': Minimum market capitalization threshold.
            - 'min_ebit': Minimum operating earnings (EBIT) required.
    """
    
    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Greenblatt Earnings Yield & Return on Capital strategy.
        
        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override default parameters.
                Defaults are chosen with the Indian market in mind (NSE/BSE).
        """
        super().__init__(db_config, params)
        self.params = params or {
            'min_market_cap': 500e6,  # e.g., 500 million INR; adjust as necessary.
            'min_ebit': 25e6          # e.g., 25 million INR minimum EBIT.
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
        Generate historical trading signals based on Greenblatt's Earnings Yield & Return on Capital strategy.
        
        Retrieves historical income statement and balance sheet data for the provided ticker(s),
        merges them on the 'ticker' and 'date' columns, aligns each financial statement date with
        the nearest available closing price (using backward fill), and computes the following metrics:
        
            - Market Capitalization = ordinary_shares_number * close price
            - Enterprise Value = market_cap + total_debt - cash_and_cash_equivalents
            - Earnings Yield = EBIT / Enterprise Value
            - Return on Capital = EBIT / Invested Capital
            - Final Score = (rank of earnings yield + rank of return on capital) on each financial date
        
        The lower the final score, the more attractive the company. The output contains a trend
        of these metrics over the financial statement history.
        
        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): Starting position (not used in this strategy).
            latest_only (bool): If True, returns only the most recent available signal per ticker.
        
        Returns:
            pd.DataFrame: DataFrame with the columns:
                ['ticker', 'date', 'operating_earnings', 'market_cap', 'enterprise_value', 
                 'earnings_yield', 'return_on_capital', 'final_score']
            where 'final_score' can be used to rank various tickers later.
        """
        try:
            # Retrieve complete financial data (all available historical periods)
            income_stmt = self.get_financials(tickers, 'income_statement', lookback=None)
            balance_sheet = self.get_financials(tickers, 'balance_sheet', lookback=None)
            
            # Validate that sufficient data was retrieved.
            if income_stmt.empty or balance_sheet.empty:
                self.logger.warning(f"Incomplete financial statements for {tickers}")
                return pd.DataFrame()

            # Merge income statement and balance sheet data based on common 'ticker' and 'date'.
            merged = self._merge_financials(income_stmt, balance_sheet)
            if merged.empty:
                return pd.DataFrame()
            
            # Retrieve and align historical closing prices for the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed.")
                return pd.DataFrame()
            
            # Compute the key financial metrics.
            results = self._calculate_metrics(merged, price_data)
            if results.empty:
                return pd.DataFrame()
            
            # Apply any date filtering if provided.
            if start_date:
                results = results[results['date'] >= pd.to_datetime(start_date)]
            if end_date:
                results = results[results['date'] <= pd.to_datetime(end_date)]
            
            # Filter based on minimum market capitalization and EBIT criteria.
            results = results[(results['market_cap'] >= self.params.get('min_market_cap', 0)) &
                              (results['operating_earnings'] >= self.params.get('min_ebit', 0))]
            
            # If only the latest record per ticker is desired.
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
        Merge income statement and balance sheet data on the common 'ticker' and 'date'.
        
        For the income statement, this function selects the first available field among 
        ['ebit', 'operating_income'] to serve as the measure of operating earnings.
        
        For the balance sheet, the following columns are required:
            - total_debt
            - cash_and_cash_equivalents
            - ordinary_shares_number
            - invested_capital
        
        Args:
            income_stmt (pd.DataFrame): Historical income statement data.
            balance_sheet (pd.DataFrame): Historical balance sheet data.
        
        Returns:
            pd.DataFrame: A merged DataFrame with the financial data needed for further analysis.
        """
        required_income_cols = ['ebit', 'operating_income']
        ebit_source = next((col for col in required_income_cols if col in income_stmt.columns), None)
        if not ebit_source:
            self.logger.warning("Missing EBIT/operating income data in income statement")
            return pd.DataFrame()
        
        required_balance_cols = ['total_debt', 'cash_and_cash_equivalents', 'ordinary_shares_number', 'invested_capital']
        missing_balance = [col for col in required_balance_cols if col not in balance_sheet.columns]
        if missing_balance:
            self.logger.warning(f"Missing balance sheet columns: {missing_balance}")
            return pd.DataFrame()
        
        # Ensure both DataFrames have 'ticker' and 'date' as columns with proper datetime types.
        for df in [income_stmt, balance_sheet]:
            if isinstance(df.index, pd.MultiIndex):
                df.reset_index(inplace=True)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            if 'ticker' not in df.columns or df['ticker'].isnull().all():
                df['ticker'] = 'N/A'
        
        # Select the required columns and rename the chosen EBIT field to "ebit".
        income_df = income_stmt[['ticker', 'date', ebit_source]].copy()
        income_df.rename(columns={ebit_source: 'ebit'}, inplace=True)
        
        balance_df = balance_sheet[['ticker', 'date'] + required_balance_cols].copy()
        
        # Merge on 'ticker' and 'date'.
        merged = pd.merge(income_df, balance_df, on=['ticker', 'date'], how='inner')
        
        return merged

    def _get_price_data(self, merged_data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for the financial statement dates.
        
        This method uses the base strategy's get_historical_prices to pull closing prices within the
        range of the financial statement dates, then uses merge_asof to align the statement dates
        with the most recent available closing price (backward fill) based on each ticker.
        
        Args:
            merged_data (pd.DataFrame): Merged financial statement data containing 'date' and 'ticker'.
            tickers (Union[str, List[str]]): Single ticker or list of tickers.
        
        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'] containing aligned closing prices.
        """
        if merged_data.empty:
            return pd.DataFrame()
        
        # Determine the date range from financial statement data.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()
        
        # Reset index in case the date column is set as the index.
        if isinstance(price_df.index, pd.DatetimeIndex):
            price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            # If only one ticker is provided.
            if isinstance(tickers, str):
                price_df['ticker'] = tickers
            else:
                price_df['ticker'] = tickers[0]
        
        # Ensure the date column is in datetime format.
        price_df['date'] = pd.to_datetime(price_df['date'])
        
        # Sort values to prepare for merge_asof.
        price_df.sort_values(['ticker', 'date'], inplace=True)
        merged_data = merged_data.sort_values(['ticker', 'date'])
        
        # Use merge_asof to match each financial statement date with the nearest past closing price.
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
        Calculate fundamental metrics: market capitalization, enterprise value, earnings yield, and return on capital.
        
        The computations are as follows:
            - Market Capitalization = ordinary_shares_number * close price.
            - Enterprise Value = market_cap + total_debt - cash_and_cash_equivalents.
            - Earnings Yield = EBIT / enterprise_value.
            - Return on Capital = EBIT / invested_capital.
        
        For ranking, the earnings yield and return on capital are ranked (by date) in descending order
        so that a higher ratio gets the best (lowest) rank. The final score is defined as the sum of these
        two rankings – lower scores indicate an attractive combination.
        
        Args:
            merged_data (pd.DataFrame): Merged financial statement data.
            price_data (pd.DataFrame): Aligned historical price data.
        
        Returns:
            pd.DataFrame: DataFrame containing calculated metrics with the following columns:
                ['ticker', 'date', 'operating_earnings', 'market_cap', 'enterprise_value',
                 'earnings_yield', 'return_on_capital', 'final_score']
        """
        if merged_data.empty or price_data.empty:
            return pd.DataFrame()
        
        # Merge the financial statement data with corresponding price data.
        data = pd.merge(merged_data, price_data, on=['ticker', 'date'], how='left')
        if data['close'].isnull().all():
            self.logger.warning("Price data alignment failed: no matching close prices found.")
            return pd.DataFrame()
        
        # Compute Market Capitalization.
        data['market_cap'] = data['ordinary_shares_number'] * data['close']
        
        # Compute Enterprise Value: market_cap + total_debt - cash_and_cash_equivalents.
        data['enterprise_value'] = data['market_cap'] + data['total_debt'] - data['cash_and_cash_equivalents']
        
        # Avoid division by zero.
        data['enterprise_value'].replace(0, np.nan, inplace=True)
        data['invested_capital'].replace(0, np.nan, inplace=True)
        
        # Calculate Earnings Yield and Return on Capital.
        data['earnings_yield'] = data['ebit'] / data['enterprise_value']
        data['return_on_capital'] = data['ebit'] / data['invested_capital']
        
        # Rank the stocks within each financial statement date for each ratio.
        # Higher value = better rank (i.e. rank 1 is best).
        data['ey_rank'] = data.groupby('date')['earnings_yield'].rank(ascending=False, method='min')
        data['roc_rank'] = data.groupby('date')['return_on_capital'].rank(ascending=False, method='min')
        data['final_score'] = data['ey_rank'] + data['roc_rank']
        
        # Rename EBIT for clarity.
        data.rename(columns={'ebit': 'operating_earnings'}, inplace=True)
        
        # Select the relevant columns for output.
        results = data[['ticker', 'date', 'operating_earnings', 'market_cap', 'enterprise_value',
                        'earnings_yield', 'return_on_capital', 'final_score']].copy()
        return results