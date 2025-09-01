# trading_system/src/strategies/value/oshaugnessy_trending_value_strat.py

# TODO: Verify

from datetime import timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class TrendingValueStrategy(BaseStrategy):
    """
    Implementation of O'Shaughnessy’s Trending Value Strategy.
    
    This strategy combines value and momentum by screening for:
      - Low P/E, P/S, and P/B ratios based on historical fundamental data.
      - Strong price momentum over 6-12 months, determined by comparing the 
        financial statement date’s closing price to closing prices 6 and 12 months earlier.
    
    For each fundamental (quarterly) date available from the income statement
    and balance sheet, the strategy performs the following:
      1. Merges income statement and balance sheet data (using key measures such as 
         net income, total revenue, number of outstanding shares, and common equity).
      2. Uses the financial statement date along with historical daily prices to 
         calculate a market capitalization and, hence, the three ratios:
           • P/E = market cap / net income
           • P/S = market cap / total revenue
           • P/B = market cap / common stock equity
         (Ratios are defined only if the denominator is positive; otherwise they are NaN.)
      3. Retrieves extended price data and calculates two momentum measures:
           • 6-month momentum = (current close / price 6 months before - 1)
           • 12-month momentum = (current close / price 12 months before - 1)
         The average of these two values is used as the momentum parameter.
      4. Combines the inverted value ratios and momentum into a final score using 
         a weighted sum. Stocks with lower (better) value ratios and higher momentum 
         receive a higher final score.
    
    The output DataFrame contains one row per fundamental date (plus ticker information)
    and includes the calculated ratios and final score. This score can be used later
    for ranking various tickers.
    
    Default parameter values have been chosen to optimize for the Indian market (NSE and BSE).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Trending Value strategy.
        
        Args:
            db_config: Database configuration settings.
            params (dict, optional): Strategy specific parameters. If not provided,
              defaults are:
                - w_pe: 1.0 (weight for P/E ratio)
                - w_ps: 1.0 (weight for P/S ratio)
                - w_pb: 1.0 (weight for P/B ratio)
                - w_mom: 1.0 (weight for momentum)
                - momentum_6m_days: 180 (lookback period for 6-month momentum)
                - momentum_12m_days: 365 (lookback period for 12-month momentum)
                - min_market_cap: 100e6 (minimum market cap requirement)
        """
        super().__init__(db_config, params)
        self.params = params or {
            'w_pe': 1.0,
            'w_ps': 1.0,
            'w_pb': 1.0,
            'w_mom': 1.0,
            'momentum_6m_days': 180,
            'momentum_12m_days': 365,
            'min_market_cap': 100e6  # e.g., INR 100M minimum market cap
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
        Generate trading signals for the provided ticker(s) based on the Trending Value Strategy.
        
        This method performs the following:
            1. Retrieves historical income statement and balance sheet data.
            2. Merges the two sources on ticker and date to obtain the required fundamentals.
            3. Aligns each financial statement date with the corresponding closing price.
            4. Calculates market capitalization and value ratios (P/E, P/S, P/B).
            5. Retrieves extended historical prices to compute 6-month and 12-month momentum.
            6. Combines inverted value ratios and momentum (using specified weights) into a final score.
            7. Optionally filters the data by date and/or returns only the latest signal per ticker.
        
        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent signal per ticker.
        
        Returns:
            pd.DataFrame: A DataFrame with columns:
              ['ticker', 'date', 'net_income', 'total_revenue', 'ordinary_shares_number',
               'common_stock_equity', 'close', 'market_cap', 'pe_ratio', 'ps_ratio', 
               'pb_ratio', 'momentum', 'score']
              where 'score' is the final ranking metric.
        """
        try:
            # Retrieve complete financial data (all available dates) for income statement and balance sheet.
            income_df = self.get_financials(tickers, statement_type='income_statement', lookback=None)
            balance_df = self.get_financials(tickers, statement_type='balance_sheet', lookback=None)

            # Validate that both financial statements have been retrieved.
            if income_df.empty or balance_df.empty:
                self.logger.warning(f"Incomplete financial statements for {tickers}")
                return pd.DataFrame()

            # Merge income and balance sheet data on ticker and date.
            merged = self._merge_financials(income_df, balance_df)
            if merged.empty:
                self.logger.warning("Merged fundamental data is empty.")
                return pd.DataFrame()

            # Retrieve and align closing price data to the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed.")
                return pd.DataFrame()

            # Merge the fundamental data with the aligned price data.
            # (The financial statement date is used for the valuation ratios.)
            data = pd.merge(merged, price_data, on=['ticker', 'date'], how='left')
            if data['close'].isnull().all():
                self.logger.warning("All closing prices are missing after alignment.")
                return pd.DataFrame()

            # Calculate key value metrics.
            # Market Cap = closing price * shares outstanding.
            data['market_cap'] = data['close'] * data['ordinary_shares_number']

            # P/E Ratio: Only defined when net income is positive.
            data['pe_ratio'] = np.where(data['net_income'] > 0,
                                        data['market_cap'] / data['net_income'],
                                        np.nan)
            # P/S Ratio: Only when total revenue is positive.
            data['ps_ratio'] = np.where(data['total_revenue'] > 0,
                                        data['market_cap'] / data['total_revenue'],
                                        np.nan)
            # P/B Ratio: Only when common stock equity is positive.
            data['pb_ratio'] = np.where(data['common_stock_equity'] > 0,
                                        data['market_cap'] / data['common_stock_equity'],
                                        np.nan)

            # Retrieve extended price data and calculate price momentum.
            data = self._calculate_momentum(data, tickers)

            # Compute inverse ratios (lower ratios are more attractive).
            inv_pe = np.where(data['pe_ratio'] > 0, 1 / data['pe_ratio'], np.nan)
            inv_ps = np.where(data['ps_ratio'] > 0, 1 / data['ps_ratio'], np.nan)
            inv_pb = np.where(data['pb_ratio'] > 0, 1 / data['pb_ratio'], np.nan)

            # Retrieve strategy weights.
            w_pe = self.params.get('w_pe', 1.0)
            w_ps = self.params.get('w_ps', 1.0)
            w_pb = self.params.get('w_pb', 1.0)
            w_mom = self.params.get('w_mom', 1.0)

            # Combine the inverted value ratios and momentum into a composite score.
            data['score'] = (w_pe * inv_pe + w_ps * inv_ps + w_pb * inv_pb) * (1 + w_mom * data['momentum'])

            # Apply minimum market cap filter.
            min_mc = self.params.get('min_market_cap', 100e6)
            data = data[data['market_cap'] >= min_mc]

            # Apply date range filtering if provided.
            if start_date:
                data = data[data['date'] >= pd.to_datetime(start_date)]
            if end_date:
                data = data[data['date'] <= pd.to_datetime(end_date)]

            # If only the latest signal per ticker is requested, group and pick last record.
            if latest_only:
                if isinstance(tickers, list) or 'ticker' in data.columns:
                    data = data.sort_values('date').groupby('ticker').tail(1)
                else:
                    data = data.sort_values('date').tail(1)

            return data.reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _merge_financials(self, income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame) -> pd.DataFrame:
        """
        Merge income statement and balance sheet data on 'ticker' and 'date'.
        
        For income statement, this function prioritizes 'net_income_common_stockholders'
        over 'net_income' for the operating earnings metric. It also extracts 'total_revenue'.
        For the balance sheet, it requires columns:
          - 'ordinary_shares_number'
          - 'common_stock_equity'
        
        Args:
            income_stmt (pd.DataFrame): Historical income statement data.
            balance_sheet (pd.DataFrame): Historical balance sheet data.
        
        Returns:
            pd.DataFrame: Merged DataFrame containing [ticker, date, net_income, total_revenue,
                          ordinary_shares_number, common_stock_equity].
        """
        # Ensure that 'date' and 'ticker' are explicit columns.
        for df in [income_stmt, balance_sheet]:
            if not isinstance(df.index, pd.MultiIndex):
                df.reset_index(inplace=True)
            # If missing, add a dummy ticker column.
            if 'ticker' not in df.columns or df['ticker'].isnull().all():
                df['ticker'] = 'N/A'
            df['date'] = pd.to_datetime(df['date'])

        # Determine which net income column to use.
        net_inc_col = None
        for col in ['net_income_common_stockholders', 'net_income']:
            if col in income_stmt.columns:
                net_inc_col = col
                break
        if not net_inc_col:
            self.logger.warning("Missing net income data in income statement.")
            return pd.DataFrame()

        # Check that required columns exist.
        if 'total_revenue' not in income_stmt.columns:
            self.logger.warning("Missing total revenue in income statement.")
            return pd.DataFrame()
        for col in ['ordinary_shares_number', 'common_stock_equity']:
            if col not in balance_sheet.columns:
                self.logger.warning(f"Missing {col} in balance sheet.")
                return pd.DataFrame()

        # Rename the net income column for clarity.
        income_stmt = income_stmt.rename(columns={net_inc_col: 'net_income'})

        # Select only the required columns.
        income_filtered = income_stmt[['ticker', 'date', 'net_income', 'total_revenue']]
        balance_filtered = balance_sheet[['ticker', 'date', 'ordinary_shares_number', 'common_stock_equity']]

        # Perform an inner join on 'ticker' and 'date'.
        merged = pd.merge(income_filtered, balance_filtered, on=['ticker', 'date'], how='inner')
        return merged

    def _get_price_data(self, merged_data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for the financial statement dates.
        
        Uses the base strategy's get_historical_prices method to pull closing prices over
        the date range spanned by the fundamental data, then aligns each financial statement date
        with the most recent available closing price via a merge_asof join.
        
        Args:
            merged_data (pd.DataFrame): Merged fundamental data containing a 'date' column.
            tickers (Union[str, List[str]]): Ticker(s) to retrieve price data for.
        
        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'].
        """
        if merged_data.empty:
            return pd.DataFrame()
        # Determine the date range from the fundamental data.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()
        # Reset index so that 'ticker' and 'date' are explicit columns.
        price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]
        price_df['date'] = pd.to_datetime(price_df['date'])
        # Sort both dataframes for merge_asof.
        price_df = price_df.sort_values(['ticker', 'date'])
        merged_data = merged_data.sort_values(['ticker', 'date'])
        aligned = pd.merge_asof(
            merged_data,
            price_df[['ticker', 'date', 'close']],
            on='date',
            by='ticker',
            direction='backward'
        )
        return aligned[['ticker', 'date', 'close']]

    def _calculate_momentum(self, data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Calculate price momentum for each fundamental date using 6-month and 12-month returns.
        
        This function retrieves extended daily price data (covering the period needed to calculate
        the 6-month and 12-month returns) and uses merge_asof to align each fundamental date with
        the most recent closing price on or before (date - lookback_period).
        
        Args:
            data (pd.DataFrame): DataFrame containing at least ['ticker', 'date', 'close'].
            tickers (Union[str, List[str]]): Ticker(s) for which momentum is calculated.
        
        Returns:
            pd.DataFrame: Updated DataFrame (same as input) with a new column 'momentum' that is the 
                          average of the 6-month and 12-month return.
        """
        if data.empty:
            return data

        # Determine overall min and max fundamental dates.
        min_date = data['date'].min()
        max_date = data['date'].max()
        # Buffer to ensure the extended price data covers the full range.
        buffer_days = max(self.params['momentum_6m_days'], self.params['momentum_12m_days']) + 10
        extended_from_date = (min_date - pd.Timedelta(days=buffer_days)).strftime('%Y-%m-%d')
        extended_to_date = max_date.strftime('%Y-%m-%d')

        ext_price_df = self.get_historical_prices(tickers, from_date=extended_from_date, to_date=extended_to_date)
        if ext_price_df.empty:
            self.logger.warning("Extended price data retrieval failed for momentum calculation.")
            data['momentum'] = np.nan
            return data
        ext_price_df = ext_price_df.reset_index()
        if 'ticker' not in ext_price_df.columns:
            ext_price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]
        ext_price_df['date'] = pd.to_datetime(ext_price_df['date'])
        ext_price_df = ext_price_df.sort_values(['ticker', 'date'])

        # Create copies for calculating 6-month and 12-month target dates.
        df_mom = data.copy()
        df_mom['target_date_6'] = df_mom['date'] - pd.Timedelta(days=self.params['momentum_6m_days'])
        df_mom['target_date_12'] = df_mom['date'] - pd.Timedelta(days=self.params['momentum_12m_days'])

        # Calculate price 6 months ago using merge_asof.
        df_mom = df_mom.sort_values(['ticker', 'target_date_6'])
        mom6 = pd.merge_asof(
            df_mom,
            ext_price_df[['ticker', 'date', 'close']],
            left_on='target_date_6',
            right_on='date',
            by='ticker',
            direction='backward',
            suffixes=('', '_6')
        )
        mom6.rename(columns={'close': 'price_6'}, inplace=True)

        # Calculate price 12 months ago.
        df_mom = df_mom.sort_values(['ticker', 'target_date_12'])
        mom12 = pd.merge_asof(
            df_mom,
            ext_price_df[['ticker', 'date', 'close']],
            left_on='target_date_12',
            right_on='date',
            by='ticker',
            direction='backward',
            suffixes=('', '_12')
        )
        mom12.rename(columns={'close': 'price_12'}, inplace=True)

        # Add the retrieved historical prices to the main dataframe.
        df_mom['price_6'] = mom6['price_6'].values
        df_mom['price_12'] = mom12['price_12'].values

        # Calculate 6-month and 12-month momentum returns.
        df_mom['momentum_6'] = np.where(df_mom['price_6'] > 0,
                                        df_mom['close'] / df_mom['price_6'] - 1,
                                        np.nan)
        df_mom['momentum_12'] = np.where(df_mom['price_12'] > 0,
                                         df_mom['close'] / df_mom['price_12'] - 1,
                                         np.nan)
        # Average the two momentum measures.
        df_mom['momentum'] = (df_mom['momentum_6'] + df_mom['momentum_12']) / 2

        # Drop intermediate columns.
        df_mom.drop(columns=['target_date_6', 'target_date_12', 'price_6', 'price_12',
                             'momentum_6', 'momentum_12'], inplace=True)

        return df_mom