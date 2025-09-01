# trading_system/src/strategies/value/schloss_low_pb_strategy.py

# TODO: Verify

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class SchlossLowPBScreenerStrategy(BaseStrategy):
    """
    Implementation of Schloss' Low P/B Screener strategy.

    This strategy computes the Price-to-Book (P/B) ratio for one or more tickers using 
    historical Balance Sheet data as the primary source. The strategy aligns each balance sheet
    date with historical closing prices and calculates the P/B ratio as:

        Book Value Per Share = stockholders_equity / ordinary_shares_number
        Price-to-Book Ratio  = closing price / book value per share

    A binary signal is generated using a pre-specified threshold (default: 1.0 for the Indian market):
      - Signal = 1 if the calculated P/B ratio is less than or equal to the threshold,
      - Signal = 0 otherwise.

    If insufficient balance sheet data is available (with at least the required columns),
    the strategy falls back to company info.

    Attributes:
        params (dict): Dictionary containing strategy parameters:
            - 'max_pb': Maximum acceptable P/B ratio threshold (default: 1.0).
            - 'fallback_to_company_info': Whether to use company info if balance sheet data is insufficient (default: True).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Schloss Low P/B Screener strategy.

        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override default parameters.
        """
        default_params = {
            'max_pb': 1.0,  # Maximum acceptable P/B ratio threshold.
            'fallback_to_company_info': True  # Fallback to company_info if balance sheet data is insufficient.
        }
        if params:
            default_params.update(params)
        super().__init__(db_config, default_params)
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
        Generate historical trading signals based on Schloss' Low P/B Screener.
        
        Retrieves balance sheet data for the provided ticker(s), aligns each financial statement date with the 
        corresponding historical closing price, calculates the book value per share and the resulting
        Price-to-Book (P/B) ratio, and then produces a binary signal based on the configured threshold.

        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) filter.
            end_date (str, optional): End date (YYYY-MM-DD) filter.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent signal per ticker.

        Returns:
            pd.DataFrame: DataFrame containing the following columns:
                - 'ticker': Stock ticker.
                - 'date': Date corresponding to the financial statement.
                - 'close': Historical closing price.
                - 'stockholders_equity': Equity value from balance sheet.
                - 'ordinary_shares_number': Number of ordinary shares.
                - 'book_value_per_share': Computed book value per share.
                - 'price_to_book': Final P/B ratio (final score).
                - 'signal': Binary trading signal (1 if price_to_book ≤ max_pb, else 0).
        """
        try:
            # Retrieve all available historical balance sheet data.
            bs_raw = self.get_financials(tickers, 'balance_sheet', lookback=None)

            # Ensure that the required columns are available.
            required_columns = ['stockholders_equity', 'ordinary_shares_number']
            if bs_raw.empty or not all(col in bs_raw.columns for col in required_columns):
                self.logger.warning(
                    f"Insufficient balance sheet data for {tickers}. "
                    "Falling back to company info."
                )
                return self._fallback_to_company_info(tickers, start_date, end_date, latest_only)

            # Preprocess the balance sheet data.
            bs_data = self._prepare_balance_sheet(bs_raw)

            # Retrieve and align historical price data based on balance sheet dates.
            price_data = self._get_price_data(bs_data, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed.")
                return pd.DataFrame()

            # Calculate key metrics including book value per share and the P/B ratio.
            metrics = self._calculate_metrics(bs_data, price_data)
            if metrics.empty:
                return pd.DataFrame()

            # Apply date range filtering if provided.
            if start_date:
                metrics = metrics[metrics['date'] >= pd.to_datetime(start_date)]
            if end_date:
                metrics = metrics[metrics['date'] <= pd.to_datetime(end_date)]

            # Generate a binary signal: 1 if P/B ratio is less than or equal to the threshold.
            metrics['signal'] = self._generate_signal_column(metrics)

            # If only the latest signal is desired, select the most recent record per ticker.
            if latest_only:
                if isinstance(tickers, list) or 'ticker' in metrics.columns:
                    metrics = metrics.sort_values('date').groupby('ticker').tail(1)
                else:
                    metrics = metrics.sort_values('date').tail(1)

            return metrics.reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _prepare_balance_sheet(self, bs: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess raw balance sheet data to ensure proper formatting.
        
        Ensures that 'ticker' and 'date' columns exist (resetting the index if needed) and that
        the 'date' column is in datetime format.

        Args:
            bs (pd.DataFrame): Raw balance sheet data.

        Returns:
            pd.DataFrame: Processed balance sheet DataFrame sorted by ticker and date.
        """
        if not isinstance(bs.index, pd.MultiIndex):
            bs = bs.reset_index()
        if 'ticker' not in bs.columns:
            bs['ticker'] = 'N/A'
        bs['date'] = pd.to_datetime(bs['date'])
        bs = bs.sort_values(['ticker', 'date'])
        return bs

    def _get_price_data(
        self,
        fs_data: pd.DataFrame,
        tickers: Union[str, List[str]]
    ) -> pd.DataFrame:
        """
        Retrieve and align historical closing price data for the balance sheet dates.
        
        Uses the base strategy's get_historical_prices method to pull closing prices over the date range
        determined by the balance sheet data, and aligns each financial statement date with the most recent
        available closing price (using a merge_asof).
        
        Args:
            fs_data (pd.DataFrame): Processed balance sheet data containing 'date' and 'ticker'.
            tickers (Union[str, List[str]]): A single ticker or list of tickers.

        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'] containing aligned closing prices.
        """
        if fs_data.empty:
            return pd.DataFrame()

        # Determine the date range from the financial statement dates.
        min_date = fs_data['date'].min().strftime('%Y-%m-%d')
        max_date = fs_data['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        # Ensure 'ticker' and 'date' are explicit columns.
        price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            if isinstance(tickers, str):
                price_df['ticker'] = tickers
            else:
                price_df['ticker'] = tickers[0]
        if 'date' not in price_df.columns and 'Date' in price_df.columns:
            price_df.rename(columns={'Date': 'date'}, inplace=True)
        price_df['date'] = pd.to_datetime(price_df['date'])

        price_df.sort_values(['ticker', 'date'], inplace=True)
        fs_data.sort_values(['ticker', 'date'], inplace=True)

        # Align the balance sheet dates with the most recent closing price.
        aligned = pd.merge_asof(
            fs_data,
            price_df[['ticker', 'date', 'close']],
            on='date',
            by='ticker',
            direction='backward'
        )
        return aligned[['ticker', 'date', 'close']]

    def _calculate_metrics(
        self,
        bs_data: pd.DataFrame,
        price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate key metrics: book value per share and the Price-to-Book (P/B) ratio.
        
        Calculations:
            - Book Value Per Share = stockholders_equity / ordinary_shares_number
            - Price-to-Book Ratio = close / book value per share

        Rows with invalid data (e.g. zero or missing ordinary_shares_number) are filtered out.

        Args:
            bs_data (pd.DataFrame): Processed balance sheet data.
            price_data (pd.DataFrame): Aligned closing price data.

        Returns:
            pd.DataFrame: DataFrame containing the following columns:
                ['ticker', 'date', 'close', 'stockholders_equity', 'ordinary_shares_number',
                 'book_value_per_share', 'price_to_book']
        """
        # Merge balance sheet data with price data on 'ticker' and 'date'.
        data = pd.merge(bs_data, price_data, on=['ticker', 'date'], how='left')
        if data.empty or data['close'].isnull().all():
            self.logger.warning("Price data alignment failed: no matching close prices found.")
            return pd.DataFrame()

        # Avoid division by zero by replacing zero ordinary_shares_number with NaN.
        data['ordinary_shares_number'] = data['ordinary_shares_number'].replace(0, np.nan)
        data['book_value_per_share'] = data['stockholders_equity'] / data['ordinary_shares_number']
        data['price_to_book'] = data['close'] / data['book_value_per_share']

        # Filter out rows with non-positive book value per share or missing P/B ratio.
        valid = data[
            (data['book_value_per_share'] > 0) &
            (data['price_to_book'].notnull())
        ].copy()

        return valid[['ticker', 'date', 'close', 'stockholders_equity', 'ordinary_shares_number',
                      'book_value_per_share', 'price_to_book']]

    def _generate_signal_column(self, df: pd.DataFrame) -> np.ndarray:
        """
        Generate binary trading signals based on the P/B ratio threshold.
        
        A signal of 1 is generated if the Price-to-Book ratio is less than or equal to the configured threshold,
        and 0 otherwise.

        Args:
            df (pd.DataFrame): DataFrame containing the 'price_to_book' column.

        Returns:
            np.ndarray: Array of binary signals.
        """
        return np.where(df['price_to_book'] <= self.params['max_pb'], 1, 0).astype(int)

    def _fallback_to_company_info(
        self,
        tickers: Union[str, List[str]],
        start_date: Optional[str],
        end_date: Optional[str],
        latest_only: bool
    ) -> pd.DataFrame:
        """
        Fallback method to derive the P/B ratio from company info when balance sheet data is insufficient.
        
        Uses company_info fields such as 'currentprice', 'bookvalue', or directly 'pricetobook' (if available)
        to compute the P/B ratio. Note that since company_info usually provides only a single record per
        ticker, this output will not include a trend over time.

        Args:
            tickers (Union[str, List[str]]): Ticker or list of tickers.
            start_date (str, optional): Start date (YYYY-MM-DD) filter.
            end_date (str, optional): End date (YYYY-MM-DD) filter.
            latest_only (bool): If True, returns only the most recent record per ticker.

        Returns:
            pd.DataFrame: DataFrame with columns:
                ['ticker', 'date', 'currentprice', 'bookvalue', 'price_to_book', 'signal']
        """
        try:
            info = self.get_company_info(tickers)
            if isinstance(info, pd.Series):
                info = info.to_frame().T
            if 'ticker' not in info.columns:
                info = info.reset_index()
            # Use the updated_at field as the reference date if available.
            if 'updated_at' in info.columns:
                info['date'] = pd.to_datetime(info['updated_at'])
            else:
                info['date'] = pd.Timestamp.today()

            # Compute the P/B ratio from company info. If the 'pricetobook' column is numeric, we use that.
            if 'pricetobook' in info.columns and pd.api.types.is_numeric_dtype(info['pricetobook']):
                info['price_to_book'] = info['pricetobook']
            else:
                # Avoid division by zero.
                info['bookvalue'] = info['bookvalue'].replace(0, np.nan)
                info['price_to_book'] = info['currentprice'] / info['bookvalue']

            info['signal'] = np.where(info['price_to_book'] <= self.params['max_pb'], 1, 0)

            result = info[['ticker', 'date', 'currentprice', 'bookvalue', 'price_to_book', 'signal']]

            # Apply date filters if provided.
            if start_date:
                result = result[result['date'] >= pd.to_datetime(start_date)]
            if end_date:
                result = result[result['date'] <= pd.to_datetime(end_date)]
            if latest_only:
                result = result.sort_values('date').groupby('ticker').tail(1)

            return result.reset_index(drop=True)
        except Exception as e:
            self.logger.error(f"Fallback using company info failed: {str(e)}", exc_info=True)
            return pd.DataFrame()