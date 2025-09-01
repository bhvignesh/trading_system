# trading_system/src/collectors/price_collector.py

import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

class PriceCollector(BaseCollector):
    """
    Retrieves and stores price data from yFinance.
    
    Provides methods for incremental updates and full historical refreshes, ensuring 
    data integrity through rigorous quality validation.
    """

    def fetch_and_save(self, ticker: str) -> None:
        """
        Fetch incremental price data and persist it to the database.
        
        Retrieves the most recent date of stored data for the ticker and fetches new data 
        from yFinance starting the following day. After processing and validating the data, 
        it appends the new records to the target table.
        
        Args:
            ticker (str): The stock ticker symbol.
        """
        try:
            table_name = "daily_prices"
            latest_date = self._get_latest_date(table_name, ticker)
            
            stock = yf.Ticker(ticker)
            
            if latest_date:
                # Calculate start date for incremental fetch
                start_date = latest_date + timedelta(days=1)
                
                # Only fetch if we need recent data
                # Use > so today's data is included
                if start_date.date() > datetime.now().date():
                    logger.debug(f"No new data needed for {ticker}")
                    return
                
                logger.debug(f"Fetching incremental data for {ticker} from {start_date.date()}")
                data = stock.history(start=start_date.strftime('%Y-%m-%d'))
                
                # Remove any overlap to prevent duplicates
                if not data.empty and latest_date:
                    data = data[data.index.tz_localize(None) > latest_date]
            else:
                logger.info(f"No existing data for {ticker}, fetching full history")
                data = stock.history(period="max")

            if data.empty:
                logger.debug(f"No new price data for {ticker}")
                return

            # Process and validate data
            processed_data = self._process_price_data(data, ticker)
            if processed_data.empty:
                logger.warning(f"No valid price data after processing for {ticker}")
                return

            # Ensure schema and save
            self._ensure_table_schema(table_name, processed_data)
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
            self._save_to_database(processed_data, table_name, required_columns)
            
            logger.debug(f"Successfully saved {len(processed_data)} price records for {ticker}")

        except Exception as e:
            logger.error(f"Error fetching price data for {ticker}: {e}")
            raise

    def refresh_data(self, ticker: str) -> None:
        """
        Refresh the complete historical price data for a ticker.
        
        Retrieves the entire historical dataset from yFinance, processes and validates the data, 
        and replaces any existing records in the database associated with the ticker.
        
        Args:
            ticker (str): The stock ticker symbol.
        """
        try:
            table_name = "daily_prices"
            
            # Fetch complete history
            stock = yf.Ticker(ticker)
            data = stock.history(period='max')

            if data.empty:
                logger.warning(f"No historical data available for {ticker}")
                return

            # Process data
            processed_data = self._process_price_data(data, ticker)
            if processed_data.empty:
                logger.warning(f"No valid data after processing for {ticker}")
                return

            # Replace existing data
            self._delete_existing_data(table_name, ticker)
            self._ensure_table_schema(table_name, processed_data)
            
            required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']
            self._save_to_database(processed_data, table_name, required_columns)
            
            logger.info(f"Successfully refreshed {len(processed_data)} price records for {ticker}")

        except Exception as e:
            logger.error(f"Error refreshing price data for {ticker}: {e}")
            raise

    def _process_price_data(self, data: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Process raw price data and enforce quality standards.
        
        Standardizes column names, enriches the data with metadata, and performs validations 
        to remove anomalies. The resulting data is chronologically sorted.
        
        Args:
            data (pd.DataFrame): Raw price data retrieved from yFinance.
            ticker (str): The stock ticker symbol.
        
        Returns:
            pd.DataFrame: The cleaned and processed price data.
        """
        if data.empty:
            return pd.DataFrame()
        
        df = data.reset_index().copy()
        
        # Standardize column names
        column_mapping = {
            'Date': 'date',
            'Open': 'open', 
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }
        df = df.rename(columns=column_mapping)
        
        # Add metadata
        df['ticker'] = ticker
        df['updated_at'] = datetime.now()
        df['data_source'] = 'yfinance'
        
        # Data quality validation
        df = self._validate_price_data(df, ticker)
        
        # Sort by date
        df = df.sort_values('date')
        
        return df

    def _validate_price_data(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Validate price data and eliminate erroneous records.
        
        Checks for missing, zero, or negative values in price columns; ensures logical consistency 
        (e.g., high not less than low); and removes records with abnormal volume spikes.
        
        Args:
            df (pd.DataFrame): The DataFrame containing price data.
            ticker (str): The stock ticker symbol.
        
        Returns:
            pd.DataFrame: The validated DataFrame with invalid records removed.
        """
        initial_rows = len(df)
        
        # Remove rows with invalid prices
        price_cols = ['open', 'high', 'low', 'close']
        df = df.dropna(subset=price_cols)
        
        # Remove rows where high < low (data error)
        df = df[df['high'] >= df['low']]
        
        # Remove rows with zero or negative prices
        for col in price_cols:
            df = df[df[col] > 0]
        
        # Remove rows with extremely high volume spikes (potential data errors)
        if 'volume' in df.columns and len(df) > 10:
            volume_median = df['volume'].median()
            volume_threshold = volume_median * 100  # 100x median volume
            extreme_volume_mask = df['volume'] > volume_threshold
            if extreme_volume_mask.any():
                logger.warning(f"Removing {extreme_volume_mask.sum()} rows with extreme volume for {ticker}")
                df = df[~extreme_volume_mask]
        
        # Check for price consistency (close should be between low and high)
        inconsistent_mask = (df['close'] < df['low']) | (df['close'] > df['high'])
        if inconsistent_mask.any():
            logger.warning(f"Removing {inconsistent_mask.sum()} rows with inconsistent prices for {ticker}")
            df = df[~inconsistent_mask]
        
        rows_removed = initial_rows - len(df)
        if rows_removed > 0:
            logger.info(f"Removed {rows_removed} invalid price records for {ticker}")
        
        return df

    def _delete_existing_data(self, table_name: str, ticker: str) -> None:
        """
        Delete all existing records for a ticker from the specified table.
        
        Executes a delete operation within a transactional scope and logs the number of records removed.
        
        Args:
            table_name (str): The name of the database table.
            ticker (str): The stock ticker symbol.
        """
        try:
            with self._db_connection() as conn:
                from sqlalchemy import inspect
                if inspect(conn).has_table(table_name):
                    from sqlalchemy import text
                    query = text(f"DELETE FROM {table_name} WHERE ticker = :ticker")
                    result = conn.execute(query, {"ticker": ticker})
                    deleted_count = result.rowcount
                    logger.debug(f"Deleted {deleted_count} existing records for {ticker}")
        except Exception as e:
            logger.error(f"Error deleting existing data for {ticker}: {e}")
            raise