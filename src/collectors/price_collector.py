# trading_system/src/collectors/price_collector.py

import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from src.collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)

class PriceCollector(BaseCollector):
    """Collect price data using yfinance and store it in the database."""
    
    def fetch_price_data(self, ticker: str) -> None:
        """
        Fetch additional daily prices for a ticker and save to the database.
        
        Args:
            ticker: Ticker symbol.
        """
        try:
            table_name = "daily_prices"
            # Retrieve the latest date from the database using the BaseCollector method.
            latest_date = self._get_latest_date(table_name, ticker)
            stock = yf.Ticker(ticker)

            if latest_date:
                start_date = latest_date + timedelta(days=1)
                logger.info(
                    f"Fetching daily prices for {ticker} starting from {start_date.strftime('%Y-%m-%d')}."
                )
                data = stock.history(start=start_date.strftime('%Y-%m-%d'))
            else:
                logger.info(f"No latest date found for {ticker}. Fetching data from the beginning.")
                data = stock.history(period="max")

            if data.empty:
                logger.warning(f"No new price data available for {ticker}.")
                return

            # Process and standardize the data:
            data = (
                data.reset_index()
                    .rename(columns={
                        'Date': 'date',
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
            )
            data['ticker'] = ticker
            data['updated_at'] = datetime.now()
            data['data_source'] = 'yfinance'

            # Standardize column names
            data.columns = (
                data.columns.str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace(r'[^\w_]', '', regex=True)
            )

            required_columns = [
                'date', 'open', 'high', 'low', 'close', 'volume',
                'ticker', 'updated_at', 'data_source'
            ]
            
            # Ensure the database table schema matches the DataFrame.
            self._ensure_table_schema(table_name, data)
            # Save the new data.
            self._save_to_database(data, table_name, required_columns)
            logger.info(f"Successfully fetched daily prices for {ticker}.")
            
        except Exception as e:
            logger.error(f"Error fetching daily prices for {ticker}: {e}")
            raise

    def refresh_data(self, ticker: str) -> None:
        """
        Refresh data for a specific ticker by deleting all existing data and replacing it with new data.
        
        Args:
            ticker: Ticker symbol.
        """
        try:
            table_name = "daily_prices"
            stock = yf.Ticker(ticker)
            data = stock.history(period='max')

            if data.empty:
                logger.warning(f"No data available for {ticker} in {table_name}.")
                return

            # Process and standardize the data:
            data = (
                data.reset_index()
                    .rename(columns={
                        'Date': 'date',
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
            )
            data['ticker'] = ticker
            data['updated_at'] = datetime.now()
            data['data_source'] = 'yfinance'

            # Standardize column names
            data.columns = (
                data.columns.str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace(r'[^\w_]', '', regex=True)
            )

            required_columns = [
                'date', 'open', 'high', 'low', 'close', 'volume',
                'ticker', 'updated_at', 'data_source'
            ]

            # Delete existing data for the ticker.
            self._delete_existing_data(table_name, ticker)
            # Ensure the table schema matches the new data.
            self._ensure_table_schema(table_name, data)
            # Save the new data.
            self._save_to_database(data, table_name, required_columns)
            logger.info(f"Successfully refreshed data for {ticker} in {table_name}.")

        except Exception as e:
            logger.error(f"Error refreshing data for {ticker}: {e}")
            raise
