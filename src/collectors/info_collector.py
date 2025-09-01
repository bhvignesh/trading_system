# trading_system/src/collectors/info_collector.py

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd
import yfinance as yf
from sqlalchemy import exc, text
from tenacity import retry, stop_after_attempt, wait_exponential

from src.collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class InfoCollector(BaseCollector):
    """Collect company information using yfinance and store it in the database."""

    def _flatten_nested_dict(self, nested_dict: Dict) -> Dict:
        """
        Flatten a nested dictionary structure.
        
        Args:
            nested_dict: A dictionary with potential nested structures.

        Returns:
            A flat dictionary with nested keys concatenated.
        """
        flat_dict = {}

        def flatten(d, parent_key=''):
            for k, v in d.items():
                new_key = f"{parent_key}_{k}" if parent_key else k
                if isinstance(v, dict):
                    flatten(v, new_key)
                else:
                    flat_dict[new_key] = v

        flatten(nested_dict)
        return flat_dict
    
    def _get_last_update_date(self, table_name: str, ticker: str) -> Optional[datetime]:
        """Get the last updated date for a specific ticker."""
        query = text(f"SELECT MAX(updated_at) FROM {table_name} WHERE ticker = :ticker")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"ticker": ticker}).scalar()
                return pd.to_datetime(result) if result else None
        except exc.OperationalError as e:
            if "no such table" in str(e):
                return None
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_company_info(self, ticker: str, table_name: str = "company_info") -> None:
        """
        Fetch company information and save it to the database if outdated or missing.
        
        Args:
            ticker: Ticker symbol.
            table_name: The name of the database table (default: 'company_info').
        """
        try:
            # Check the last update date
            last_update = self._get_last_update_date(table_name, ticker)
            if last_update and (datetime.now() - last_update < timedelta(days=30)):
                logger.info(f"Skipping {ticker}: Data is less than 30 days old.")
                return

            # Fetch company information using yfinance
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info:
                logger.warning(f"No company info available for {ticker}.")
                return
            
            # Convert lists/dicts to JSON strings
            for key, value in info.items():
                if isinstance(value, (list, dict)):
                    info[key] = json.dumps(value)

            # Flatten and convert to DataFrame
            flat_info = self._flatten_nested_dict(info)
            info_df = pd.DataFrame([flat_info])
            info_df['ticker'] = ticker
            info_df['updated_at'] = datetime.now()
            info_df['data_source'] = 'yfinance'

            # Ensure these columns exist even if source data is missing
            for col in ['ticker', 'updated_at', 'data_source']:
                if col not in info_df.columns:
                    info_df[col] = None

            info_df.columns = (
                info_df.columns.str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace(r'[^\w_]', '', regex=True)
            )

            # Ensure schema matches the DataFrame
            self._ensure_table_schema(table_name, info_df)

            # Save data to the database
            required_columns = ['ticker', 'updated_at']
            self._save_to_database(info_df, table_name, required_columns)
            logger.info(f"Successfully fetched and saved data for {ticker} in {table_name}.")

        except Exception as e:
            logger.error(f"Error fetching company info for {ticker}: {e}")
            raise

    def refresh_data(self, ticker: str, table_name: str = "company_info") -> None:
        """
        Refresh company information for a specific ticker by calling `fetch_company_info`.
        
        Args:
            ticker: Ticker symbol.
            table_name: The name of the database table (default: 'company_info').
        """
        try:
            # Simply fetch company info, as fetch_company_info handles refreshing logic
            self.fetch_company_info(ticker, table_name)
            logger.info(f"Successfully refreshed data for {ticker} in {table_name}.")
        except Exception as e:
            logger.error(f"Error refreshing data for {ticker} in {table_name}: {e}")
            raise

    def fetch_and_save(self, ticker: str) -> None:
        """
        Fetches and saves company info. For this collector, it is the same as a full refresh.
        
        Args:
            ticker: Ticker symbol.
        """
        # For company info, incremental fetch is the same as a refresh.
        self.refresh_data(ticker)
