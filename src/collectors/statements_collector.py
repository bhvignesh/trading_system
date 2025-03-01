# trading_system/src/collectors/statements_collector.py
import logging
import yfinance as yf
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime, timedelta
from src.collectors.base_collector import BaseCollector
from typing import Callable

logger = logging.getLogger(__name__)

class StatementsCollector(BaseCollector):
    """Collect financial statements using yfinance and store them in the database."""

    def __init__(self, db_engine):
        """
        Initialize the StatementsCollector with a database engine.
            
        """
        super().__init__(db_engine)
        self.financial_statements = [
            ('balance_sheet', lambda stock: stock.balance_sheet),
            ('income_statement', lambda stock: stock.income_stmt),
            ('cash_flow', lambda stock: stock.cash_flow)
        ]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_financial_statement(self, ticker: str, statement_type: str, fetch_function: Callable) -> None:
        """
        Fetch and process financial statements.

        Args:
            ticker: Ticker symbol
            statement_type: Type of statement (e.g., 'balance_sheet')
            fetch_function: Function to fetch the statement from yfinance
        """
        try:
            table_name = statement_type.lower()
            latest_date = self._get_latest_date(table_name, ticker)

            # Skip fetching if data is already up-to-date
            if latest_date and (datetime.now() - latest_date < timedelta(days=40)):
                logger.info(f"Skipping {ticker} {statement_type} - data is up to date.")
                return

            # Fetch financial data
            stock = yf.Ticker(ticker)
            data = fetch_function(stock)

            # Handle cases where no data is returned
            if data is None:
                logger.warning(f"No data returned for {ticker} {statement_type}")
                return
            if not isinstance(data, pd.DataFrame):
                logger.warning(f"Unexpected data type {type(data)} for {statement_type}")
                return
            if data is None or data.empty:
                logger.warning(f"No {statement_type} data available for {ticker}.")
                return

            # Process the data
            data = data.fillna(pd.NA).infer_objects(copy=False).T 
            data['ticker'] = ticker
            data = data.reset_index().rename(columns={'index': 'date'})

            # Filter out old data if latest_date is available
            if latest_date:
                data = data[data['date'] > latest_date]

            if not data.empty:
                # Add metadata columns
                data['updated_at'] = datetime.now()
                data['data_source'] = 'yfinance'

                data.columns = (
                    data.columns.str.strip()
                    .str.lower()
                    .str.replace(' ', '_')
                    .str.replace(r'[^\w_]', '', regex=True)
                )

                # Dynamically update database schema if needed
                self._ensure_table_schema(table_name, data)

                # Save to database
                required_columns = ['date', 'ticker', 'updated_at']
                self._save_to_database(data, table_name, required_columns)

        except Exception as e:
            logger.error(f"Error processing {statement_type} for {ticker}: {e}")
            raise

    def refresh_data(self, ticker: str) -> None:
        """Intentionally left empty - statements don't need refreshing"""
        pass
