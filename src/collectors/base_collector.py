# trading_system/src/collectors/base_collector.py

from typing import Optional
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.sql import text
from abc import ABC, abstractmethod
import logging
from contextlib import contextmanager
from sqlalchemy import exc, inspect, DDL
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger(__name__)

class BaseCollector(ABC):
    """Base class for collecting financial data."""

    def __init__(self, db_engine):
        """
        Initialize collector with database engine and configuration.
        
        Args:
            db_engine: SQLAlchemy engine object.
        """
        self.engine = db_engine

    @contextmanager
    def _db_connection(self, transactional: bool = True):
        """
        Context manager for database connections with optional transactions.
        
        Args:
            transactional (bool): If True, uses a transactional connection (via engine.begin()).
                                  Otherwise, uses a standard connection (engine.connect()).
        """
        if transactional:
            with self.engine.begin() as connection:
                yield connection
        else:
            with self.engine.connect() as connection:
                yield connection

    def _validate_dataframe(self, df: pd.DataFrame, required_columns: list) -> bool:
        """
        Validate DataFrame structure and content.
        
        Args:
            df: DataFrame to validate.
            required_columns: List of required column names.
            
        Returns:
            bool: True if validation passes, False otherwise.
        """
        if df is None or df.empty:
            return False
            
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        return True

    def _delete_existing_data(self, table_name: str, ticker: str) -> None:
        """
        Delete existing data for a specific ticker.
        """
        if not inspect(self.engine).has_table(table_name):
            return  # Skip deletion if table doesn't exist
        
        query = text(f"DELETE FROM {table_name} WHERE ticker = :ticker")
        
        try:
            with self._db_connection() as conn:
                conn.execute(query, {"ticker": ticker})
            logger.info(f"Deleted existing data for {ticker} in {table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error deleting data for {ticker} in {table_name}: {e}")
            raise

    def _get_latest_date(self, table_name: str, ticker: str) -> Optional[datetime]:
        """
        Get the latest date for a ticker.
        """
        query = text(f"SELECT MAX(date) FROM {table_name} WHERE ticker = :ticker")
        
        try:
            # Use a non-transactional connection for read-only operations.
            with self._db_connection(transactional=False) as conn:
                result = conn.execute(query, {"ticker": ticker}).scalar()
                return pd.to_datetime(result) if result else None
        except exc.OperationalError as e:
            if "no such table" in str(e).lower():
                return None
            else:
                logger.error(f"Operational error getting latest date for {ticker}: {e}")
                raise
        except SQLAlchemyError as e:
            logger.error(f"Error getting latest date for {ticker}: {e}")
            raise

    @retry(stop=stop_after_attempt(3),
           wait=wait_fixed(1),
           retry=retry_if_exception_type(OperationalError))
    def _save_to_database(self, df: pd.DataFrame, table_name: str, 
                         required_columns: list) -> None:
        """
        Save DataFrame to database with validation.
        
        Args:
            df: DataFrame to save.
            table_name: Target table name.
            required_columns: List of required columns.
        """
        try:
            if not self._validate_dataframe(df, required_columns):
                raise ValueError(f"Invalid data for {table_name}")

            # Handle NaN values consistently
            df = df.replace([pd.NA, pd.NaT], None)
            
            with self._db_connection() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
            logger.info(f"Successfully saved {len(df)} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving data to {table_name}: {e}")
            raise

    def _ensure_table_schema(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Ensure the database table schema matches the DataFrame columns.
        
        This method uses an atomic table creation statement with IF NOT EXISTS
        to avoid race conditions when multiple threads attempt to create the same table.
        If the table already exists, any missing columns (based on the DataFrame) are added.
        
        Args:
            table_name: Name of the database table.
            data: DataFrame whose columns are used to verify or build the table schema.
        """
        with self._db_connection() as conn:
            inspector = inspect(conn)
            
            # If the table does not exist, create it atomically using IF NOT EXISTS.
            if not inspector.has_table(table_name):
                columns = []
                for col in data.columns:
                    if pd.api.types.is_integer_dtype(data[col]):
                        col_type = "INTEGER"
                    elif pd.api.types.is_float_dtype(data[col]):
                        col_type = "FLOAT"  # You might also consider "DOUBLE" for precision.
                    elif pd.api.types.is_datetime64_any_dtype(data[col]):
                        col_type = "TIMESTAMP"  # Updated for DuckDB compatibility.
                    else:
                        col_type = "VARCHAR"
                    
                    # Quote column names to handle reserved words or special characters.
                    columns.append(f'"{col}" {col_type}')
                
                create_table = DDL(
                    f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        {', '.join(columns)}
                    )"""
                )
                
                try:
                    conn.execute(create_table)
                    logger.info(f"Created table {table_name} with IF NOT EXISTS")
                except SQLAlchemyError as e:
                    logger.error(f"Error creating table {table_name}: {e}")
                    raise
                return  # Table created successfully.
            
            # If the table already exists, check for missing columns.
            existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
            missing_columns = [col for col in data.columns if col not in existing_columns]

            if missing_columns:
                preparer = self.engine.dialect.identifier_preparer
                for column in missing_columns:
                    if pd.api.types.is_integer_dtype(data[column]):
                        col_type = "INTEGER"
                    elif pd.api.types.is_float_dtype(data[column]):
                        col_type = "FLOAT"  # Or "DOUBLE"
                    elif pd.api.types.is_datetime64_any_dtype(data[column]):
                        col_type = "TIMESTAMP"  # Updated for DuckDB.
                    else:
                        col_type = "VARCHAR"
                        
                    column_quoted = preparer.quote(column)
                    alter_sql = f'ALTER TABLE {table_name} ADD COLUMN {column_quoted} {col_type}'
                    try:
                        conn.execute(text(alter_sql))
                        logger.info(f"Added column {column_quoted} to {table_name}")
                    except exc.OperationalError as e:
                        if "duplicate column" in str(e).lower():
                            logger.warning(f"Column {column} already exists in {table_name}, skipping.")
                        else:
                            logger.error(f"Error adding column {column}: {e}")
                            raise

    @abstractmethod
    def refresh_data(self, ticker: str, **kwargs) -> None:
        """
        Refresh data for a specific ticker.
        
        Args:
            ticker: Ticker symbol.
            **kwargs: Additional arguments specific to each collector.
        """
        pass
