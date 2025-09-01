# trading_system/src/collectors/base_collector.py

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_fixed)

logger = logging.getLogger(__name__)

class BaseCollector(ABC):
    """
    Abstract base class for data collectors that implement state tracking, bulk operations,
    schema evolution, and signal storage. Provides common functionality for managing system
    tables and database interactions.
    """

    def __init__(self, db_engine: Engine):
        """
        Initialize the BaseCollector with a database engine.

        Args:
            db_engine (Engine): The SQLAlchemy database engine instance.
        """
        self.engine = db_engine
        self._ensure_system_tables()

    def _ensure_system_tables(self):
        """
        Ensure required system tables exist in the database.

        Creates tables for maintaining refresh state, storing trading signals, and tracking
        strategy performance. The tables will be created if they do not already exist.
        """
        
        # Refresh state tracking table
        refresh_state_sql = """
        CREATE TABLE IF NOT EXISTS refresh_state (
            ticker VARCHAR(20) PRIMARY KEY,
            last_full_refresh DATE,
            refresh_cycle INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Signal storage table for ensemble methods
        signals_sql = """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            strategy_name VARCHAR(100) NOT NULL,
            signal INTEGER,
            signal_strength FLOAT,
            confidence FLOAT,
            metadata TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, date, strategy_name)
        )
        """
        
        # Strategy performance tracking
        strategy_performance_sql = """
        CREATE TABLE IF NOT EXISTS strategy_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name VARCHAR(100) NOT NULL,
            ticker VARCHAR(20) NOT NULL,
            start_date DATE,
            end_date DATE,
            total_return FLOAT,
            sharpe_ratio FLOAT,
            max_drawdown FLOAT,
            win_rate FLOAT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        with self.engine.connect() as conn:
            for sql in [refresh_state_sql, signals_sql, strategy_performance_sql]:
                try:
                    conn.execute(text(sql))
                except SQLAlchemyError as e:
                    logger.warning(f"Error creating system table: {e}")
            conn.commit()

    @contextmanager
    def _db_connection(self):
        """
        Provide a transactional database connection.

        Yields:
            A database connection with an active transaction. Commits the transaction on
            success; otherwise, rolls back on error.
        """
        connection = self.engine.connect()
        transaction = connection.begin()
        try:
            yield connection
            transaction.commit()
        except Exception:
            transaction.rollback()
            raise
        finally:
            connection.close()

    def get_refresh_state(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve the refresh state for the specified ticker.

        Args:
            ticker (str): The ticker identifier.

        Returns:
            Optional[Dict[str, Any]]: A dictionary with refresh state details if found, else None.
        """
        query = text("""
            SELECT ticker, last_full_refresh, refresh_cycle, updated_at
            FROM refresh_state 
            WHERE ticker = :ticker
        """)
        
        with self._db_connection() as conn:
            result = conn.execute(query, {"ticker": ticker}).fetchone()
            if result:
                return {
                    "ticker": result[0],
                    "last_full_refresh": result[1],
                    "refresh_cycle": result[2],
                    "updated_at": result[3]
                }
        return None

    def update_refresh_state(self, ticker: str, refresh_date: datetime, cycle: int):
        """
        Update the refresh state for a given ticker.

        Args:
            ticker (str): The ticker identifier.
            refresh_date (datetime): The date of the refresh.
            cycle (int): The current refresh cycle count.
        """
        upsert_query = text("""
            INSERT OR REPLACE INTO refresh_state 
            (ticker, last_full_refresh, refresh_cycle, updated_at)
            VALUES (:ticker, :refresh_date, :cycle, :updated_at)
        """)
        
        with self._db_connection() as conn:
            conn.execute(upsert_query, {
                "ticker": ticker,
                "refresh_date": refresh_date.date(),
                "cycle": cycle,
                "updated_at": datetime.now()
            })

    def get_tickers_for_refresh_cycle(self, all_tickers: List[str], cycle_day: int) -> List[str]:
        """
        Determine which tickers should be refreshed on a specified cycle day.

        Uses a deterministic hash-based grouping to consistently assign tickers across runs.

        Args:
            all_tickers (List[str]): The complete list of ticker identifiers.
            cycle_day (int): The day in the cycle (1-based) for which to select tickers.

        Returns:
            List[str]: A list of tickers scheduled for refresh on the given cycle day.
        """
        if not all_tickers:
            return []
            
        # Group tickers into 30 buckets based on hash
        ticker_buckets = {}
        for ticker in all_tickers:
            bucket = hash(ticker) % 30
            if bucket not in ticker_buckets:
                ticker_buckets[bucket] = []
            ticker_buckets[bucket].append(ticker)
        
        # Return tickers for the current cycle day
        cycle_bucket = (cycle_day - 1) % 30  # Convert to 0-29 range
        return ticker_buckets.get(cycle_bucket, [])

    def save_signals(self, signals_df: pd.DataFrame, strategy_name: str):
        """
        Store trading signals in the database.

        The method enriches the DataFrame with the strategy name and timestamp before performing
        a bulk insert. If required columns are missing, an error is logged.

        Args:
            signals_df (pd.DataFrame): DataFrame containing signal data.
            strategy_name (str): The name of the strategy generating these signals.
        """
        if signals_df.empty:
            return
            
        # Prepare signals data
        signals_data = signals_df.copy()
        signals_data['strategy_name'] = strategy_name
        signals_data['created_at'] = datetime.now()
        
        # Ensure required columns exist
        required_cols = ['ticker', 'date', 'signal', 'signal_strength']
        missing_cols = [col for col in required_cols if col not in signals_data.columns]
        if missing_cols:
            logger.error(f"Missing required columns for signals: {missing_cols}")
            return
            
        try:
            with self._db_connection() as conn:
                signals_data.to_sql(
                    'signals',
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            logger.info(f"Saved {len(signals_data)} signals for strategy {strategy_name}")
        except Exception as e:
            logger.error(f"Error saving signals: {e}")

    def _validate_dataframe(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate a DataFrame ensuring required columns are present and data quality is acceptable.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            required_columns (List[str]): List of expected column names.

        Returns:
            bool: True if the DataFrame meets validation criteria, otherwise False.
        """
        if df is None or df.empty:
            logger.warning("DataFrame is empty or None")
            return False
            
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        # Check for data quality issues
        if df.isnull().all().any():
            null_cols = df.columns[df.isnull().all()].tolist()
            logger.warning(f"Columns with all null values: {null_cols}")
            
        return True

    def _ensure_table_schema(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Verify and adjust the database table schema to match the DataFrame structure.

        Creates the table if it does not exist or adds new columns to accommodate evolved schema.

        Args:
            table_name (str): The name of the table.
            data (pd.DataFrame): Source DataFrame to infer schema from.
        """
        with self.engine.connect() as conn:
            inspector = inspect(conn)

            if not inspector.has_table(table_name):
                self._create_table_from_dataframe(conn, table_name, data)
                return

            # Handle schema evolution for existing tables
            existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
            new_columns = [col for col in data.columns if col not in existing_columns]

            if new_columns:
                self._add_columns_to_table(conn, table_name, data, new_columns)

    def _create_table_from_dataframe(self, conn, table_name: str, data: pd.DataFrame):
        """
        Create a new table based on the DataFrame schema.

        Infers SQL column types for each DataFrame column and executes the creation statement.

        Args:
            conn: The active database connection.
            table_name (str): The name of the table to be created.
            data: DataFrame used to derive the table schema.
        """
        columns = []
        for col in data.columns:
            sql_type = self._infer_sql_type(data[col])
            columns.append(f'"{col}" {sql_type}')
        
        create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        
        try:
            conn.execute(text(create_sql))
            logger.info(f"Created table {table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def _infer_sql_type(self, series: pd.Series) -> str:
        """
        Infer an appropriate SQL column type based on a pandas Series.

        Considers the data type and content length (for strings) to determine the column type.

        Args:
            series (pd.Series): The Series to analyze.

        Returns:
            str: The corresponding SQL column type.
        """
        # Check boolean dtype before integer to avoid bools being treated as
        # integers by pandas type checks.
        if pd.api.types.is_bool_dtype(series):
            return "BOOLEAN"
        elif pd.api.types.is_integer_dtype(series):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(series):
            return "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "DATETIME"
        else:
            # For strings, try to estimate appropriate VARCHAR length
            if series.dtype == 'object':
                max_len = series.astype(str).str.len().max()
                if pd.isna(max_len) or max_len <= 255:
                    return "VARCHAR(255)"
                else:
                    return "TEXT"
            return "TEXT"

    def _add_columns_to_table(self, conn, table_name: str, data: pd.DataFrame, new_columns: List[str]):
        """
        Add new columns to an existing table based on the DataFrame structure.

        Args:
            conn: The active database connection.
            table_name (str): The name of the table to be altered.
            data: DataFrame containing the new columns.
            new_columns (List[str]): List of column names to add.
        """
        preparer = self.engine.dialect.identifier_preparer
        
        for column in new_columns:
            sql_type = self._infer_sql_type(data[column])
            column_quoted = preparer.quote(column)
            alter_sql = f'ALTER TABLE {table_name} ADD COLUMN {column_quoted} {sql_type}'
            
            try:
                conn.execute(text(alter_sql))
                logger.info(f"Added column {column_quoted} to {table_name}")
            except OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    logger.error(f"Error adding column {column_quoted}: {e}")
                    raise

    def _get_latest_date(self, table_name: str, ticker: str) -> Optional[datetime]:
        """
        Retrieve the latest date for a given ticker from the specified table.

        Args:
            table_name (str): The table to query.
            ticker (str): The ticker identifier.

        Returns:
            Optional[datetime]: The most recent date if available, otherwise None.
        """
        try:
            with self._db_connection() as conn:
                if not inspect(conn).has_table(table_name):
                    return None

                query = text(f"SELECT MAX(date) FROM {table_name} WHERE ticker = :ticker")
                result = conn.execute(query, {"ticker": ticker}).scalar()
                return pd.to_datetime(result) if result else None
                
        except OperationalError as e:
            if "no such table" in str(e).lower():
                return None
            logger.error(f"Error getting latest date for {ticker}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(OperationalError)
    )
    def _save_to_database(self, df: pd.DataFrame, table_name: str, required_columns: List[str]) -> None:
        """
        Save a DataFrame to the database using bulk operations with retry support.

        Validates the DataFrame, cleans it, and performs a bulk insert into the specified table.
        The process is retried in case of operational errors.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            table_name (str): The target database table.
            required_columns (List[str]): List of required columns that must be present.
        """
        if not self._validate_dataframe(df, required_columns):
            raise ValueError(f"Invalid data for {table_name}")

        # Clean data for database insertion
        df_clean = df.copy()
        df_clean = df_clean.replace([pd.NA, pd.NaT], None)
        
        # Remove any completely empty rows
        df_clean = df_clean.dropna(how='all')
        
        if df_clean.empty:
            logger.warning(f"No valid data to save to {table_name}")
            return

        try:
            with self._db_connection() as conn:
                df_clean.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            logger.debug(f"Saved {len(df_clean)} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving data to {table_name}: {e}")
            raise

    @abstractmethod
    def refresh_data(self, ticker: str) -> None:
        """
        Refresh all market or instrument data for the specified ticker.

        Args:
            ticker (str): The identifier of the ticker to refresh.
        """
        pass

    @abstractmethod
    def fetch_and_save(self, ticker: str) -> None:
        """
        Fetch incremental data for the specified ticker and store it in the database.

        Args:
            ticker (str): The identifier of the ticker.
        """
        pass