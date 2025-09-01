# trading_system/src/strategies/base_strat.py

"""
Base strategy class for implementing trading strategies with database operations and signal storage.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import MetaData, Table, bindparam, select, text

from src.database.config import DatabaseConfig
from src.database.engine import create_db_engine


class DataRetrievalError(Exception):
    """Exception raised when data retrieval fails."""
    pass

class BaseStrategy(ABC):
    """
    Abstract base strategy class that provides interfaces for:
    - Caching with time-to-live (TTL)
    - Bulk data operations
    - Signal storage
    - Performance monitoring
    - Memory-efficient data retrieval
    
    Subclasses should implement the generate_signals method.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict[str, Any]] = None):
        """
        Initialize the strategy with the given database configuration and optional parameters.
        
        Args:
            db_config (DatabaseConfig): Configuration for connecting to the database.
            params (Optional[Dict[str, Any]]): Optional parameters for strategy customization.
        """
        self.db_config = db_config
        self.params = params or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_engine = create_db_engine(db_config)
        
        # Enhanced caching with TTL
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 300  # 5 minutes default TTL
        
        # Performance monitoring
        self._query_stats = {'count': 0, 'total_time': 0.0}

    def get_historical_prices(
        self,
        tickers: Union[str, List[str]],
        lookback: Optional[int] = None,
        data_source: str = 'yfinance',
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Retrieve historical price data with memory efficiency and flexible column selection.
        
        Args:
            tickers (Union[str, List[str]]): One or more stock ticker symbols.
            lookback (Optional[int]): Maximum number of records to retrieve (applies when no date bounds are provided).
            data_source (str): Identifier for the data source.
            from_date (Optional[str]): Start date in 'YYYY-MM-DD' format.
            to_date (Optional[str]): End date in 'YYYY-MM-DD' format.
            columns (Optional[List[str]]): List of column names to retrieve (defaults to standard price columns).
        
        Returns:
            pd.DataFrame: DataFrame containing historical price data.
        """
        # Default columns if not specified
        if columns is None:
            columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        
        # Create cache key
        cache_key = f"prices_{tickers}_{lookback}_{data_source}_{from_date}_{to_date}_{','.join(columns)}"
        
        # Check cache with TTL
        if self._is_cache_valid(cache_key):
            self.logger.debug("Using cached price data")
            return self._cache[cache_key]['data']

        t0 = perf_counter()
        
        try:
            if isinstance(tickers, str):
                df = self._fetch_single_ticker_prices(
                    tickers, lookback, data_source, from_date, to_date, columns
                )
            else:
                df = self._fetch_multiple_ticker_prices(
                    tickers, lookback, data_source, from_date, to_date, columns
                )
            
            # Cache the result
            self._cache_result(cache_key, df)
            
            t1 = perf_counter()
            self._update_query_stats(t1 - t0)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error retrieving price data: {e}")
            raise DataRetrievalError(f"Failed to retrieve price data: {e}")

    def _fetch_single_ticker_prices(
        self, 
        ticker: str, 
        lookback: Optional[int], 
        data_source: str,
        from_date: Optional[str], 
        to_date: Optional[str],
        columns: List[str]
    ) -> pd.DataFrame:
        """
        Retrieve price data for a single ticker using a parameterized query.
        
        Args:
            ticker (str): Stock ticker symbol.
            lookback (Optional[int]): Number of records to retrieve.
            data_source (str): Data source identifier.
            from_date (Optional[str]): Start date filter.
            to_date (Optional[str]): End date filter.
            columns (List[str]): List of columns to include in the result.
        
        Returns:
            pd.DataFrame: DataFrame containing price data for the specified ticker.
        """
        # Build column selection
        column_str = ', '.join(columns)
        
        params = {'ticker': ticker, 'data_source': data_source}
        
        base_query = f"""
            SELECT {column_str}
            FROM daily_prices
            WHERE ticker = :ticker AND data_source = :data_source
        """
        
        # Add date filters
        if from_date:
            base_query += " AND date >= :from_date"
            params['from_date'] = from_date
        if to_date:
            base_query += " AND date <= :to_date"
            params['to_date'] = to_date
        
        # Handle lookback and ordering
        if not from_date and not to_date and lookback:
            base_query += " ORDER BY date DESC LIMIT :lookback"
            params['lookback'] = lookback
        else:
            base_query += " ORDER BY date ASC"
        
        df = self._execute_query(text(base_query), params)
        
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            
            # Re-sort if we used lookback
            if not from_date and not to_date and lookback:
                df = df.sort_index()
        
        return df

    def _fetch_multiple_ticker_prices(
        self,
        tickers: List[str],
        lookback: Optional[int],
        data_source: str,
        from_date: Optional[str],
        to_date: Optional[str],
        columns: List[str]
    ) -> pd.DataFrame:
        """
        Retrieve price data for multiple tickers in batches.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            lookback (Optional[int]): Number of records to retrieve per ticker.
            data_source (str): Data source identifier.
            from_date (Optional[str]): Start date filter.
            to_date (Optional[str]): End date filter.
            columns (List[str]): List of columns to include in the result.
        
        Returns:
            pd.DataFrame: DataFrame containing price data for the specified tickers.
        """
        # Add ticker to columns if not present
        if 'ticker' not in columns:
            columns = columns + ['ticker']
        
        column_str = ', '.join(columns)
        
        # Process in batches to avoid SQL parameter limits
        batch_size = 100
        all_dfs = []
        
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]
            
            params = {'tickers': batch_tickers, 'data_source': data_source}
            
            base_query = f"""
                SELECT {column_str}
                FROM daily_prices
                WHERE ticker IN :tickers AND data_source = :data_source
            """
            
            if from_date:
                base_query += " AND date >= :from_date"
                params['from_date'] = from_date
            if to_date:
                base_query += " AND date <= :to_date"
                params['to_date'] = to_date
            
            base_query += " ORDER BY ticker, date ASC"
            
            query = text(base_query).bindparams(bindparam("tickers", expanding=True))
            batch_df = self._execute_query(query, params)
            
            if not batch_df.empty:
                all_dfs.append(batch_df)
        
        if not all_dfs:
            return pd.DataFrame()
        
        df = pd.concat(all_dfs, ignore_index=True)
        
        # Apply lookback per ticker if needed
        if not from_date and not to_date and lookback:
            df = df.groupby('ticker', group_keys=False).apply(
                lambda group: group.tail(lookback)
            )
        
        # Set proper index
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            if 'ticker' in df.columns:
                df = df.set_index(['ticker', 'date']).sort_index()
        
        return df

    def get_signals_history(
        self,
        strategy_name: str,
        tickers: Union[str, List[str]] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        min_confidence: float = 0.0
    ) -> pd.DataFrame:
        """
        Retrieve historical signals for analysis.
        
        Args:
            strategy_name (str): Name of the strategy.
            tickers (Union[str, List[str]], optional): Ticker symbol(s) to filter results.
            from_date (Optional[str]): Start date filter.
            to_date (Optional[str]): End date filter.
            min_confidence (float): Minimum signal confidence threshold.
        
        Returns:
            pd.DataFrame: DataFrame containing historical signals.
        """
        cache_key = f"signals_{strategy_name}_{tickers}_{from_date}_{to_date}_{min_confidence}"
        
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['data']
        
        params = {
            'strategy_name': strategy_name,
            'min_confidence': min_confidence
        }
        
        query = """
            SELECT ticker, date, signal, signal_strength, confidence, metadata
            FROM signals
            WHERE strategy_name = :strategy_name
            AND confidence >= :min_confidence
        """
        
        if tickers:
            if isinstance(tickers, str):
                query += " AND ticker = :ticker"
                params['ticker'] = tickers
            else:
                query += " AND ticker IN :tickers"
                params['tickers'] = tickers
        
        if from_date:
            query += " AND date >= :from_date"
            params['from_date'] = from_date
        
        if to_date:
            query += " AND date <= :to_date"
            params['to_date'] = to_date
        
        query += " ORDER BY ticker, date"
        
        if isinstance(tickers, list):
            query_obj = text(query).bindparams(bindparam("tickers", expanding=True))
        else:
            query_obj = text(query)
        
        df = self._execute_query(query_obj, params)
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index(['ticker', 'date']) if isinstance(tickers, list) or tickers is None else df.set_index('date')
        
        self._cache_result(cache_key, df)
        return df

    def save_signals(
        self,
        signals_df: pd.DataFrame,
        strategy_name: str,
        confidence: float = 1.0,
        metadata: Optional[Dict] = None
    ) -> None:
        """
        Persist generated signals to the database.
        
        Args:
            signals_df (pd.DataFrame): DataFrame with signal data (must include ticker, date, signal, and signal_strength).
            strategy_name (str): Name of the strategy generating the signals.
            confidence (float): Overall confidence score for the signals.
            metadata (Optional[Dict]): Additional metadata to store with the signals.
        """
        if signals_df.empty:
            return
        
        # Prepare signals for database insertion
        signals_to_save = signals_df.copy()
        
        # Ensure required columns
        required_cols = ['ticker', 'date', 'signal', 'signal_strength']
        missing_cols = [col for col in required_cols if col not in signals_to_save.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Add metadata
        signals_to_save['strategy_name'] = strategy_name
        signals_to_save['confidence'] = confidence
        signals_to_save['metadata'] = str(metadata) if metadata else None
        signals_to_save['created_at'] = datetime.now()
        
        # Save to database
        try:
            with self.db_engine.connect() as conn:
                # Use INSERT OR REPLACE for SQLite to handle duplicates
                signals_to_save.to_sql(
                    'signals',
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            
            self.logger.info(f"Saved {len(signals_to_save)} signals for strategy {strategy_name}")
            
        except Exception as e:
            self.logger.error(f"Error saving signals: {e}")
            raise

    def _is_cache_valid(self, cache_key: str) -> bool:
        """
        Determine if cached data for the given key is still valid based on its time-to-live (TTL).
        
        Args:
            cache_key (str): The cache key associated with the data.
        
        Returns:
            bool: True if the cache entry is valid; otherwise, False.
        """
        if cache_key not in self._cache:
            return False
        
        cache_entry = self._cache[cache_key]
        age = datetime.now() - cache_entry['timestamp']
        return age.total_seconds() < self._cache_ttl

    def _cache_result(self, cache_key: str, data: pd.DataFrame) -> None:
        """
        Cache the query result along with the current timestamp.
        
        Args:
            cache_key (str): Key for the cache entry.
            data (pd.DataFrame): DataFrame to be cached.
        """
        self._cache[cache_key] = {
            'data': data.copy(),
            'timestamp': datetime.now()
        }
        
        # Limit cache size
        if len(self._cache) > 100:
            # Remove oldest entries
            oldest_keys = sorted(
                self._cache.keys(),
                key=lambda k: self._cache[k]['timestamp']
            )[:20]
            for key in oldest_keys:
                del self._cache[key]

    def _update_query_stats(self, execution_time: float) -> None:
        """
        Update internal query performance statistics.
        
        Args:
            execution_time (float): Time taken to execute a query in seconds.
        """
        self._query_stats['count'] += 1
        self._query_stats['total_time'] += execution_time
        
        if self._query_stats['count'] % 100 == 0:  # Log every 100 queries
            avg_time = self._query_stats['total_time'] / self._query_stats['count']
            self.logger.info(f"Query performance: {self._query_stats['count']} queries, "
                           f"avg time: {avg_time:.3f}s")

    def _execute_query(self, query: Any, params: dict, index_col: Optional[str] = None) -> pd.DataFrame:
        """
        Execute a database query with the specified parameters and return the result as a DataFrame.
        
        Args:
            query (Any): SQLAlchemy query object.
            params (dict): Parameters for the query.
            index_col (Optional[str]): Column to set as the index in the resulting DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame containing the query results.
        
        Raises:
            DataRetrievalError: If the query execution fails.
        """
        t0 = perf_counter()
        
        try:
            with self.db_engine.connect() as connection:
                result = connection.execute(query, params)
                data = result.fetchall()
                
                if data:
                    df = pd.DataFrame(data, columns=result.keys())
                else:
                    # Return empty DataFrame with proper columns
                    df = pd.DataFrame(columns=result.keys())
                
                if index_col and index_col in df.columns:
                    df[index_col] = pd.to_datetime(df[index_col])
                    df = df.set_index(index_col).sort_index()
                
        except Exception as e:
            self.logger.error(f"Database query failed: {str(e)}")
            raise DataRetrievalError(f"Query execution failed: {e}")
        
        t1 = perf_counter()
        execution_time = t1 - t0
        
        if execution_time > 2.0:  # Log slow queries
            self.logger.warning(f"Slow query detected ({execution_time:.2f}s): {str(query)[:100]}...")
        
        self.logger.debug(f"Query executed in {execution_time:.4f}s, returned {len(df)} rows")
        return df

    def get_company_info(
        self,
        tickers: Union[str, List[str]],
        data_source: str = 'yfinance',
        columns: Optional[List[str]] = None
    ) -> Union[pd.Series, pd.DataFrame]:
        """
        Retrieve company information with optional column selection.
        
        Args:
            tickers (Union[str, List[str]]): Single or multiple ticker symbols.
            data_source (str): Identifier for the data source.
            columns (Optional[List[str]]): Specific columns to retrieve; retrieves all if not specified.
        
        Returns:
            Union[pd.Series, pd.DataFrame]: Company info as a Series (single ticker) or DataFrame (multiple tickers).
        """
        cache_key = f"company_info_{tickers}_{data_source}_{columns}"
        
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['data']
        
        # Build column selection
        if columns:
            column_str = ', '.join(columns + ['ticker'])
        else:
            column_str = '*'
        
        if isinstance(tickers, str):
            query = text(f"""
                SELECT {column_str}
                FROM company_info
                WHERE ticker = :ticker AND data_source = :data_source
                ORDER BY updated_at DESC LIMIT 1
            """)
            params = {'ticker': tickers, 'data_source': data_source}
            
        else:
            query = text(f"""
                SELECT {column_str}
                FROM company_info
                WHERE ticker IN :tickers AND data_source = :data_source
                ORDER BY ticker, updated_at DESC
            """).bindparams(bindparam("tickers", expanding=True))
            params = {'tickers': tickers, 'data_source': data_source}
        
        df = self._execute_query(query, params)
        
        if df.empty:
            raise DataRetrievalError(f"No company info found for {tickers}")
        
        if isinstance(tickers, str):
            result = df.iloc[0]
        else:
            # Get most recent record per ticker
            result = df.sort_values(['ticker', 'updated_at'], ascending=[True, False])
            result = result.groupby('ticker').first()
        
        self._cache_result(cache_key, result)
        return result

    def get_financials(
        self,
        tickers: Union[str, List[str]],
        statement_type: str,
        lookback: Optional[int] = None,
        data_source: str = 'yfinance',
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Retrieve financial statement data for the given ticker(s).
        
        Args:
            tickers (Union[str, List[str]]): Stock ticker symbol(s).
            statement_type (str): Type of financial statement ('balance_sheet', 'income_statement', or 'cash_flow').
            lookback (Optional[int]): Maximum number of records to retrieve (applies when date filters are not provided).
            data_source (str): Identifier for the data source.
            columns (Optional[List[str]]): List of columns to retrieve (defaults to all columns if not specified).
        
        Returns:
            pd.DataFrame: DataFrame containing financial statement data.
        
        Raises:
            ValueError: If an invalid statement type is provided.
        """
        cache_key = f"financials_{tickers}_{statement_type}_{lookback}_{data_source}_{columns}"
        
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['data']
        
        # Validate statement type
        valid_statements = ['balance_sheet', 'income_statement', 'cash_flow']
        if statement_type not in valid_statements:
            raise ValueError(f"Invalid statement type: {statement_type}")
        
        # Build query
        if columns:
            column_str = ', '.join(columns + ['ticker', 'date'])
        else:
            column_str = '*'
        
        if isinstance(tickers, str):
            if lookback:
                query = text(f"""
                    SELECT {column_str}
                    FROM {statement_type}
                    WHERE ticker = :ticker AND data_source = :data_source
                    ORDER BY date DESC LIMIT :lookback
                """)
                params = {'ticker': tickers, 'data_source': data_source, 'lookback': lookback}
            else:
                query = text(f"""
                    SELECT {column_str}
                    FROM {statement_type}
                    WHERE ticker = :ticker AND data_source = :data_source
                    ORDER BY date ASC
                """)
                params = {'ticker': tickers, 'data_source': data_source}
        else:
            # Multiple tickers
            query = text(f"""
                SELECT {column_str}
                FROM {statement_type}
                WHERE ticker IN :tickers AND data_source = :data_source
                ORDER BY ticker, date ASC
            """).bindparams(bindparam("tickers", expanding=True))
            params = {'tickers': tickers, 'data_source': data_source}
        
        df = self._execute_query(query, params)
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            
            if isinstance(tickers, list):
                df = df.set_index(['ticker', 'date'])
                if lookback:
                    df = df.groupby(level=0).apply(
                        lambda x: x.droplevel(0).tail(lookback)
                    )
            else:
                df = df.set_index('date')
                if lookback:
                    df = df.tail(lookback)
            
            df = df.sort_index()
        
        self._cache_result(cache_key, df)
        return df

    def _validate_data(self, df: pd.DataFrame, min_records: int = 1) -> bool:
        """
        Validate that the DataFrame has sufficient records for analysis.

        Args:
            df (pd.DataFrame): DataFrame to check.
            min_records (int): Minimum required records.

        Returns:
            bool: True if data has enough records, False otherwise.
        """
        if df.empty or len(df) < min_records:
            self.logger.warning("Insufficient data for analysis")
            return False
        return True

    @abstractmethod
    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate trading signals based on historical data.
        
        Subclasses must implement this method.
        
        Args:
            ticker (Union[str, List[str]]): Stock ticker symbol(s) for which to generate signals.
            start_date (Optional[str]): Start date for signal generation.
            end_date (Optional[str]): End date for signal generation.
            initial_position (int): Initial position size.
            latest_only (bool): Whether to return only the most recent signals.
        
        Returns:
            pd.DataFrame: DataFrame containing generated signals.
        """
        pass

    def __del__(self):
        """
        Release resources and log query performance metrics upon object deletion.
        
        Closes the database engine connection and logs aggregate query statistics.
        """
        if hasattr(self, 'db_engine'):
            try:
                if hasattr(self, '_query_stats') and self._query_stats['count'] > 0:
                    avg_time = self._query_stats['total_time'] / self._query_stats['count']
                    self.logger.info(f"Strategy session stats: {self._query_stats['count']} queries, "
                                   f"avg time: {avg_time:.3f}s")
                
                self.db_engine.dispose()
                self.logger.debug("Database engine disposed successfully")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")