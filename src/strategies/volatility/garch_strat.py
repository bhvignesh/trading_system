# trading_system/src/strategies/volatality/garch_strat.py

import logging
import multiprocessing
import warnings
from time import perf_counter  # Added for static query execution
from typing import Any, Dict, List, Optional, Union  # Added Any

import numpy as np
import pandas as pd
from arch import arch_model
from joblib import Parallel, delayed
from sqlalchemy import bindparam, text  # Added for static query execution

from src.database.config import DatabaseConfig
from src.database.engine import \
    create_db_engine  # For creating engine in worker
from src.strategies.base_strat import \
    BaseStrategy  # Keep for GARCHModel inheritance
from src.strategies.risk_management import RiskManager

# --- Helper functions for parallel processing ---

def _setup_worker_logger(name_prefix: str, ticker_symbol: str) -> logging.Logger:
    # (Same as before)
    logger_name = f"{name_prefix}_{ticker_symbol}_{multiprocessing.current_process().pid}"
    logger = logging.getLogger(logger_name)
    if not logger.hasHandlers(): 
        handler = logging.StreamHandler() 
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG) # Changed to DEBUG for more verbose worker logs initially
    logger.propagate = False 
    return logger

# --- Static Data Fetching Utilities (adapted from BaseStrategy) ---

def _execute_query_static(
    db_engine: Any, # Expects an SQLAlchemy engine
    logger: logging.Logger,
    query: Any, 
    params: dict, 
    index_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Static version of _execute_query.
    Executes a SQL query using a provided engine and logger.
    """
    t0 = perf_counter()
    try:
        with db_engine.connect() as connection:
            result = connection.execute(query, params)
            data = result.fetchall()
            df = pd.DataFrame(data, columns=result.keys())
            if index_col and index_col in df.columns:
                df[index_col] = pd.to_datetime(df[index_col])
                df = df.set_index(index_col).sort_index()
    except Exception as e:
        logger.error(f"Static DB query error: {str(e)}", exc_info=True)
        raise # Re-raise to be caught by the worker task
    t1 = perf_counter()
    logger.debug(f"Static query executed in {t1 - t0:.4f} seconds. Query: {str(query).strip()[:100]}...")
    return df

def get_historical_prices_static(
    db_engine: Any, # Expects an SQLAlchemy engine
    logger: logging.Logger,
    tickers: Union[str, List[str]],
    lookback: Optional[int] = None,
    data_source: str = 'yfinance',
    from_date: Optional[str] = None,
    to_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Static version of get_historical_prices.
    Retrieves historical price data using a provided engine and logger.
    Note: This static version does not use instance-level caching like BaseStrategy.
    """
    # Cache key for this function would be local if implemented, or not used.
    # For simplicity, we'll omit complex caching here; joblib might already do some.
    
    t0 = perf_counter()
    df = pd.DataFrame() # Initialize empty DataFrame

    if isinstance(tickers, str):
        # (Logic from BaseStrategy.get_historical_prices for single ticker)
        params_dict = { # Renamed params to params_dict to avoid conflict
            'ticker': tickers,
            'data_source': data_source
        }
        base_query_str = """
            SELECT date, open, high, low, close, volume 
            FROM daily_prices
            WHERE ticker = :ticker 
            AND data_source = :data_source
        """
        if from_date:
            base_query_str += " AND date >= :from_date"
            params_dict['from_date'] = from_date
        if to_date:
            base_query_str += " AND date <= :to_date"
            params_dict['to_date'] = to_date

        if not from_date and not to_date:
            if lookback is not None:
                base_query_str += " ORDER BY date DESC LIMIT :lookback"
                params_dict['lookback'] = lookback
            else:
                base_query_str += " ORDER BY date ASC"
        else:
            base_query_str += " ORDER BY date ASC"
        
        query = text(base_query_str)
        df = _execute_query_static(db_engine, logger, query, params_dict, index_col='date')
        if not from_date and not to_date and lookback is not None and not df.empty:
            df = df.sort_index() # Ensure ascending order after DESC LIMIT
        # df.index should already be datetime from _execute_query_static if index_col='date'

    else: # List of tickers
        # (Logic from BaseStrategy.get_historical_prices for multiple tickers)
        params_dict = {
            'tickers': tickers,
            'data_source': data_source
        }
        base_query_str = """
            SELECT date, open, high, low, close, volume, ticker
            FROM daily_prices
            WHERE ticker = ANY(:tickers) -- Using ANY for array binding if supported, or IN
            AND data_source = :data_source
        """ # Note: SQL IN syntax is `IN :tickers` with `expanding=True` for sqlalchemy
          # Or `ticker = ANY(:tickers)` if passing a list/tuple directly if DB supports array.
          # Given the original code used .bindparams(bindparam("tickers", expanding=True)),
          # we'll stick to that logic with `IN`.

        base_query_str_multi = """
            SELECT date, open, high, low, close, volume, ticker
            FROM daily_prices
            WHERE ticker IN :tickers 
            AND data_source = :data_source
        """

        if from_date:
            base_query_str_multi += " AND date >= :from_date"
            params_dict['from_date'] = from_date
        if to_date:
            base_query_str_multi += " AND date <= :to_date"
            params_dict['to_date'] = to_date
        
        base_query_str_multi += " ORDER BY ticker, date ASC"
        query = text(base_query_str_multi).bindparams(bindparam("tickers", expanding=True))

        if not from_date and not to_date and lookback is not None:
            df_full = _execute_query_static(db_engine, logger, query, params_dict)
            if not df_full.empty:
                df = df_full.groupby('ticker', group_keys=False).apply(lambda group: group.iloc[-lookback:])
            else:
                df = df_full # empty
        else:
            df = _execute_query_static(db_engine, logger, query, params_dict)

        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index(['ticker', 'date']).sort_index()

    t1 = perf_counter()
    logger.debug(f"get_historical_prices_static for {tickers} executed in {t1 - t0:.4f} seconds. Found {len(df)} rows.")
    return df


# --- Worker Task and Signal Generation (modified) ---

def _validate_data_static(price_data: pd.DataFrame, min_records: int, ticker_symbol: str, logger: logging.Logger) -> bool:
    # (Same as before)
    if price_data is None or price_data.empty:
        logger.debug(f"[{ticker_symbol}] Price data is None or empty.")
        return False
    if len(price_data) < min_records:
        logger.debug(f"[{ticker_symbol}] Insufficient records: {len(price_data)} < {min_records}")
        return False
    
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in price_data.columns for col in required_cols):
        logger.debug(f"[{ticker_symbol}] Missing one or more required columns: {required_cols}")
        return False
    return True


def _generate_signal_for_ticker_static(
    ticker_symbol: str, 
    price_data: pd.DataFrame, 
    initial_position: int,
    params: dict,
    logger: logging.Logger
) -> Optional[pd.DataFrame]:
    # (Largely same as before, ensure it uses passed logger)
    if 'close' not in price_data.columns or not pd.api.types.is_numeric_dtype(price_data['close']):
        logger.error(f"[{ticker_symbol}] 'close' column is missing or not numeric.")
        return None 

    close_prices = price_data['close']
    shifted_close = close_prices.shift(1)
    returns = pd.Series(np.nan, index=price_data.index, dtype=float) 

    if params['return_type'] == 'log':
        valid_mask = (close_prices > 0) & (shifted_close > 0) & shifted_close.notna()
        if valid_mask.any():
            returns.loc[valid_mask] = np.log(close_prices.loc[valid_mask] / shifted_close.loc[valid_mask]) * 100
    else: 
        valid_mask = (shifted_close.notna()) & (shifted_close != 0)
        if valid_mask.any():
            returns.loc[valid_mask] = (close_prices.loc[valid_mask] / shifted_close.loc[valid_mask] - 1) * 100
    
    result_df = price_data.copy()
    result_df['returns'] = returns
    result_df['forecast_volatility'] = np.nan
    result_df['volatility_change'] = np.nan
    result_df['signal'] = 0 
    
    if returns.dropna().empty:
        logger.warning(f"[{ticker_symbol}] No valid returns computed. Cannot proceed with GARCH modeling.")
        return price_data.iloc[0:0].assign(
            returns=pd.Series(dtype=float), 
            forecast_volatility=pd.Series(dtype=float),
            volatility_change=pd.Series(dtype=float),
            signal=pd.Series(dtype=int)
        )

    window_size = int(params['window_size'])
    p_garch, q_garch = int(params['p']), int(params['q'])
    forecast_horizon = int(params['forecast_horizon'])
    vol_threshold = params['vol_threshold']
    
    col_idx_forecast_vol = result_df.columns.get_loc('forecast_volatility')
    col_idx_vol_change = result_df.columns.get_loc('volatility_change')
    col_idx_signal = result_df.columns.get_loc('signal')
    
    fit_failures = 0
    if len(returns) < window_size:
        logger.warning(f"[{ticker_symbol}] Returns series length ({len(returns)}) is less than window size ({window_size}). No GARCH models will be fit.")
        result_df = result_df.dropna(subset=['forecast_volatility']) 
        return result_df if not result_df.empty else pd.DataFrame(columns=result_df.columns)

    for i in range(window_size, len(returns)):
        current_window_returns = returns.iloc[i - window_size : i]
        # Make sure current_window_returns is a Series, if it's already .values, np.std will work
        window_returns_values = current_window_returns.dropna().values 

        if len(window_returns_values) < window_size * 0.9: 
            continue
        
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore") 
                garch = arch_model(window_returns_values, vol='Garch', p=p_garch, q=q_garch, rescale=False)
                garch_fit_result = garch.fit(disp='off', show_warning=False)

            forecast = garch_fit_result.forecast(horizon=forecast_horizon, reindex=False) 
            forecast_variance_h_step = forecast.variance.values[-1, 0] 
            forecast_vol_h_step = np.sqrt(forecast_variance_h_step)
        
        except Exception as e: 
            fit_failures += 1
            if fit_failures <= 5: 
                logger.warning(f"[{ticker_symbol}] GARCH processing failed at {result_df.index[i]} (iter {i}): {str(e)}")
            elif fit_failures == 6:
                 logger.warning(f"[{ticker_symbol}] Further GARCH processing failures for this ticker will not be logged individually.")
            continue 
        
        result_df.iloc[i, col_idx_forecast_vol] = forecast_vol_h_step
        
        hist_vol_val = np.nan # Renamed to avoid conflict with hist_vol parameter if it exists
        if len(window_returns_values) > 1:
            # Ensure window_returns_values is 1D array for pd.Series().std()
            hist_vol_val = pd.Series(window_returns_values.flatten()).std() * np.sqrt(252)


        if pd.notna(hist_vol_val) and hist_vol_val != 0:
            vol_change = (forecast_vol_h_step - hist_vol_val) / hist_vol_val
        else:
            vol_change = np.nan 
        result_df.iloc[i, col_idx_vol_change] = vol_change
        
        if pd.notna(vol_change): 
            if vol_change > vol_threshold:
                result_df.iloc[i, col_idx_signal] = 1
            elif vol_change < -vol_threshold:
                result_df.iloc[i, col_idx_signal] = -1
    
    if fit_failures > 0:
        total_attempts = max(0, len(returns) - window_size)
        if total_attempts > 0:
            logger.info(f"[{ticker_symbol}] GARCH model fitting/forecasting failed {fit_failures} times out of {total_attempts} attempts.")
        else:
            logger.info(f"[{ticker_symbol}] GARCH model fitting/forecasting failed {fit_failures} times (no valid attempts were made).")


    min_periods_std = max(1, int(window_size * 0.25)) 
    rolling_std_vol_change = result_df['volatility_change'].rolling(window=window_size, min_periods=min_periods_std).std()
    result_df['signal_strength'] = (result_df['volatility_change'].abs() / (rolling_std_vol_change + 1e-9))
    
    result_df = result_df.dropna(subset=['forecast_volatility'])

    if result_df.empty:
        logger.warning(f"[{ticker_symbol}] No valid GARCH forecasts after processing, DataFrame is empty.")
        # To ensure a DataFrame with correct columns is returned even if empty:
        # Reconstruct empty DF with expected columns if result_df is empty.
        # However, `RiskManager` might expect certain columns.
        # It's safer to return pd.DataFrame() and let downstream handle it if empty after this stage.
        return pd.DataFrame() 

    if params['long_only']:
        result_df['signal'] = result_df['signal'].clip(lower=0)
    
    risk_manager = RiskManager(
        stop_loss_pct=params.get('stop_loss_pct', 0.05),
        take_profit_pct=params.get('take_profit_pct', 0.10),
        trailing_stop_pct=params.get('trailing_stop_pct', 0.00),
        slippage_pct=params.get('slippage_pct', 0.001),
        transaction_cost_pct=params.get('transaction_cost_pct', 0.001)
    )
    result_with_rm = risk_manager.apply(result_df.copy(), initial_position=initial_position)
    
    result_with_rm = result_with_rm.rename(columns={
        'return': 'rm_strategy_return',
        'cumulative_return': 'rm_cumulative_return',
        'exit_type': 'rm_action'
    })
    
    return result_with_rm

def _garch_worker_task(
    ticker_symbol: str, 
    start_date_str: Optional[str], 
    end_date_str: Optional[str], 
    initial_position: int, 
    latest_only: bool,
    db_config_dict: dict, 
    strategy_params: dict
    ) -> Optional[pd.DataFrame]:
    """
    Worker function for processing a single ticker in parallel.
    Initializes DB engine locally and uses static methods for data fetching.
    """
    logger = _setup_worker_logger("garch_worker", ticker_symbol)
    db_engine = None # Initialize to None

    try:
        # Reconstruct DatabaseConfig and create a new engine for this worker
        db_config_instance = DatabaseConfig(**db_config_dict)
        db_engine = create_db_engine(db_config_instance)
        logger.info(f"[{ticker_symbol}] Worker DB engine created.")
        
    except Exception as e:
        logger.error(f"[{ticker_symbol}] Error initializing DB engine in worker: {e}", exc_info=True)
        if db_engine: db_engine.dispose()
        return None

    window_size = int(strategy_params['window_size'])
    price_data = pd.DataFrame() # Initialize
    try:
        if start_date_str:
            start_date_dt = pd.to_datetime(start_date_str)
            buffer_days = window_size * 3 
            fetch_from_date = (start_date_dt - pd.Timedelta(days=buffer_days)).strftime('%Y-%m-%d')
            price_data = get_historical_prices_static(
                db_engine, logger, ticker_symbol, 
                from_date=fetch_from_date, to_date=end_date_str,
                data_source=strategy_params.get('data_source', 'yfinance') # Assuming data_source can be in params
            )
        else:
            default_lookback = int(strategy_params.get('default_lookback_days', max(500, window_size + 250)))
            price_data = get_historical_prices_static(
                db_engine, logger, ticker_symbol, 
                lookback=default_lookback,
                data_source=strategy_params.get('data_source', 'yfinance')
            )
    except Exception as e:
        logger.error(f"[{ticker_symbol}] Error fetching historical prices using static method: {e}", exc_info=True)
        if db_engine: db_engine.dispose()
        return None

    min_required_records = window_size + int(strategy_params.get('min_additional_records', 50))
    if not _validate_data_static(price_data, min_records=min_required_records, ticker_symbol=ticker_symbol, logger=logger):
        logger.warning(f"[{ticker_symbol}] Data validation failed. Need {min_required_records}, got {len(price_data) if price_data is not None else 'None/Empty'}.")
        if db_engine: db_engine.dispose()
        return None
    
    # NOTE: get_historical_prices_static for a single ticker already returns data for that ticker only,
    # so no need to .xs() if it's correctly implemented.
    # If it could return MultiIndex, then this is needed:
    price_data_tic = price_data
    if isinstance(price_data.index, pd.MultiIndex): # Defensive check
        if ticker_symbol in price_data.index.get_level_values('ticker'):
            price_data_tic = price_data.xs(ticker_symbol, level='ticker')
        else: # This case should ideally not happen if get_historical_prices_static is correct for single ticker
            logger.warning(f"[{ticker_symbol}] Ticker not found in MultiIndex data from static fetcher (unexpected).")
            if db_engine: db_engine.dispose()
            return None
    
    signal_df = None # Initialize
    try:
        signal_df = _generate_signal_for_ticker_static(
            ticker_symbol, price_data_tic, initial_position, strategy_params, logger
        )
    except Exception as e: 
        logger.error(f"[{ticker_symbol}] Unhandled error in _generate_signal_for_ticker_static: {e}", exc_info=True)
        if db_engine: db_engine.dispose()
        return None 
    
    # Dispose DB engine after use in this worker
    if db_engine:
        db_engine.dispose()
        logger.info(f"[{ticker_symbol}] Worker DB engine disposed.")

    if signal_df is None or signal_df.empty:
        logger.info(f"[{ticker_symbol}] No signals generated or an error occurred in _generate_signal_for_ticker_static.")
        return None

    if start_date_str:
        signal_df = signal_df.loc[signal_df.index >= pd.to_datetime(start_date_str)]
    
    if signal_df.empty: 
         logger.info(f"[{ticker_symbol}] No signals remaining after filtering for start_date {start_date_str}.")
         return None

    if latest_only:
        if not signal_df.empty:
            signal_df = signal_df.iloc[[-1]]
        if signal_df.empty: 
            return None 
    
    if not signal_df.empty:
        signal_df['ticker'] = ticker_symbol 
        return signal_df
    else:
        return None


# --- GARCHModel Class Definition ---
class GARCHModel(BaseStrategy): # GARCHModel still inherits from BaseStrategy
    """
    GARCH Model Strategy with Integrated Risk Management.
    (Full docstring from original code)
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None, n_jobs: Optional[int] = None):
        """
        Initialize the GARCH Model strategy.
        (Full docstring from original code)
        """
        default_params = {
            'window_size': 100, 'forecast_horizon': 1, 'vol_threshold': 0.1,
            'p': 1, 'q': 1, 'return_type': 'log', 'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10, 'slippage_pct': 0.001, 'trailing_stop_pct': 0.00,
            'transaction_cost_pct': 0.001, 'long_only': True,
            'default_lookback_days': 500, 'min_additional_records': 50,
            'data_source': 'yfinance' # Added default data_source
        }
        if params:
            default_params.update(params)
        
        # Convert db_config to serializable dict for pickling
        if hasattr(db_config, 'to_dict') and callable(db_config.to_dict):
            self.db_config_params = db_config.to_dict()
        elif isinstance(db_config, dict):
             self.db_config_params = db_config.copy()
        elif hasattr(db_config, '__dict__'): 
             self.db_config_params = db_config.__dict__.copy()
        else:
            raise TypeError(
                "db_config could not be converted to a dictionary for pickling. "
                "Ensure it has a .to_dict() method, is a dataclass, or is a simple object "
                "whose __dict__ attribute contains its serializable state."
            )

        # BaseStrategy __init__ is called for the main GARCHModel instance.
        # This sets up self.db_engine, self.logger, self.params for the *main* process.
        super().__init__(db_config, default_params) 
        
        if n_jobs is None or n_jobs == -1: 
            self.n_jobs = multiprocessing.cpu_count()
        elif n_jobs == 0: 
            self.logger.warning("n_jobs=0 interpreted as 1 (sequential processing).")
            self.n_jobs = 1
        elif n_jobs > 0 :
            self.n_jobs = n_jobs
        else: 
             self.logger.warning(f"Invalid n_jobs value {n_jobs}. Defaulting to 1 (sequential processing).")
             self.n_jobs = 1
        self.logger.info(f"GARCHModel initialized to use {self.n_jobs} parallel job(s).")
    
    # GARCHModel needs to implement generate_signals because BaseStrategy declares it abstract.
    # This implementation will now orchestrate the parallel calls.
    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals based on GARCH volatility forecasts.
        This method orchestrates parallel processing of tickers.
        """
        tickers_list = [ticker] if isinstance(ticker, str) else ticker
        
        tasks = [
            delayed(_garch_worker_task)(
                tic, 
                start_date, 
                end_date, 
                initial_position, 
                latest_only,
                self.db_config_params,    # Serialized DB config for worker
                self.params.copy()        # Strategy params for worker
            ) for tic in tickers_list
        ]
        
        self.logger.info(f"Starting GARCH signal generation for {len(tickers_list)} ticker(s) using {self.n_jobs} parallel job(s).")
        
        processed_results = []
        try:
            # Set a timeout for tasks if they might hang indefinitely
            # For example, timeout=300 seconds per task.
            # backend="loky" is default if prefer="processes"
            processed_results = Parallel(n_jobs=self.n_jobs, prefer="processes", timeout=600)(tasks)
        except Exception as e: 
            self.logger.error(f"Parallel GARCH signal generation failed: {e}", exc_info=True)
            # Depending on joblib version and specific error (e.g. TimeoutError),
            # processed_results might be partially filled or empty.
            # It's safer to re-raise or return empty if the whole batch fails.
            # For now, we'll try to process any results obtained before the error.
            if not processed_results: # if Parallel itself failed before returning anything
                 return pd.DataFrame()

        results_dfs = [res_df for res_df in processed_results if res_df is not None and not res_df.empty]
        
        if results_dfs:
            final_result = pd.concat(results_dfs)
            final_result = final_result.reset_index().set_index(['ticker', 'date']).sort_index() 
            self.logger.info(f"Successfully generated GARCH signals for {len(results_dfs)}/{len(tickers_list)} requested ticker(s).")
            return final_result
        else:
            self.logger.warning("No GARCH signals generated for any ticker after parallel processing.")
            return pd.DataFrame()