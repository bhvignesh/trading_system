"""
GARCH‑X Strategy Implementation with Two-Phase Optimization (Robust Production Version)

This module implements a GARCH‑X strategy with separate time series model training
and strategy parameter optimization phases.
"""

import os
import numpy as np
import pandas as pd
import logging
import pickle
import hashlib
import json
import warnings
import gc
from typing import Dict, Optional, Union, List, Tuple, Any
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import lru_cache, wraps
from datetime import datetime, timedelta

from arch import arch_model
from sklearn.decomposition import PCA
from joblib import Parallel, delayed, dump, load

# Try to import optional performance libraries
try:
    from numba import jit
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    warnings.warn("Numba not installed. Some operations will be slower.")
    def jit(nopython=True, **_):
        def decorator(func):
            return func
        return decorator

# Try to import bottleneck for faster operations
try:
    import bottleneck as bn
    HAS_BOTTLENECK = True
except ImportError:
    HAS_BOTTLENECK = False
    warnings.warn("Bottleneck not installed. Using pandas for rolling operations (slower).")

try:
    from numpy.lib.stride_tricks import sliding_window_view
    HAS_SLIDING = True
except ImportError:
    HAS_SLIDING = False

from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.database.config import DatabaseConfig
from src.strategies.risk_management import RiskManager

warnings.filterwarnings('ignore', category=RuntimeWarning, module='arch')


def timer_decorator(func):
    """Decorator to time function execution for performance monitoring."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        duration = (datetime.now() - start).total_seconds()
        if hasattr(args[0], 'logger'):
            args[0].logger.debug(f"{func.__name__} took {duration:.3f}s")
        return result
    return wrapper


def _fast_rolling(series: Union[pd.Series, np.ndarray], window: int, func: str, min_periods: Optional[int] = None) -> np.ndarray:
    """
    Compute rolling statistics efficiently. Uses bottleneck if available,
    otherwise falls back to pandas with optional numba engine support.
    """
    if isinstance(series, np.ndarray):
        series = pd.Series(series)

    min_periods = window if min_periods is None else min_periods
    roll = series.rolling(window=window, min_periods=min_periods)

    if HAS_NUMBA:
        try:
            return getattr(roll, func)(engine="numba", engine_kwargs={"parallel": True}).to_numpy()
        except Exception:
            pass

    return getattr(roll, func)().to_numpy()


def _build_lagged_arrays(data_dict: Dict[str, np.ndarray], max_lag: int) -> Dict[str, np.ndarray]:
    """
    Build lag matrices for multiple 1-D arrays in a vectorized manner.
    Returns a dict mapping keys to 2-D arrays (rows = observations, cols = lags).
    """
    result = {}
    for key, values in data_dict.items():
        values = np.asarray(values, dtype=np.float64)
        n = values.shape[0]
        lagged = np.full((n, max_lag), np.nan, dtype=np.float64)

        if HAS_SLIDING and n > max_lag:
            window = sliding_window_view(values, max_lag + 1)
            lagged[max_lag:, :] = window[:, :-1]
        else:
            for lag in range(1, max_lag + 1):
                lagged[lag:, lag - 1] = values[:-lag]

        result[key] = lagged

    return result


if HAS_NUMBA:
    @jit(nopython=True, cache=True)
    def _calculate_ema_numpy_jit(data: np.ndarray, alpha: float) -> np.ndarray:
        ema = np.empty_like(data)
        ema[0] = data[0]
        for i in range(1, len(data)):
            if np.isnan(data[i]):
                ema[i] = ema[i - 1]
            else:
                ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
        return ema


def _calculate_ema_numpy(data: np.ndarray, alpha: float) -> np.ndarray:
    """
    Calculate exponential moving average. Uses numba JIT compilation if available.
    """
    data = np.asarray(data, dtype=np.float64)
    if data.size == 0:
        return np.array([], dtype=np.float64)

    if HAS_NUMBA:
        return _calculate_ema_numpy_jit(data, alpha)

    ema = np.empty_like(data, dtype=np.float64)
    ema[0] = data[0] if not np.isnan(data[0]) else 0.0
    for i in range(1, len(data)):
        if np.isnan(data[i]):
            ema[i] = ema[i - 1]
        else:
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
    return ema


class GarchXStrategyStrategy(BaseStrategy):
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        default_params = {
            'mode': "train",
            'forecast_horizon': 30,
            'forecast_lookback': 200,
            'min_volume_strength': 1.0,
            'signal_strength_threshold': 0.1,
            'pca_components': 5,
            'winsorize_quantiles': (0.01, 0.99),
            'vol_window': 20,
            'cv_folds': 3,
            'parallel_cv': True,
            'enable_feature_cache': True,
            'enable_forecast_cache': True,
            'batch_size': 10,
            'n_jobs': -1,
            'use_joblib_backend': 'threading',
            'max_convergence_attempts': 3,
            'forecast_simulations': 1000,
            'capital': 1.0,
            'long_only': True,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trail_stop_pct': 0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
        }
        default_params.update(params or {})
        super().__init__(db_config, default_params)

        self.mode = self.params.get("mode", "train")
        if self.mode not in ["train", "forecast"]:
            raise ValueError(f"Invalid mode: {self.mode}. Must be 'train' or 'forecast'")

        self.forecast_horizon = int(self.params.get('forecast_horizon'))
        self.forecast_lookback = int(self.params.get('forecast_lookback'))
        self.cv_folds = int(self.params.get('cv_folds'))
        self.parallel_cv = bool(self.params.get('parallel_cv'))

        self.min_volume_strength = float(self.params.get('min_volume_strength'))
        self.signal_strength_threshold = float(self.params.get('signal_strength_threshold'))

        self.pca_components = int(self.params.get('pca_components'))
        self.winsorize_quantiles = self.params.get('winsorize_quantiles')
        self.vol_window = int(self.params.get('vol_window'))

        self.enable_feature_cache = bool(self.params.get('enable_feature_cache'))
        self.enable_forecast_cache = bool(self.params.get('enable_forecast_cache'))
        self.batch_size = int(self.params.get('batch_size'))
        self.n_jobs = int(self.params.get('n_jobs'))
        self.use_joblib_backend = self.params.get('use_joblib_backend')
        self.max_convergence_attempts = int(self.params.get('max_convergence_attempts'))
        self.forecast_simulations = int(self.params.get('forecast_simulations'))

        self.capital = float(self.params.get('capital'))
        self.long_only = bool(self.params.get('long_only'))

        risk_params = {
            'stop_loss_pct': self.params.get('stop_loss_pct'),
            'take_profit_pct': self.params.get('take_profit_pct'),
            'trailing_stop_pct': self.params.get('trail_stop_pct'),
            'slippage_pct': self.params.get('slippage_pct'),
            'transaction_cost_pct': self.params.get('transaction_cost_pct'),
        }
        self.risk_manager = RiskManager(**risk_params)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.model_base_folder = os.path.join("trading_system", "models", "garchx")
        self.model_config_folder = os.path.join(self.model_base_folder, "configs")
        self.model_cache_folder = os.path.join(self.model_base_folder, "cache")

        for folder in [self.model_base_folder, self.model_config_folder, self.model_cache_folder]:
            os.makedirs(folder, exist_ok=True)

        self._model_cache = {}
        self._feature_cache = {}
        self._forecast_cache = {}
        self._max_cache_size = 100
        self._cache_hits = 0
        self._cache_misses = 0

        self._perf_stats = {
            'feature_engineering_time': 0.0,
            'model_training_time': 0.0,
            'forecast_time': 0.0,
            'total_operations': 0
        }

    @timer_decorator
    def train_time_series_models(
        self,
        tickers: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, bool]:
        if isinstance(tickers, str):
            tickers = [tickers]

        self.logger.info(f"Starting time series model training for {len(tickers)} tickers")
        self.logger.info(f"Training period: {start_date} to {end_date}")
        self.logger.info(f"Using {self.cv_folds} forward CV folds with parallel processing: {self.parallel_cv}")

        if len(tickers) > 1:
            self.logger.info(f"Pre-fetching data for {len(tickers)} tickers in batch")
            self._prefetch_ticker_data(tickers, start_date, end_date)

        results = {}
        if len(tickers) > 1:
            batch_results = []
            for i in range(0, len(tickers), self.batch_size):
                batch = tickers[i:i + self.batch_size]
                batch_result = Parallel(
                    n_jobs=self.n_jobs,
                    backend=self.use_joblib_backend,
                    verbose=0
                )(
                    delayed(self._train_single_ticker_model)(ticker, start_date, end_date)
                    for ticker in batch
                )
                batch_results.extend(zip(batch, batch_result))

                if i % (self.batch_size * 5) == 0:
                    gc.collect()

            results = dict(batch_results)
        else:
            ticker = tickers[0]
            success = self._train_single_ticker_model(ticker, start_date, end_date)
            results = {ticker: success}

        successful_models = sum(results.values())
        self.logger.info(f"Training completed: {successful_models}/{len(tickers)} models trained successfully")

        self._clear_training_cache()
        gc.collect()

        self._log_performance_stats()

        return results

    def _prefetch_ticker_data(
        self,
        tickers: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> None:
        try:
            lookback_buffer = max(self.forecast_lookback, self.forecast_horizon) + 50

            if start_date and end_date:
                _ = self.get_historical_prices(
                    tickers, from_date=start_date, to_date=end_date, lookback=lookback_buffer
                )
            else:
                _ = self.get_historical_prices(tickers, lookback=lookback_buffer)

            self.logger.info(f"Pre-fetched data for {len(tickers)} tickers")

        except Exception as e:
            self.logger.warning(f"Failed to pre-fetch data: {str(e)}")

    def _train_single_ticker_model(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> bool:
        try:
            self.logger.info(f"Training model for ticker: {ticker}")

            lookback_buffer = max(self.forecast_lookback, self.forecast_horizon) + 50

            if start_date and end_date:
                data = self.get_historical_prices(
                    ticker, from_date=start_date, to_date=end_date, lookback=lookback_buffer
                )
            else:
                data = self.get_historical_prices(ticker, lookback=lookback_buffer)

            if data.empty or len(data) < self.forecast_lookback:
                self.logger.warning(f"Insufficient data for {ticker}: {len(data)} records")
                return False

            if not self._validate_price_data(data):
                self.logger.warning(f"Invalid price data for {ticker}")
                return False

            cache_key = f"{ticker}_{len(data)}_{self.vol_window}"
            if self.enable_feature_cache and cache_key in self._feature_cache:
                processed_data = self._feature_cache[cache_key]
                self._cache_hits += 1
            else:
                processed_data = self._prepare_model_features_robust(data)
                if self.enable_feature_cache and not processed_data.empty:
                    self._feature_cache[cache_key] = processed_data
                self._cache_misses += 1

            if processed_data.empty:
                self.logger.warning(f"Feature preparation failed for {ticker}")
                return False

            cv_results = self._forward_cross_validate_model(processed_data, ticker)
            if not cv_results:
                self.logger.warning(f"Cross-validation failed for {ticker}")
                return False

            best_model_info = self._select_best_model(cv_results, ticker)
            if best_model_info is None:
                self.logger.warning(f"No valid model found for {ticker}")
                return False

            success = self._save_ticker_model_optimized(ticker, best_model_info)
            if success:
                self.logger.info(f"Successfully trained and saved model for {ticker}")
            else:
                self.logger.error(f"Failed to save model for {ticker}")

            return success

        except Exception as e:
            self.logger.error(f"Error training model for {ticker}: {str(e)}")
            return False

    def _validate_price_data(self, data: pd.DataFrame) -> bool:
        required_cols = ['open', 'high', 'low', 'close', 'volume']

        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            self.logger.error(f"Missing required columns: {missing_cols}")
            return False

        if len(data) < self.forecast_lookback:
            self.logger.error(f"Insufficient data: {len(data)} < {self.forecast_lookback}")
            return False

        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if data[col].isnull().any():
                self.logger.warning(f"Found NaN values in {col}")
                data[col].fillna(method='ffill', inplace=True)

            if (data[col] <= 0).any() and col != 'volume':
                self.logger.error(f"Found non-positive values in {col}")
                return False

        if (data['high'] < data['low']).any():
            self.logger.error("Found high < low")
            return False

        if (data['high'] < data['close']).any() or (data['low'] > data['close']).any():
            self.logger.warning("Found close outside high-low range")

        return True

    @timer_decorator
    def _prepare_model_features_robust(self, data: pd.DataFrame) -> pd.DataFrame:
        try:
            df = data.copy()

            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index(level=0, drop=True)
            df.sort_index(inplace=True)

            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna(subset=['close', 'volume'])

            if len(df) < self.vol_window * 2:
                self.logger.error(f"Insufficient data after cleaning: {len(df)}")
                return pd.DataFrame()

            close_prices = df['close'].to_numpy(dtype=np.float64, copy=False)
            high_prices = df['high'].to_numpy(dtype=np.float64, copy=False)
            low_prices = df['low'].to_numpy(dtype=np.float64, copy=False)
            open_prices = df['open'].to_numpy(dtype=np.float64, copy=False)
            volumes = df['volume'].to_numpy(dtype=np.float64, copy=False)

            log_close = np.log(close_prices)
            log_returns = np.empty_like(log_close)
            log_returns[0] = np.nan
            log_returns[1:] = 100 * np.diff(log_close)
            df["log_return"] = log_returns

            volumes = np.where(volumes <= 0, 1e-8, volumes)
            vol_log = np.log(volumes + 1)
            df["vol_log"] = vol_log
            vol_series = pd.Series(vol_log, index=df.index, copy=False)

            if HAS_BOTTLENECK:
                vol_rolling_mean = bn.move_mean(vol_log, window=self.vol_window, min_count=self.vol_window)
                vol_rolling_std = bn.move_std(vol_log, window=self.vol_window, min_count=self.vol_window)
            else:
                vol_rolling_mean = _fast_rolling(vol_series, self.vol_window, 'mean')
                vol_rolling_std = _fast_rolling(vol_series, self.vol_window, 'std')

            vol_rolling_std = np.where(vol_rolling_std <= 0, 1e-8, vol_rolling_std)
            df["vol_z"] = (vol_log - vol_rolling_mean) / (vol_rolling_std + 1e-8)

            if HAS_BOTTLENECK:
                vol_ma = bn.move_mean(volumes, window=self.vol_window, min_count=self.vol_window)
            else:
                vol_ma = _fast_rolling(df['volume'], self.vol_window, 'mean')
            vol_ma = np.where(vol_ma <= 0, 1e-8, vol_ma)
            df["vol_ratio"] = volumes / vol_ma

            alpha = 2.0 / (self.vol_window + 1)
            vol_ema = _calculate_ema_numpy(vol_log, alpha)
            df["vol_ema"] = vol_ema

            if HAS_BOTTLENECK:
                vol_std = bn.move_std(vol_log, window=self.vol_window, min_count=1)
            else:
                vol_std = _fast_rolling(vol_series, self.vol_window, 'std', min_periods=1)
            vol_std = np.where(vol_std <= 0, 1e-8, vol_std)
            df["vol_z_ema"] = (vol_log - vol_ema) / vol_std

            prev_close = np.roll(close_prices, 1)
            prev_close[0] = close_prices[0]
            prev_close = np.where(prev_close <= 0, 1e-8, prev_close)

            df["hl_range"] = (high_prices - low_prices) / prev_close
            df["oc_range"] = np.abs(open_prices - close_prices) / prev_close

            hl = high_prices - low_prices
            hc = np.abs(high_prices - prev_close)
            lc = np.abs(low_prices - prev_close)
            true_range = np.maximum(hl, np.maximum(hc, lc)) / prev_close
            df["true_range"] = true_range

            high_safe = np.where(high_prices <= 0, 1e-8, high_prices)
            low_safe = np.where(low_prices <= 0, 1e-8, low_prices)
            log_hl = np.log(high_safe / low_safe)
            df["log_hl"] = log_hl

            log_hl_sq = log_hl ** 2
            if HAS_BOTTLENECK:
                parkinson_vol = np.sqrt(bn.move_mean(log_hl_sq, window=20, min_count=20) / (4 * np.log(2)))
            else:
                parkinson_vol = np.sqrt(_fast_rolling(log_hl_sq, 20, 'mean') / (4 * np.log(2)))
            df["parkinson_vol"] = parkinson_vol

            open_safe = np.where(open_prices <= 0, 1e-8, open_prices)
            close_safe = np.where(close_prices <= 0, 1e-8, close_prices)

            df["overnight_ret"] = 100 * np.log(open_safe / prev_close)
            df["oc_ret"] = 100 * np.log(close_safe / open_safe)

            if HAS_BOTTLENECK:
                overnight_var = bn.move_var(df["overnight_ret"].to_numpy(dtype=np.float64), window=20, min_count=20)
                oc_var = bn.move_var(df["oc_ret"].to_numpy(dtype=np.float64), window=20, min_count=20)
                log_hl_var = bn.move_var(log_hl, window=20, min_count=20)
            else:
                overnight_var = _fast_rolling(df["overnight_ret"], 20, 'var')
                oc_var = _fast_rolling(df["oc_ret"], 20, 'var')
                log_hl_var = _fast_rolling(log_hl, 20, 'var')

            overnight_var = np.where(overnight_var < 0, 0, overnight_var)
            oc_var = np.where(oc_var < 0, 0, oc_var)
            log_hl_var = np.where(log_hl_var < 0, 0, log_hl_var)

            df["yz_vol"] = np.sqrt(overnight_var + oc_var + 0.5 * log_hl_var)

            lag_count = 5
            lag_sources = {
                "return": df["log_return"].to_numpy(dtype=np.float64, copy=False),
                "vol": df["vol_z"].to_numpy(dtype=np.float64, copy=False),
                "hl": df["hl_range"].to_numpy(dtype=np.float64, copy=False),
                "tr": df["true_range"].to_numpy(dtype=np.float64, copy=False),
            }
            lagged_data = _build_lagged_arrays(lag_sources, lag_count)

            for prefix, arr in lagged_data.items():
                df[[f"lag{lag}_{prefix}" for lag in range(1, lag_count + 1)]] = arr

            df = df.replace([np.inf, -np.inf], np.nan)
            df.dropna(inplace=True)

            if len(df) < self.vol_window * 2:
                self.logger.error(f"Insufficient data after feature engineering: {len(df)}")
                return pd.DataFrame()

            return df

        except Exception as e:
            self.logger.error(f"Error in feature preparation: {str(e)}")
            return pd.DataFrame()

    def _forward_cross_validate_model(self, data: pd.DataFrame, ticker: str) -> List[Dict[str, Any]]:
        cv_results = []

        total_periods = len(data)
        min_train_size = self.forecast_lookback
        test_size = max(30, self.forecast_horizon)

        if total_periods < min_train_size + test_size * self.cv_folds:
            self.logger.warning(f"Insufficient data for {self.cv_folds} folds on {ticker}")
            reduced_folds = max(1, (total_periods - min_train_size) // test_size)
            if reduced_folds > 0:
                self.logger.info(f"Reducing folds from {self.cv_folds} to {reduced_folds}")
                self.cv_folds = reduced_folds
            else:
                return cv_results

        fold_ranges = []
        for fold in range(self.cv_folds):
            train_end_idx = min_train_size + fold * test_size
            test_start_idx = train_end_idx
            test_end_idx = min(test_start_idx + test_size, total_periods)

            if test_end_idx <= test_start_idx:
                break

            fold_ranges.append((train_end_idx, test_start_idx, test_end_idx))

        if not fold_ranges:
            return cv_results

        if self.parallel_cv and len(fold_ranges) > 1:
            try:
                with ThreadPoolExecutor(max_workers=min(len(fold_ranges), 4)) as executor:
                    futures = []
                    for fold_idx, (train_end, test_start, test_end) in enumerate(fold_ranges):
                        future = executor.submit(
                            self._train_model_on_fold_wrapper,
                            data, fold_idx, train_end, test_start, test_end, ticker
                        )
                        futures.append(future)

                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result is not None:
                                cv_results.append(result)
                        except Exception as e:
                            self.logger.warning(f"Fold processing failed: {str(e)}")

            except Exception as e:
                self.logger.warning(f"Parallel CV failed for {ticker}: {str(e)}")
                for fold_idx, (train_end, test_start, test_end) in enumerate(fold_ranges):
                    result = self._train_model_on_fold_wrapper(
                        data, fold_idx, train_end, test_start, test_end, ticker
                    )
                    if result:
                        cv_results.append(result)
        else:
            for fold_idx, (train_end, test_start, test_end) in enumerate(fold_ranges):
                result = self._train_model_on_fold_wrapper(
                    data, fold_idx, train_end, test_start, test_end, ticker
                )
                if result:
                    cv_results.append(result)

        return cv_results

    def _train_model_on_fold_wrapper(
        self,
        data: pd.DataFrame,
        fold_idx: int,
        train_end_idx: int,
        test_start_idx: int,
        test_end_idx: int,
        ticker: str
    ) -> Optional[Dict[str, Any]]:
        try:
            train_data = data.iloc[:train_end_idx].copy()
            test_data = data.iloc[test_start_idx:test_end_idx].copy()

            self.logger.debug(f"Fold {fold_idx+1}: Train={len(train_data)}, Test={len(test_data)}")

            return self._train_model_on_fold_robust(train_data, test_data, fold_idx, ticker)

        except Exception as e:
            self.logger.warning(f"Fold {fold_idx+1} failed for {ticker}: {str(e)}")
            return None

    @timer_decorator
    def _train_model_on_fold_robust(
        self,
        train_data: pd.DataFrame,
        test_data: pd.DataFrame,
        fold: int,
        ticker: str
    ) -> Optional[Dict[str, Any]]:
        try:
            exog_cols = [f"lag{l}_{feat}" for l in range(1, 6)
                        for feat in ["return", "vol", "hl", "tr"]]

            missing_cols = [col for col in exog_cols if col not in train_data.columns]
            if missing_cols:
                self.logger.error(f"Missing columns: {missing_cols}")
                return None

            X_train = train_data[exog_cols].to_numpy(dtype=np.float64)
            y_train = train_data["log_return"].to_numpy(dtype=np.float64)

            if np.any(np.isnan(X_train)) or np.any(np.isnan(y_train)):
                self.logger.warning("NaN values found in training data")
                mask = ~(np.any(np.isnan(X_train), axis=1) | np.isnan(y_train))
                X_train = X_train[mask]
                y_train = y_train[mask]

            if len(X_train) < 50:
                self.logger.warning(f"Insufficient training samples: {len(X_train)}")
                return None

            train_winsorize_params = {}
            lower_quantiles = np.nanpercentile(X_train, self.winsorize_quantiles[0] * 100, axis=0)
            upper_quantiles = np.nanpercentile(X_train, self.winsorize_quantiles[1] * 100, axis=0)

            for i, col in enumerate(exog_cols):
                train_winsorize_params[col] = (lower_quantiles[i], upper_quantiles[i])

            X_train = np.clip(X_train, lower_quantiles, upper_quantiles)

            y_lower = np.nanpercentile(y_train, self.winsorize_quantiles[0] * 100)
            y_upper = np.nanpercentile(y_train, self.winsorize_quantiles[1] * 100)
            y_train = np.clip(y_train, y_lower, y_upper)
            train_winsorize_params['target'] = (y_lower, y_upper)

            n_components = min(self.pca_components, X_train.shape[1], X_train.shape[0] // 10)
            if n_components < 1:
                n_components = 1

            pca = PCA(n_components=n_components, svd_solver='auto')
            try:
                X_train_pca = pca.fit_transform(X_train)
            except Exception as e:
                self.logger.warning(f"PCA failed: {str(e)}, using original features")
                X_train_pca = X_train[:, :n_components]
                pca = None

            X_train_pca_df = pd.DataFrame(
                X_train_pca,
                index=train_data.index[:len(X_train_pca)],
                columns=[f'pca_{i}' for i in range(X_train_pca.shape[1])]
            )
            y_train_series = pd.Series(y_train, index=train_data.index[:len(y_train)])

            best_model = None
            best_aic = np.inf
            best_order = None
            is_backup = False

            egarch_orders = [(1, 1), (1, 2), (2, 1)]
            optimization_attempts = [
                {'maxiter': 1000, 'ftol': 1e-4, 'tol': 1e-4},
                {'maxiter': 500,  'ftol': 1e-3, 'tol': 1e-3},
                {'maxiter': 300,  'ftol': 1e-2, 'tol': 1e-2}
            ]

            for (p, q) in egarch_orders:
                for attempt_idx, opt_params in enumerate(optimization_attempts):
                    try:
                        self.logger.debug(f"Trying EGARCH({p},{q}) - Attempt {attempt_idx + 1}")

                        model = arch_model(
                            y_train_series,
                            x=X_train_pca_df,
                            mean="ARX",
                            lags=0,
                            vol="EGARCH",
                            p=p,
                            q=q,
                            dist="t"
                        )

                        tol = opt_params.get('tol')
                        options = {k: v for k, v in opt_params.items() if k != 'tol'}
                        result = model.fit(
                            disp="off",
                            tol=tol,
                            options=options,
                            show_warning=False
                        )

                        if result.convergence_flag == 0 and result.aic < best_aic:
                            best_aic = result.aic
                            best_model = result
                            best_order = (p, q)
                            self.logger.debug(f"EGARCH({p},{q}) converged with AIC: {result.aic:.2f}")
                            break

                    except Exception as e:
                        self.logger.debug(f"EGARCH({p},{q}) attempt {attempt_idx + 1} failed: {str(e)}")
                        continue

                if best_model is not None:
                    break

            if best_model is None:
                self.logger.warning("All EGARCH models failed, trying simpler alternatives")

                simple_models = [
                    ("GARCH", [(1, 1), (1, 2), (2, 1)]),
                    ("ARCH", [(1,), (2,), (3,)])
                ]

                for model_type, orders in simple_models:
                    for order in orders:
                        for opt_params in optimization_attempts:
                            try:
                                if model_type == "GARCH":
                                    model = arch_model(
                                        y_train_series,
                                        mean="Constant",
                                        lags=0,
                                        vol="GARCH",
                                        p=order[0],
                                        q=order[1],
                                        dist="t"
                                    )
                                else:
                                    model = arch_model(
                                        y_train_series,
                                        mean="Constant",
                                        lags=0,
                                        vol="ARCH",
                                        p=order[0],
                                        dist="t"
                                    )

                                tol = opt_params.get('tol')
                                options = {k: v for k, v in opt_params.items() if k != 'tol'}
                                result = model.fit(
                                    disp="off",
                                    tol=tol,
                                    options=options,
                                    show_warning=False
                                )

                                if result.convergence_flag == 0:
                                    best_model = result
                                    best_order = order
                                    best_aic = result.aic
                                    is_backup = True
                                    self.logger.info(f"Backup {model_type}{order} converged")
                                    break

                            except Exception:
                                continue

                        if best_model is not None:
                            break

                    if best_model is not None:
                        break

            if best_model is None:
                self.logger.error(f"All models failed for fold {fold}")
                return None

            try:
                horizon_len = min(len(test_data), self.forecast_horizon)

                if is_backup:
                    if horizon_len == 1:
                        forecast = best_model.forecast(horizon=1, method='analytic')
                    else:
                        forecast = best_model.forecast(
                            horizon=horizon_len,
                            method='simulation',
                            simulations=100,
                            reindex=False
                        )
                else:
                    X_test = test_data[exog_cols].to_numpy(dtype=np.float64)[:horizon_len]

                    for i, col in enumerate(exog_cols):
                        lower, upper = train_winsorize_params[col]
                        X_test[:, i] = np.clip(X_test[:, i], lower, upper)

                    if pca is not None:
                        X_test_pca = pca.transform(X_test)
                    else:
                        X_test_pca = X_test[:, :X_train_pca.shape[1]]

                    X_test_pca_df = pd.DataFrame(
                        X_test_pca,
                        index=test_data.index[:horizon_len],
                        columns=[f'pca_{i}' for i in range(X_test_pca.shape[1])]
                    )

                    if horizon_len == 1:
                        x_dict = {col: X_test_pca_df[col].to_numpy(dtype=np.float64) for col in X_test_pca_df.columns}
                        forecast = best_model.forecast(
                            horizon=1,
                            x=x_dict,
                            method='analytic'
                        )
                    else:
                        forecast = best_model.forecast(
                            horizon=horizon_len,
                            x=X_test_pca_df,
                            method='simulation',
                            simulations=100,
                            reindex=False
                        )

                if hasattr(forecast.mean, 'iloc'):
                    forecast_returns = forecast.mean.iloc[-1].values
                else:
                    forecast_returns = forecast.mean.values.flatten()

                actual_returns = test_data["log_return"].to_numpy(dtype=np.float64)[:horizon_len]

                mse = np.mean((forecast_returns - actual_returns) ** 2)
                mae = np.mean(np.abs(forecast_returns - actual_returns))
                directional_accuracy = np.mean(
                    np.sign(forecast_returns) == np.sign(actual_returns)
                )

                if hasattr(forecast.variance, 'iloc'):
                    forecast_var = forecast.variance.iloc[-1].values
                else:
                    forecast_var = forecast.variance.values.flatten()

                forecast_vol = np.sqrt(np.mean(forecast_var))
                actual_vol = np.nanstd(actual_returns)
                vol_ratio = forecast_vol / (actual_vol + 1e-8)

            except Exception as e:
                self.logger.warning(f"Validation failed: {str(e)}")
                mse = np.inf
                mae = np.inf
                directional_accuracy = 0.0
                vol_ratio = 1.0

            return {
                'fold': fold,
                'model': best_model,
                'pca': pca,
                'order': best_order,
                'aic': best_aic,
                'mse': mse,
                'mae': mae,
                'directional_accuracy': directional_accuracy,
                'vol_ratio': vol_ratio,
                'winsorize_params': train_winsorize_params,
                'n_components': X_train_pca.shape[1] if not is_backup else 0,
                'explained_variance_ratio': pca.explained_variance_ratio_.sum() if pca is not None else 0.0,
                'is_backup': is_backup,
                'model_type': 'Backup' if is_backup else 'EGARCH-X'
            }

        except Exception as e:
            self.logger.error(f"Error training model on fold {fold}: {str(e)}")
            return None

    def _select_best_model(self, cv_results: List[Dict[str, Any]], ticker: str) -> Optional[Dict[str, Any]]:
        if not cv_results:
            return None

        valid_results = [r for r in cv_results if r['mse'] < np.inf]
        if not valid_results:
            self.logger.warning("No valid models found")
            return cv_results[0]

        scored_results = []
        for result in valid_results:
            aic_scores = [r['aic'] for r in valid_results]
            mse_scores = [r['mse'] for r in valid_results if np.isfinite(r['mse'])]
            mae_scores = [r['mae'] for r in valid_results if np.isfinite(r['mae'])]

            normalized_aic = result['aic'] / (np.mean(aic_scores) + 1e-8)
            normalized_mse = result['mse'] / (np.mean(mse_scores) + 1e-8) if mse_scores else 1.0
            normalized_mae = result['mae'] / (np.mean(mae_scores) + 1e-8) if mae_scores else 1.0

            directional_score = result['directional_accuracy']
            vol_penalty = abs(result['vol_ratio'] - 1.0)

            backup_penalty = 0.05 if result.get('is_backup', False) else 0.0

            combined_score = (
                0.25 * normalized_aic +
                0.25 * normalized_mse +
                0.20 * normalized_mae +
                0.15 * vol_penalty -
                0.15 * directional_score +
                backup_penalty
            )

            scored_results.append((combined_score, result))

        best_score, best_result = min(scored_results, key=lambda x: x[0])

        self.logger.info(
            f"Selected model for {ticker}: {best_result['model_type']}, "
            f"Order {best_result['order']}, AIC: {best_result['aic']:.2f}, "
            f"MSE: {best_result['mse']:.6f}, Dir.Acc: {best_result['directional_accuracy']:.3f}"
        )

        return best_result

    def _save_ticker_model_optimized(self, ticker: str, model_info: Dict[str, Any]) -> bool:
        try:
            safe_ticker = ticker.replace('.', '_').replace('/', '_')
            model_file = os.path.join(self.model_cache_folder, f"{safe_ticker}_model.pkl")

            model_data = {
                'model': model_info['model'],
                'pca': model_info.get('pca'),
                'order': model_info['order'],
                'aic': model_info['aic'],
                'mse': model_info['mse'],
                'mae': model_info.get('mae', np.inf),
                'directional_accuracy': model_info['directional_accuracy'],
                'vol_ratio': model_info.get('vol_ratio', 1.0),
                'winsorize_params': model_info['winsorize_params'],
                'n_components': model_info.get('n_components', 0),
                'explained_variance_ratio': model_info.get('explained_variance_ratio', 0.0),
                'trained_at': pd.Timestamp.now(),
                'params_hash': self._get_model_params_hash(),
                'is_backup': model_info.get('is_backup', False),
                'model_type': model_info.get('model_type', 'EGARCH-X')
            }

            dump(model_data, model_file, compress=3)

            config_file = os.path.join(self.model_config_folder, f"{safe_ticker}_config.json")
            config_data = {
                'ticker': ticker,
                'model_type': model_info.get('model_type', 'EGARCH-X'),
                'is_backup': model_info.get('is_backup', False),
                'order': model_info['order'],
                'aic': float(model_info['aic']),
                'mse': float(model_info['mse']),
                'mae': float(model_info.get('mae', np.inf)),
                'directional_accuracy': float(model_info['directional_accuracy']),
                'vol_ratio': float(model_info.get('vol_ratio', 1.0)),
                'n_components': model_info.get('n_components', 0),
                'trained_at': pd.Timestamp.now().isoformat(),
                'params_hash': self._get_model_params_hash()
            }

            with open(config_file, 'w') as f:
                json.dump(config_data, f, indent=2)

            return True

        except Exception as e:
            self.logger.error(f"Error saving model for {ticker}: {str(e)}")
            return False

    @lru_cache(maxsize=128)
    def _get_model_params_hash(self) -> str:
        model_params = {
            'pca_components': self.pca_components,
            'winsorize_quantiles': self.winsorize_quantiles,
            'vol_window': self.vol_window,
            'cv_folds': self.cv_folds,
            'forecast_lookback': self.forecast_lookback,
        }

        param_str = json.dumps(model_params, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()

    def _load_ticker_model_optimized(self, ticker: str) -> Optional[Dict[str, Any]]:
        try:
            if ticker in self._model_cache:
                self._cache_hits += 1
                return self._model_cache[ticker]

            self._cache_misses += 1

            safe_ticker = ticker.replace('.', '_').replace('/', '_')
            model_file = os.path.join(self.model_cache_folder, f"{safe_ticker}_model.pkl")

            if not os.path.exists(model_file):
                return None

            model_data = load(model_file)

            current_hash = self._get_model_params_hash()
            saved_hash = model_data.get('params_hash', '')

            if saved_hash != current_hash:
                self.logger.warning(f"Model parameters changed for {ticker}")
                return None

            if len(self._model_cache) >= self._max_cache_size:
                evict_key = next(iter(self._model_cache))
                del self._model_cache[evict_key]

            self._model_cache[ticker] = model_data

            return model_data

        except Exception as e:
            self.logger.error(f"Error loading model for {ticker}: {str(e)}")
            return None

    def _load_ticker_model(self, ticker: str) -> Optional[Dict[str, Any]]:
        return self._load_ticker_model_optimized(ticker)

    @timer_decorator
    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False,
    ) -> pd.DataFrame:
        try:
            if self.mode not in ["train", "forecast"]:
                raise ValueError(f"Invalid mode: {self.mode}")

            self.logger.info(f"Running in {self.mode} mode")

            lookback_buffer = max(self.forecast_lookback, self.forecast_horizon) + 50

            if start_date and end_date:
                data = self.get_historical_prices(
                    ticker, from_date=start_date, to_date=end_date, lookback=lookback_buffer
                )
            else:
                data = self.get_historical_prices(ticker, lookback=lookback_buffer)

            if data.empty:
                self.logger.warning(f"No data retrieved for {ticker}")
                return pd.DataFrame()

            data = data.sort_index()

            if isinstance(ticker, list):
                all_signals = []

                for batch_start in range(0, len(ticker), self.batch_size):
                    batch_end = min(batch_start + self.batch_size, len(ticker))
                    batch_tickers = ticker[batch_start:batch_end]

                    signals_list = []
                    for ticker_name in batch_tickers:
                        if ticker_name in data.index.get_level_values(0):
                            group = data.loc[ticker_name]

                            if not self._validate_price_data(group):
                                self.logger.warning(f"Invalid data for {ticker_name}")
                                continue

                            if self.mode == "forecast":
                                model_data = self._load_ticker_model_optimized(ticker_name)
                                if model_data is None:
                                    self.logger.warning(f"No model for {ticker_name}")
                                    continue

                            signals = self._calculate_signals_single_robust(
                                group, latest_only, ticker_name
                            )
                            if not signals.empty:
                                signals_list.append(signals)

                    if signals_list:
                        batch_signals = pd.concat(signals_list)
                        all_signals.append(batch_signals)

                    if batch_start % (self.batch_size * 5) == 0:
                        gc.collect()

                if not all_signals:
                    return pd.DataFrame()

                signals = pd.concat(all_signals)
                if latest_only:
                    signals = signals.groupby('ticker').tail(1)

            else:
                if not self._validate_price_data(data):
                    self.logger.warning(f"Invalid data for {ticker}")
                    return pd.DataFrame()

                if self.mode == "forecast":
                    model_data = self._load_ticker_model_optimized(ticker)
                    if model_data is None:
                        self.logger.warning(f"No model for {ticker}")
                        return pd.DataFrame()

                if not self._validate_data(data, min_records=self.forecast_lookback):
                    return pd.DataFrame()

                signals = self._calculate_signals_single_robust(data, latest_only, ticker)

            if signals.empty:
                return pd.DataFrame()

            signals = self.risk_manager.apply(signals, initial_position)

            signals['daily_return'] = signals['close'].pct_change().fillna(0)
            signals['strategy_return'] = signals['daily_return'] * signals['position'].shift(1).fillna(0)
            signals.rename(
                columns={'return': 'rm_strategy_return',
                         'cumulative_return': 'rm_cumulative_return',
                         'exit_type': 'rm_action'},
                inplace=True,
            )

            return signals

        except Exception as e:
            self.logger.error(f"Error generating signals: {str(e)}")
            return pd.DataFrame()

    @timer_decorator
    def _calculate_signals_single_robust(
        self,
        data: pd.DataFrame,
        latest_only: bool,
        ticker_name: Optional[str] = None
    ) -> pd.DataFrame:
        try:
            if self.enable_forecast_cache and latest_only:
                cache_key = f"{ticker_name}_{len(data)}_{self.forecast_horizon}"
                if cache_key in self._forecast_cache:
                    self._cache_hits += 1
                    return self._forecast_cache[cache_key]

            feature_cache_key = f"{ticker_name}_{len(data)}_{self.vol_window}"
            if self.enable_feature_cache and feature_cache_key in self._feature_cache:
                df = self._feature_cache[feature_cache_key]
            else:
                df = self._prepare_model_features_robust(data)
                if self.enable_feature_cache and not df.empty:
                    self._feature_cache[feature_cache_key] = df

            if df.empty:
                self.logger.warning(f"Feature preparation failed for {ticker_name}")
                return pd.DataFrame()

            required_features = [f"lag{l}_{feat}" for l in range(1, 6) for feat in ["return", "vol", "hl", "tr"]]
            missing_features = [f for f in required_features if f not in df.columns]
            if missing_features:
                self.logger.error(f"Missing features: {missing_features}")
                return pd.DataFrame()

            df["forecast_return"] = np.nan
            df["signal"] = np.nan
            df["signal_strength"] = np.nan
            df["position_size"] = np.nan

            if latest_only:
                train = df.iloc[-self.forecast_lookback:] if len(df) >= self.forecast_lookback else df

                if len(train) < self.vol_window * 2:
                    self.logger.warning(f"Insufficient data for forecast: {len(train)}")
                    return pd.DataFrame()

                fc_ret, sig, sig_str, pos_size = self._fit_forecast_robust(
                    train, self.forecast_horizon, ticker_name
                )

                if fc_ret is not None:
                    df.loc[df.index[-1], "forecast_return"] = fc_ret
                    df.loc[df.index[-1], "signal"] = sig
                    df.loc[df.index[-1], "signal_strength"] = sig_str
                    df.loc[df.index[-1], "position_size"] = pos_size

            else:
                n_forecasts = len(df) - self.forecast_lookback - self.forecast_horizon + 1

                if n_forecasts > 0:
                    successful_forecasts = 0
                    batch_size = min(50, n_forecasts)

                    for batch_start in range(0, n_forecasts, batch_size):
                        batch_end = min(batch_start + batch_size, n_forecasts)

                        for i in range(batch_start, batch_end):
                            train = df.iloc[i: i + self.forecast_lookback]

                            if len(train) < self.vol_window * 2:
                                continue

                            forecast_dates = df.index[
                                i + self.forecast_lookback: i + self.forecast_lookback + self.forecast_horizon
                            ]

                            fc_ret, sig, sig_str, pos_size = self._fit_forecast_robust(
                                train, self.forecast_horizon, ticker_name
                            )

                            if fc_ret is not None:
                                df.loc[forecast_dates, "forecast_return"] = fc_ret
                                df.loc[forecast_dates, "signal"] = sig
                                df.loc[forecast_dates, "signal_strength"] = sig_str
                                df.loc[forecast_dates, "position_size"] = pos_size
                                successful_forecasts += 1

                    if successful_forecasts == 0:
                        self.logger.warning(f"No successful forecasts for {ticker_name}")

            df.fillna(method="ffill", inplace=True)

            cols = ["open", "high", "low", "close", "forecast_return",
                    "signal", "signal_strength", "position_size"]
            result = df[cols].copy()

            if ticker_name:
                result['ticker'] = ticker_name

            if self.enable_forecast_cache and latest_only and not result.empty:
                self._forecast_cache[cache_key] = result

            return result

        except Exception as e:
            self.logger.error(f"Error calculating signals: {str(e)}")
            return pd.DataFrame()

    @timer_decorator
    def _fit_forecast_robust(
        self,
        train: pd.DataFrame,
        horizon: int,
        ticker_name: Optional[str] = None
    ) -> Tuple[Optional[float], int, float, float]:
        try:
            if self.mode == "forecast" and ticker_name:
                model_data = self._load_ticker_model_optimized(ticker_name)
                if model_data is None:
                    return None, 0, 0.0, 0.0
                else:
                    return self._forecast_with_model_robust(train, horizon, model_data)
            else:
                return self._train_and_forecast_robust(train, horizon, ticker_name)

        except Exception as e:
            self.logger.error(f"Error in fit_forecast: {str(e)}")
            return None, 0, 0.0, 0.0

    def _forecast_with_model_robust(
        self,
        train: pd.DataFrame,
        horizon: int,
        model_data: Dict[str, Any]
    ) -> Tuple[Optional[float], int, float, float]:
        try:
            model = model_data['model']
            pca = model_data.get('pca')
            winsorize_params = model_data['winsorize_params']
            n_components = model_data.get('n_components', 0)
            is_backup = model_data.get('is_backup', False)

            if horizon == 1:
                forecast_method = 'analytic'
                simulations = None
            else:
                forecast_method = 'simulation'
                simulations = self.forecast_simulations

            if is_backup:
                try:
                    if forecast_method == 'analytic':
                        forecast = model.forecast(horizon=1, method='analytic')
                    else:
                        forecast = model.forecast(
                            horizon=horizon,
                            method='simulation',
                            simulations=simulations,
                            reindex=False
                        )
                except Exception as e:
                    self.logger.warning(f"Backup model forecast failed: {str(e)}")
                    try:
                        forecast = model.forecast(
                            horizon=horizon,
                            method='bootstrap',
                            reindex=False
                        )
                    except:
                        return None, 0, 0.0, 0.0

            else:
                exog_cols = [f"lag{l}_{feat}" for l in range(1, 6)
                            for feat in ["return", "vol", "hl", "tr"]]

                missing_cols = [col for col in exog_cols if col not in train.columns]
                if missing_cols:
                    self.logger.error(f"Missing columns: {missing_cols}")
                    return None, 0, 0.0, 0.0

                X_recent = train[exog_cols].iloc[-1:].to_numpy(dtype=np.float64)

                for i, col in enumerate(exog_cols):
                    if col in winsorize_params:
                        lower, upper = winsorize_params[col]
                        X_recent[0, i] = np.clip(X_recent[0, i], lower, upper)

                if pca is not None:
                    X_recent_pca = pca.transform(X_recent)
                else:
                    X_recent_pca = X_recent[:, :n_components]

                if horizon == 1:
                    x_dict = {f'pca_{i}': np.array([X_recent_pca[0, i]])
                             for i in range(X_recent_pca.shape[1])}
                else:
                    x_dict = {f'pca_{i}': np.full(horizon, X_recent_pca[0, i])
                             for i in range(X_recent_pca.shape[1])}

                try:
                    if forecast_method == 'analytic':
                        forecast = model.forecast(
                            horizon=1,
                            x=x_dict,
                            method='analytic'
                        )
                    else:
                        forecast = model.forecast(
                            horizon=horizon,
                            x=x_dict,
                            method='simulation',
                            simulations=simulations,
                            reindex=False
                        )
                except Exception as e:
                    self.logger.warning(f"Primary forecast failed: {str(e)}")
                    try:
                        forecast = model.forecast(
                            horizon=horizon,
                            method='simulation' if horizon > 1 else 'analytic',
                            simulations=simulations if horizon > 1 else None,
                            reindex=False
                        )
                    except:
                        return None, 0, 0.0, 0.0

            if hasattr(forecast.mean, 'iloc'):
                fc_means = forecast.mean.iloc[-1].values
                fc_variances = forecast.variance.iloc[-1].values
            else:
                fc_means = forecast.mean.values.flatten()
                fc_variances = forecast.variance.values.flatten()

            cumulative_ret = (np.exp(np.sum(fc_means / 100)) - 1) * 100
            forecast_vol = np.sqrt(np.mean(fc_variances))
            confidence_bound = 1.96 * forecast_vol

            return self._calculate_signal_from_forecast(
                cumulative_ret, forecast_vol, train, confidence_bound
            )

        except Exception as e:
            self.logger.error(f"Error in forecast: {str(e)}")
            return None, 0, 0.0, 0.0

    def _train_and_forecast_robust(
        self,
        train: pd.DataFrame,
        horizon: int,
        ticker_name: Optional[str] = None
    ) -> Tuple[Optional[float], int, float, float]:
        try:
            exog_cols = [f"lag{l}_{feat}" for l in range(1, 6)
                        for feat in ["return", "vol", "hl", "tr"]]

            missing_cols = [col for col in exog_cols if col not in train.columns]
            if missing_cols:
                self.logger.error(f"Missing columns: {missing_cols}")
                return None, 0, 0.0, 0.0

            X_train = train[exog_cols].to_numpy(dtype=np.float64)
            y_train = train["log_return"].to_numpy(dtype=np.float64)

            if len(X_train) < 50:
                return None, 0, 0.0, 0.0

            mask = ~(np.any(np.isnan(X_train), axis=1) | np.isnan(y_train))
            X_train = X_train[mask]
            y_train = y_train[mask]

            if len(X_train) < 30:
                return None, 0, 0.0, 0.0

            winsorize_params = {}
            lower_quantiles = np.nanpercentile(X_train, self.winsorize_quantiles[0] * 100, axis=0)
            upper_quantiles = np.nanpercentile(X_train, self.winsorize_quantiles[1] * 100, axis=0)

            for i, col in enumerate(exog_cols):
                winsorize_params[col] = (lower_quantiles[i], upper_quantiles[i])

            X_train = np.clip(X_train, lower_quantiles, upper_quantiles)

            y_lower = np.nanpercentile(y_train, self.winsorize_quantiles[0] * 100)
            y_upper = np.nanpercentile(y_train, self.winsorize_quantiles[1] * 100)
            y_train = np.clip(y_train, y_lower, y_upper)
            winsorize_params['target'] = (y_lower, y_upper)

            n_components = min(self.pca_components, X_train.shape[1], X_train.shape[0] // 10)
            if n_components < 1:
                n_components = 1

            pca = PCA(n_components=n_components)
            try:
                X_train_pca = pca.fit_transform(X_train)
            except:
                X_train_pca = X_train[:, :n_components]
                pca = None

            valid_indices = train.index[mask]
            X_train_pca_df = pd.DataFrame(
                X_train_pca,
                index=valid_indices,
                columns=[f'pca_{i}' for i in range(X_train_pca.shape[1])]
            )
            y_train_series = pd.Series(y_train, index=valid_indices)

            best_result = None
            is_backup = False

            for attempt in range(self.max_convergence_attempts):
                try:
                    tol_val = 1e-4 * (10 ** attempt)
                    opt_params = {
                        'maxiter': max(100, 1000 - attempt * 200),
                        'ftol': tol_val
                    }

                    model = arch_model(
                        y_train_series,
                        x=X_train_pca_df,
                        mean="ARX",
                        lags=0,
                        vol="EGARCH",
                        p=1,
                        q=1,
                        dist="t"
                    )

                    result = model.fit(
                        disp="off",
                        tol=tol_val,
                        options=opt_params,
                        show_warning=False
                    )

                    if result.convergence_flag == 0:
                        best_result = result
                        break

                except Exception:
                    continue

            if best_result is None:
                for attempt in range(self.max_convergence_attempts):
                    try:
                        tol_val = 1e-3 * (10 ** attempt)
                        opt_params = {
                            'maxiter': max(100, 500 - attempt * 100),
                            'ftol': tol_val
                        }

                        model = arch_model(
                            y_train_series,
                            mean="Constant",
                            lags=0,
                            vol="GARCH",
                            p=1,
                            q=1,
                            dist="t"
                        )

                        result = model.fit(
                            disp="off",
                            tol=tol_val,
                            options=opt_params,
                            show_warning=False
                        )

                        if result.convergence_flag == 0:
                            best_result = result
                            is_backup = True
                            break

                    except Exception:
                        continue

            if best_result is None:
                return None, 0, 0.0, 0.0

            if horizon == 1:
                forecast_method = 'analytic'
            else:
                forecast_method = 'simulation'

            if is_backup:
                if forecast_method == 'analytic':
                    forecast = best_result.forecast(horizon=1, method='analytic')
                else:
                    forecast = best_result.forecast(
                        horizon=horizon,
                        method='simulation',
                        simulations=self.forecast_simulations,
                        reindex=False
                    )
            else:
                if pca is not None:
                    last_exog = X_train[-1:]
                    last_exog_pca = pca.transform(last_exog)
                else:
                    last_exog_pca = X_train_pca[-1:]

                if horizon == 1:
                    x_dict = {f'pca_{i}': np.array([last_exog_pca[0, i]])
                              for i in range(last_exog_pca.shape[1])}
                    forecast = best_result.forecast(
                        horizon=1,
                        x=x_dict,
                        method='analytic'
                    )
                else:
                    x_dict = {f'pca_{i}': np.full(horizon, last_exog_pca[0, i])
                              for i in range(last_exog_pca.shape[1])}
                    forecast = best_result.forecast(
                        horizon=horizon,
                        x=x_dict,
                        method='simulation',
                        simulations=self.forecast_simulations,
                        reindex=False
                    )

            if hasattr(forecast.mean, 'iloc'):
                fc_means = forecast.mean.iloc[-1].values
                fc_variances = forecast.variance.iloc[-1].values
            else:
                fc_means = forecast.mean.values.flatten()
                fc_variances = forecast.variance.values.flatten()

            cumulative_ret = (np.exp(np.sum(fc_means / 100)) - 1) * 100
            forecast_vol = np.sqrt(np.mean(fc_variances))
            confidence_bound = 1.96 * forecast_vol

            if self.mode == "train" and ticker_name and best_result.convergence_flag == 0:
                model_info = {
                    'model': best_result,
                    'pca': pca,
                    'order': (1, 1),
                    'aic': best_result.aic,
                    'mse': 0.0,
                    'mae': 0.0,
                    'directional_accuracy': 0.0,
                    'vol_ratio': 1.0,
                    'winsorize_params': winsorize_params,
                    'n_components': X_train_pca.shape[1] if not is_backup else 0,
                    'explained_variance_ratio': pca.explained_variance_ratio_.sum() if pca is not None else 0.0,
                    'is_backup': is_backup,
                    'model_type': 'Backup' if is_backup else 'EGARCH-X'
                }
                self._save_ticker_model_optimized(ticker_name, model_info)

            return self._calculate_signal_from_forecast(
                cumulative_ret, forecast_vol, train, confidence_bound
            )

        except Exception as e:
            self.logger.error(f"Error in train and forecast: {str(e)}")
            return None, 0, 0.0, 0.0

    def _calculate_signal_from_forecast(
        self,
        cumulative_ret: float,
        forecast_vol: float,
        train: pd.DataFrame,
        confidence_bound: Optional[float] = None
    ) -> Tuple[float, int, float, float]:
        try:
            if np.isnan(cumulative_ret) or np.isnan(forecast_vol):
                self.logger.warning("NaN values in forecast results")
                return 0.0, 0, 0.0, 0.0

            if forecast_vol < 0:
                self.logger.warning(f"Negative volatility: {forecast_vol}")
                forecast_vol = abs(forecast_vol)

            risk_adj_return = cumulative_ret / (forecast_vol + 1e-8)

            vol_z = 0.0
            vol_z_ema = 0.0

            if "vol_z" in train.columns:
                vol_z_val = train["vol_z"].iloc[-1]
                if not np.isnan(vol_z_val):
                    vol_z = vol_z_val

            if "vol_z_ema" in train.columns:
                vol_z_ema_val = train["vol_z_ema"].iloc[-1]
                if not np.isnan(vol_z_ema_val):
                    vol_z_ema = vol_z_ema_val

            if vol_z != 0 or vol_z_ema != 0:
                vol_strength = (vol_z + vol_z_ema) / 2
            else:
                vol_strength = 0.0

            base_signal_strength = risk_adj_return * vol_strength

            if confidence_bound and confidence_bound > 0:
                confidence_factor = min(abs(cumulative_ret) / confidence_bound, 2.0)
                signal_strength = base_signal_strength * confidence_factor
            else:
                signal_strength = base_signal_strength

            signal_meets_strength = abs(signal_strength) > self.signal_strength_threshold
            volume_meets_threshold = abs(vol_strength) > self.min_volume_strength

            confidence_check = True
            if confidence_bound is not None and confidence_bound > 0:
                confidence_check = abs(cumulative_ret) > 0.5 * confidence_bound

            if (risk_adj_return > 0 and signal_meets_strength and
                volume_meets_threshold and confidence_check):
                sig = 1
            elif (risk_adj_return < 0 and signal_meets_strength and
                  volume_meets_threshold and confidence_check):
                sig = 0 if self.long_only else -1
            else:
                sig = 0

            base_position_size = risk_adj_return * self.capital
            if confidence_bound and confidence_bound > 0:
                confidence_factor = min(abs(cumulative_ret) / confidence_bound, 1.5)
                position_size = base_position_size * confidence_factor
            else:
                position_size = base_position_size

            position_size = np.clip(position_size, -10.0, 10.0)
            signal_strength = np.clip(signal_strength, -10.0, 10.0)

            return cumulative_ret, sig, signal_strength, position_size

        except Exception as e:
            self.logger.error(f"Error calculating signal: {str(e)}")
            return 0.0, 0, 0.0, 0.0

    def _clear_training_cache(self):
        self._feature_cache.clear()
        self._forecast_cache.clear()
        self.logger.debug(f"Training cache cleared. Cache stats - Hits: {self._cache_hits}, Misses: {self._cache_misses}")

    def clear_model_cache(self):
        self._model_cache.clear()
        self._feature_cache.clear()
        self._forecast_cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self.logger.info("All caches cleared")

    def get_model_status(self, tickers: Union[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        if isinstance(tickers, str):
            tickers = [tickers]

        status = {}
        current_hash = self._get_model_params_hash()

        for ticker in tickers:
            safe_ticker = ticker.replace('.', '_').replace('/', '_')
            model_file = os.path.join(self.model_cache_folder, f"{safe_ticker}_model.pkl")
            config_file = os.path.join(self.model_config_folder, f"{safe_ticker}_config.json")

            ticker_status = {
                'model_exists': os.path.exists(model_file),
                'config_exists': os.path.exists(config_file),
                'cached': ticker in self._model_cache,
                'current_params_hash': current_hash
            }

            if ticker_status['config_exists']:
                try:
                    with open(config_file, 'r') as f:
                        config = json.load(f)
                    ticker_status.update(config)

                    saved_hash = config.get('params_hash', '')
                    ticker_status['needs_retraining'] = (saved_hash != current_hash)
                    ticker_status['hash_match'] = (saved_hash == current_hash)

                except Exception as e:
                    ticker_status['config_error'] = str(e)

            status[ticker] = ticker_status

        status['_cache_stats'] = {
            'model_cache_size': len(self._model_cache),
            'feature_cache_size': len(self._feature_cache),
            'forecast_cache_size': len(self._forecast_cache),
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'hit_rate': self._cache_hits / (self._cache_hits + self._cache_misses) if (self._cache_hits + self._cache_misses) > 0 else 0
        }

        return status

    def _log_performance_stats(self):
        if self._perf_stats['total_operations'] > 0:
            self.logger.info(
                f"Performance Stats - "
                f"Feature Eng: {self._perf_stats['feature_engineering_time']:.2f}s, "
                f"Model Training: {self._perf_stats['model_training_time']:.2f}s, "
                f"Forecast: {self._perf_stats['forecast_time']:.2f}s, "
                f"Total Ops: {self._perf_stats['total_operations']}"
            )

            if self._cache_hits + self._cache_misses > 0:
                hit_rate = self._cache_hits / (self._cache_hits + self._cache_misses)
                self.logger.info(f"Cache Performance - Hit Rate: {hit_rate:.2%}")

    def get_performance_report(self) -> Dict[str, Any]:
        total_time = sum(v for k, v in self._perf_stats.items() if 'time' in k)

        return {
            'timing': {
                'feature_engineering': self._perf_stats['feature_engineering_time'],
                'model_training': self._perf_stats['model_training_time'],
                'forecasting': self._perf_stats['forecast_time'],
                'total': total_time
            },
            'operations': self._perf_stats['total_operations'],
            'cache_performance': {
                'hits': self._cache_hits,
                'misses': self._cache_misses,
                'hit_rate': self._cache_hits / (self._cache_hits + self._cache_misses) if (self._cache_hits + self._cache_misses) > 0 else 0,
                'model_cache_size': len(self._model_cache),
                'feature_cache_size': len(self._feature_cache),
                'forecast_cache_size': len(self._forecast_cache)
            },
            'memory_usage': {
                'model_cache_mb': sum(
                    len(pickle.dumps(v)) / (1024 * 1024)
                    for v in self._model_cache.values()
                ) if self._model_cache else 0
            }
        }