# trading_system/src/strategies/advanced/prophet_momentum_strat.py

import concurrent.futures
import json
import logging
import os
from datetime import timedelta
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Union

import joblib
import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from hyperopt import STATUS_OK, Trials, fmin, hp, space_eval, tpe
from prophet import Prophet
from scipy import stats
from tqdm import tqdm  # Optional: for progress bars

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager

# --- Configuration ---
MODEL_CACHE_DIR = os.path.join("trading_system", "models", "prophet_momentum")
PARAMS_CACHE_DIR = os.path.join("trading_system", "params_cache", "prophet_momentum")
os.makedirs(MODEL_CACHE_DIR, exist_ok=True)
os.makedirs(PARAMS_CACHE_DIR, exist_ok=True)

# --- Helper Functions ---

def _calculate_rsi(series: pd.Series, period: int) -> pd.Series:
    """Calculates the Relative Strength Index (RSI)."""
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period, min_periods=period).mean()
    loss = -delta.clip(upper=0).rolling(window=period, min_periods=period).mean()
    rs = gain / (loss + 1e-9)  # Add epsilon to avoid division by zero
    rsi = 100 - (100 / (1 + rs))
    return rsi

def _get_ticker_model_path(ticker: str) -> str:
    """Returns the file path for a ticker's Prophet model."""
    return os.path.join(MODEL_CACHE_DIR, f"{ticker}_prophet_model.pkl")

def _get_ticker_prophet_params_path(ticker: str) -> str:
    """Returns the file path for a ticker's tuned Prophet hyperparameters."""
    return os.path.join(PARAMS_CACHE_DIR, f"{ticker}_prophet_params.json")

def _get_strategy_params_path(strategy_id: str) -> str:
    """Returns the file path for tuned strategy hyperparameters."""
    return os.path.join(PARAMS_CACHE_DIR, f"{strategy_id}_strategy_params.json")


# --- Strategy Class ---

class ProphetMomentumStrategy(BaseStrategy):
    """
    Prophet Momentum Strategy with Hyperparameter Tuning and Risk Management.

    Generates trading signals based on Prophet forecasts, trend significance,
    and optional HMM-based market regime filtering. Supports multiple modes:
    - 'backtest': Runs a rolling backtest simulation.
    - 'tune_prophet': Tunes Prophet hyperparameters for each ticker using forward CV.
    - 'tune_strategy': Tunes strategy-level parameters (thresholds, indicators, risk)
                       using backtesting results, assuming Prophet models/params exist.
    - 'train': Trains and saves the final Prophet model using all available data and
               best (or default) hyperparameters.
    - 'forecast': Loads the trained model and generates a signal for the latest data point.

    Integrates with RiskManager for stop-loss, take-profit, slippage, and costs.
    """
    MODES = ['backtest', 'tune_prophet', 'tune_strategy', 'train', 'forecast']

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict[str, Any]] = None):
        """
        Initialize the Prophet Momentum Strategy.

        Args:
            db_config (DatabaseConfig): Database configuration.
            params (dict, optional): Strategy parameters. Key parameters include:
                - mode (str): Operation mode ('backtest', 'tune_prophet', etc.). Default 'backtest'.
                - prophet_params_scope (str): 'ticker' (tune per ticker) or 'global' (tune once). Default 'ticker'.
                - strategy_params_id (str): Identifier for saving/loading tuned strategy params. Default 'default_strategy'.
                - lookback_years (int): Years of data for training/tuning. Default 3.
                - retrain_frequency (str): How often to retrain in backtest ('D', 'W', 'M', 'Q', 'A'). Default 'M'.
                - prophet_tuning_evals (int): Max evaluations for Prophet hyperopt. Default 50.
                - strategy_tuning_evals (int): Max evaluations for strategy hyperopt. Default 100.
                - use_regressors (bool): Whether to use OHLC, Volume, RSI as regressors. Default True.
                - horizon_short, horizon_mid, horizon_long (int): Forecast horizons in days. Defaults 7, 14, 30.
                - t_stat_threshold_long, t_stat_threshold_short (float): T-stat thresholds for signals. Defaults 2.0, -2.0.
                - vol_window (int): Rolling window for HMM volatility. Default 21.
                - hmm_states (int): Number of HMM states. Default 2.
                - regime_override (bool): Whether HMM regime detection overrides signals. Default True.
                - rsi_period (int): Period for RSI calculation. Default 14.
                - stop_loss_pct, take_profit_pct, trailing_stop_pct (float): RiskManager parameters.
                - slippage_pct, transaction_cost_pct (float): RiskManager cost parameters.
                - max_workers (int): Max workers for parallel processing. Default None (uses os.cpu_count()).
        """
        default_params = {
            # Operational Params
            'mode': 'backtest',
            'prophet_params_scope': 'ticker', # 'ticker' or 'global'
            'strategy_params_id': 'default_strategy',
            'lookback_years': 3,
            'retrain_frequency': 'M', # 'D', 'W', 'M', 'Q', 'A' or None (train once)
            'prophet_tuning_evals': 50,
            'strategy_tuning_evals': 100,
            'use_regressors': True,
            # Prophet Model Params (Defaults for untuned)
            'changepoint_prior_scale': 0.05,
            'seasonality_prior_scale': 10.0,
            'holidays_prior_scale': 10.0,
            'seasonality_mode': 'multiplicative', # 'additive' or 'multiplicative'
            'fourier_order': 10,
            # Strategy Logic Params
            'horizon_short': 7,
            'horizon_mid': 14,
            'horizon_long': 30,
            't_stat_threshold_long': 2.0,
            't_stat_threshold_short': -2.0,
            'vol_window': 21,
            'hmm_states': 2,
            'regime_override': True,
            'rsi_period': 14,
            # Risk Management Params
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0, # Set > 0 to enable
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            # Execution Params
            'max_workers': None, # Uses os.cpu_count() if None
        }
        # Ensure provided params override defaults
        if params:
             # Convert numeric params from string if necessary (e.g., from config file)
            for k, v in params.items():
                if k in default_params:
                    dtype = type(default_params[k])
                    try:
                        params[k] = dtype(v)
                    except (ValueError, TypeError):
                         self.logger.warning(f"Could not convert param '{k}' value '{v}' to type {dtype}. Using default.")
                         params[k] = default_params[k] # fallback to default if conversion fails
            default_params.update(params)


        super().__init__(db_config, default_params)

        self.mode = self.params.get('mode', 'backtest')
        if self.mode not in self.MODES:
            raise ValueError(f"Invalid mode '{self.mode}'. Must be one of {self.MODES}")

        self.risk_manager = RiskManager(
            stop_loss_pct=self.params['stop_loss_pct'],
            take_profit_pct=self.params['take_profit_pct'],
            trailing_stop_pct=self.params['trailing_stop_pct'],
            slippage_pct=self.params['slippage_pct'],
            transaction_cost_pct=self.params['transaction_cost_pct']
        )

        self.max_workers = self.params.get('max_workers') # Let concurrent.futures handle None

        self.prophet_params_cache = {} # Cache loaded prophet params {ticker: params_dict}
        self.strategy_params = self._load_strategy_params() # Load tuned strategy params if they exist

        # Override defaults with tuned strategy params if available
        self.params.update(self.strategy_params)
        # Re-initialize risk manager if params changed
        if any(k in self.strategy_params for k in ['stop_loss_pct', 'take_profit_pct', 'trailing_stop_pct', 'slippage_pct', 'transaction_cost_pct']):
             self.risk_manager = RiskManager(
                stop_loss_pct=self.params['stop_loss_pct'],
                take_profit_pct=self.params['take_profit_pct'],
                trailing_stop_pct=self.params['trailing_stop_pct'],
                slippage_pct=self.params['slippage_pct'],
                transaction_cost_pct=self.params['transaction_cost_pct']
            )


    def _load_prophet_params(self, ticker: str) -> Dict[str, Any]:
        """Loads tuned Prophet parameters for a ticker from cache or file."""
        if ticker in self.prophet_params_cache:
            return self.prophet_params_cache[ticker]

        params_path = _get_ticker_prophet_params_path(ticker)
        if os.path.exists(params_path):
            try:
                with open(params_path, 'r') as f:
                    params = json.load(f)
                self.prophet_params_cache[ticker] = params
                self.logger.debug(f"Loaded Prophet params for {ticker} from {params_path}")
                return params
            except Exception as e:
                self.logger.warning(f"Failed to load Prophet params for {ticker} from {params_path}: {e}")
        # Return default Prophet params if no tuned params found
        return {
            'changepoint_prior_scale': self.params['changepoint_prior_scale'],
            'seasonality_prior_scale': self.params['seasonality_prior_scale'],
            'holidays_prior_scale': self.params['holidays_prior_scale'],
            'seasonality_mode': self.params['seasonality_mode'],
            'fourier_order': int(self.params['fourier_order']), # Ensure int
        }

    def _save_prophet_params(self, ticker: str, params: Dict[str, Any]):
        """Saves tuned Prophet parameters for a ticker."""
        params_path = _get_ticker_prophet_params_path(ticker)
        try:
            # Ensure fourier_order is int for JSON serialization
            params['fourier_order'] = int(params['fourier_order'])
            with open(params_path, 'w') as f:
                json.dump(params, f, indent=4)
            self.prophet_params_cache[ticker] = params # Update cache
            self.logger.info(f"Saved Prophet params for {ticker} to {params_path}")
        except Exception as e:
            self.logger.error(f"Failed to save Prophet params for {ticker} to {params_path}: {e}")

    def _load_strategy_params(self) -> Dict[str, Any]:
        """Loads tuned strategy parameters."""
        params_path = _get_strategy_params_path(self.params['strategy_params_id'])
        if os.path.exists(params_path):
            try:
                with open(params_path, 'r') as f:
                    s_params = json.load(f)
                self.logger.info(f"Loaded strategy params from {params_path}")
                # Basic type conversion from JSON strings if needed
                converted_params = {}
                default_types = {
                    't_stat_threshold_long': float, 't_stat_threshold_short': float,
                    'vol_window': int, 'hmm_states': int, 'rsi_period': int,
                    'stop_loss_pct': float, 'take_profit_pct': float, 'trailing_stop_pct': float,
                    'slippage_pct': float, 'transaction_cost_pct': float
                }
                for k, v in s_params.items():
                    if k in default_types:
                        try:
                            converted_params[k] = default_types[k](v)
                        except (ValueError, TypeError):
                            self.logger.warning(f"Could not convert strategy param {k}={v} to type {default_types[k]}")
                            converted_params[k] = v # Keep original if conversion fails
                    else:
                         converted_params[k] = v # Keep other params as is
                return converted_params
            except Exception as e:
                self.logger.warning(f"Failed to load strategy params from {params_path}: {e}")
        return {}

    def _save_strategy_params(self, params: Dict[str, Any]):
        """Saves tuned strategy parameters."""
        params_path = _get_strategy_params_path(self.params['strategy_params_id'])
        try:
            with open(params_path, 'w') as f:
                # Ensure basic types are JSON serializable
                serializable_params = {}
                for k, v in params.items():
                     if isinstance(v, (int, float, str, bool, list, dict)):
                         serializable_params[k] = v
                     elif isinstance(v, np.generic): # Handle numpy types
                          serializable_params[k] = v.item()
                     else:
                          self.logger.warning(f"Skipping non-serializable strategy param {k} of type {type(v)}")

                json.dump(serializable_params, f, indent=4)
            self.logger.info(f"Saved strategy params to {params_path}")
        except Exception as e:
            self.logger.error(f"Failed to save strategy params to {params_path}: {e}")

    def _prepare_data_for_prophet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepares historical data DataFrame for Prophet fitting."""
        df_prophet = df[['close', 'open', 'high', 'low', 'volume']].copy()
        df_prophet.rename(columns={'close': 'y'}, inplace=True)
        df_prophet['ds'] = pd.to_datetime(df_prophet.index)

        # Calculate RSI
        df_prophet['rsi'] = _calculate_rsi(df_prophet['y'], self.params['rsi_period'])

        # Handle NaNs introduced by RSI calculation (backward fill)
        df_prophet.fillna(method='bfill', inplace=True)
        # Still might have NaNs at the start if bfill fails
        df_prophet.fillna(method='ffill', inplace=True)
         # If still NaNs (very short series), fill with a neutral value or drop? Fill with 0 for now.
        df_prophet.fillna(0, inplace=True)


        return df_prophet.reset_index(drop=True) # Prophet prefers simple index

    def _tune_prophet_hyperparameters(self, ticker: str, df_hist: pd.DataFrame) -> Dict[str, Any]:
        """
        Tunes Prophet hyperparameters using hyperopt and forward cross-validation.

        Args:
            ticker (str): The ticker symbol (for logging/saving).
            df_hist (pd.DataFrame): Historical data prepared for Prophet (ds, y, regressors).

        Returns:
            dict: Dictionary containing the best hyperparameters found.
        """
        self.logger.info(f"Starting Prophet hyperparameter tuning for {ticker}...")

        # Use the last N years for cross-validation as specified
        cv_years = self.params.get('lookback_years', 3)
        cv_start_date = df_hist['ds'].max() - pd.DateOffset(years=cv_years)
        df_cv = df_hist[df_hist['ds'] >= cv_start_date].copy()

        if len(df_cv) < 365 * 1.5: # Need ~1.5 years minimum for meaningful CV
             self.logger.warning(f"Insufficient data ({len(df_cv)} days) for {ticker} for reliable Prophet tuning. Returning defaults.")
             return self._load_prophet_params(ticker) # Return default or previously saved


        regressors = []
        if self.params['use_regressors']:
            regressors = [col for col in ['open', 'high', 'low', 'volume', 'rsi'] if col in df_cv.columns]

        def objective(params):
            # Ensure fourier_order is int for Prophet
            params['fourier_order'] = int(params['fourier_order'])
            accuracies = []
            num_folds = 0
            fold_months_step = 1 # How many months to step forward each fold
            train_years = 2 # Years of training data per fold
            test_days = 30 # Days to forecast ahead for validation

            current_fold_start = df_cv['ds'].min()
            max_date = df_cv['ds'].max()
            total_possible_folds = int(((max_date - current_fold_start).days - (train_years * 365)) / (fold_months_step * 30))
            max_folds = min(12, max(1, total_possible_folds)) # Limit to 12 folds max

            for _ in range(max_folds):
                train_end = current_fold_start + pd.DateOffset(years=train_years)
                test_end = train_end + pd.Timedelta(days=test_days)

                if test_end > max_date: break # Stop if test period goes beyond available data

                train_data = df_cv[(df_cv['ds'] >= current_fold_start) & (df_cv['ds'] < train_end)]
                test_data = df_cv[(df_cv['ds'] >= train_end) & (df_cv['ds'] < test_end)]

                if len(train_data) < 252 or len(test_data) < 5: # Need min ~1 year train, 1 week test
                    current_fold_start += pd.DateOffset(months=fold_months_step)
                    continue

                model_fold = Prophet(
                    changepoint_prior_scale=params['changepoint_prior_scale'],
                    seasonality_prior_scale=params['seasonality_prior_scale'],
                    holidays_prior_scale=params['holidays_prior_scale'],
                    seasonality_mode=params['seasonality_mode']
                )
                model_fold.add_seasonality(name='yearly', period=365.25, fourier_order=params['fourier_order'])
                # Add regressors if enabled
                for reg in regressors:
                    model_fold.add_regressor(reg)

                try:
                    model_fold.fit(train_data[['ds', 'y'] + regressors])
                except Exception as e:
                    self.logger.debug(f"Prophet fit failed in CV fold for {ticker}: {e}")
                    current_fold_start += pd.DateOffset(months=fold_months_step)
                    continue # Skip fold if model fails

                # Create future dataframe for the test period
                future_fold = model_fold.make_future_dataframe(periods=len(test_data), freq='D') # Use actual test dates if possible? make_future aims for calendar days
                # Populate future regressors with the last known value from training
                last_train_regressors = train_data.iloc[-1][regressors]
                for reg in regressors:
                     future_fold[reg] = last_train_regressors[reg]

                # Merge actual dates to keep consistent index
                future_fold = future_fold.merge(test_data[['ds'] + regressors], on='ds', how='left')
                 # Fill missing future regressors (e.g., weekends) if needed
                for reg in regressors:
                     future_fold[reg].fillna(method='ffill', inplace=True)
                     future_fold[reg].fillna(method='bfill', inplace=True) # Backfill if needed at start


                # Filter future_fold to only include dates present in test_data
                future_fold = future_fold[future_fold['ds'].isin(test_data['ds'])]


                if future_fold.empty:
                    current_fold_start += pd.DateOffset(months=fold_months_step)
                    continue


                # Predict
                forecast_fold = model_fold.predict(future_fold[['ds'] + regressors])

                # Evaluate directional accuracy
                merged_fold = pd.merge(test_data[['ds', 'y']], forecast_fold[['ds', 'yhat']], on='ds')
                if merged_fold.empty:
                     current_fold_start += pd.DateOffset(months=fold_months_step)
                     continue

                last_train_price = train_data.iloc[-1]['y']
                merged_fold['actual_dir'] = np.sign(merged_fold['y'] - last_train_price)
                merged_fold['pred_dir'] = np.sign(merged_fold['yhat'] - last_train_price)
                # Ignore cases where price didn't change (0 direction)
                merged_fold = merged_fold[merged_fold['actual_dir'] != 0]
                if not merged_fold.empty:
                    fold_accuracy = np.mean(merged_fold['actual_dir'] == merged_fold['pred_dir'])
                    accuracies.append(fold_accuracy)
                num_folds += 1
                current_fold_start += pd.DateOffset(months=fold_months_step)


            if not accuracies:
                self.logger.warning(f"Prophet tuning for {ticker} completed 0 valid folds. Returning high loss.")
                return {'loss': 1.0, 'status': STATUS_OK} # Return high loss if no folds work


            mean_accuracy = np.mean(accuracies)
            loss = 1.0 - mean_accuracy # We want to minimize loss (1 - accuracy)
            self.logger.debug(f"Tuning {ticker}: Params={params}, Folds={num_folds}, Mean Accuracy={mean_accuracy:.4f}, Loss={loss:.4f}")
            return {'loss': loss, 'status': STATUS_OK, 'mean_accuracy': mean_accuracy}

        # Define Hyperopt search space
        search_space = {
            'changepoint_prior_scale': hp.loguniform('changepoint_prior_scale', np.log(0.001), np.log(0.5)),
            'seasonality_prior_scale': hp.loguniform('seasonality_prior_scale', np.log(0.1), np.log(20)), # Adjusted range slightly
            'holidays_prior_scale': hp.loguniform('holidays_prior_scale', np.log(0.1), np.log(20)), # Adjusted range slightly
            'seasonality_mode': hp.choice('seasonality_mode', ['additive', 'multiplicative']),
            'fourier_order': hp.quniform('fourier_order', 3, 15, 1) # Keep fourier order reasonable
        }

        trials = Trials()
        rstate = np.random.default_rng(42) # Use Generator for modern numpy random state

        try:
             best = fmin(
                 fn=objective,
                 space=search_space,
                 algo=tpe.suggest,
                 max_evals=self.params['prophet_tuning_evals'],
                 trials=trials,
                 rstate=rstate,
                 show_progressbar=False # Disable hyperopt progress bar for cleaner logs
             )
        except Exception as e:
             self.logger.error(f"Hyperopt failed during Prophet tuning for {ticker}: {e}")
             return self._load_prophet_params(ticker) # Return default or previously saved on error


        # Convert best parameters from hyperopt format
        best_params = space_eval(search_space, best)
        best_params['fourier_order'] = int(best_params['fourier_order']) # Ensure int

        self.logger.info(f"Best Prophet params found for {ticker}: {best_params} (Achieved Loss: {min(trials.losses()) if trials.losses() else 'N/A'})")
        self._save_prophet_params(ticker, best_params) # Save the tuned parameters
        return best_params

    def _train_prophet_model(self, ticker: str, df_train: pd.DataFrame) -> Optional[Prophet]:
        """Trains or loads the final Prophet model for a ticker."""
        model_path = _get_ticker_model_path(ticker)
        prophet_params = self._load_prophet_params(ticker) # Load best/default params

        # In 'train' mode, always retrain and save
        if self.mode == 'train':
            self.logger.info(f"Training final Prophet model for {ticker}...")
        # In 'forecast' mode, try to load first
        elif self.mode == 'forecast':
            if os.path.exists(model_path):
                try:
                    model = joblib.load(model_path)
                    self.logger.info(f"Loaded Prophet model for {ticker} from {model_path}")
                    # Optional: Check if model params match current best params?
                    return model
                except Exception as e:
                    self.logger.warning(f"Failed to load Prophet model for {ticker} from {model_path}: {e}. Retraining...")
            else:
                 self.logger.warning(f"Prophet model for {ticker} not found at {model_path}. Training model for forecast...")
        # Other modes (like backtest, tune_strategy) might just need parameters,
        # but if called, assume training is needed for this specific instance.


        model = Prophet(
            changepoint_prior_scale=prophet_params['changepoint_prior_scale'],
            seasonality_prior_scale=prophet_params['seasonality_prior_scale'],
            holidays_prior_scale=prophet_params['holidays_prior_scale'],
            seasonality_mode=prophet_params['seasonality_mode']
            # interval_width=0.95 # Default, can be changed
        )
        model.add_seasonality(name='yearly', period=365.25, fourier_order=prophet_params['fourier_order'])

        regressors = []
        if self.params['use_regressors']:
            regressors = [col for col in ['open', 'high', 'low', 'volume', 'rsi'] if col in df_train.columns]
            for reg in regressors:
                model.add_regressor(reg)

        try:
            model.fit(df_train[['ds', 'y'] + regressors])
            # Save model only in 'train' mode or if needed for 'forecast'
            if self.mode == 'train' or (self.mode == 'forecast' and not os.path.exists(model_path)):
                 joblib.dump(model, model_path)
                 self.logger.info(f"Saved trained Prophet model for {ticker} to {model_path}")
            return model
        except Exception as e:
            self.logger.error(f"Failed to train Prophet model for {ticker}: {e}")
            return None


    def _compute_slope_ttest(self, x: np.ndarray, y: np.ndarray) -> Tuple[float, float, float, float]:
        """
        Compute linear regression slope and t-statistic on forecast points.

        Args:
            x (np.ndarray): Array of time steps (e.g., horizons).
            y (np.ndarray): Array of corresponding forecast values (yhat).

        Returns:
            Tuple[float, float, float, float]: slope, standard error, t-statistic, p-value.
        """
        if len(x) < 2 or len(y) < 2 or len(x) != len(y):
            return 0.0, 0.0, 0.0, 1.0 # Cannot compute regression

        try:
            # Ensure inputs are float arrays for linregress
            x_float = np.asarray(x, dtype=float)
            y_float = np.asarray(y, dtype=float)

             # Check for constant y values which cause division by zero in correlation/stderr
            if np.all(y_float == y_float[0]):
                 return 0.0, 0.0, 0.0, 1.0 # Slope is zero, t-stat undefined/zero

            slope, intercept, r_value, p_value, std_err = stats.linregress(x_float, y_float)

            # Handle potential NaN/inf std_err
            if std_err == 0 or not np.isfinite(std_err):
                 t_stat = 0.0 # If std_err is zero or invalid, t-stat is undefined (or effectively zero signal)
            else:
                t_stat = slope / std_err

            return slope, std_err, t_stat, p_value
        except ValueError as ve:
             self.logger.warning(f"Linregress failed: {ve}. Inputs: x={x}, y={y}")
             return 0.0, 0.0, 0.0, 1.0


    def _detect_market_regime(self, df: pd.DataFrame) -> bool:
        """
        Detects market regime using HMM on rolling volatility.

        Args:
            df (pd.DataFrame): DataFrame with 'ds' (datetime) and 'y' (price) columns.

        Returns:
            bool: True if the latest regime is identified as high volatility, False otherwise.
        """
        if len(df) < self.params['vol_window'] + 5: # Need enough data for rolling vol + HMM
            self.logger.warning("Insufficient data for HMM regime detection.")
            return False # Default to low volatility if not enough data

        df_sorted = df.sort_values(by='ds').copy()
        df_sorted['return'] = df_sorted['y'].pct_change()
        # Use rolling std dev of returns
        volatility = df_sorted['return'].rolling(window=self.params['vol_window']).std() * np.sqrt(252) # Annualize
        volatility = volatility.dropna() # Drop initial NaNs

        if volatility.empty or len(volatility) < self.params['hmm_states'] * 5: # Check if enough vol data for HMM
             self.logger.warning("Not enough valid volatility data points for HMM.")
             return False


        try:
            # Reshape for HMM input
            reshaped_vol = volatility.values.reshape(-1, 1)

            # Check for constant volatility which HMM might struggle with
            if np.all(reshaped_vol == reshaped_vol[0]):
                 self.logger.warning("Volatility is constant, HMM may not be meaningful. Defaulting to low vol.")
                 return False


            model = GaussianHMM(n_components=self.params['hmm_states'], covariance_type="diag", n_iter=1000, random_state=42)
            model.fit(reshaped_vol)

            # Check for convergence
            if not model.monitor_.converged:
                 self.logger.warning("HMM did not converge. Regime detection might be unreliable.")


            hidden_states = model.predict(reshaped_vol)

            # Identify high volatility state by mean
            state_means = model.means_[:, 0] # Means of the volatility for each state
            high_vol_state_index = np.argmax(state_means)

            # Check the latest state
            current_state = hidden_states[-1]
            is_high_vol = (current_state == high_vol_state_index)
            self.logger.debug(f"HMM Regime Detection: States={model.n_components}, Means={state_means}, HighVolState={high_vol_state_index}, CurrentState={current_state}, IsHighVol={is_high_vol}")
            return is_high_vol

        except Exception as e:
            self.logger.warning(f"HMM Regime detection failed: {e}")
            return False # Default to low volatility on error

    def _generate_signal_for_date(self,
                                  ticker: str,
                                  current_date: pd.Timestamp,
                                  df_hist: pd.DataFrame,
                                  model: Optional[Prophet] = None) -> Dict[str, Any]:
        """
        Generates the signal and related metrics for a single ticker on a specific date.

        Args:
            ticker (str): Ticker symbol.
            current_date (pd.Timestamp): The date for which to generate the signal (uses data up to this date).
            df_hist (pd.DataFrame): Prepared historical data (ds, y, regressors) up to current_date.
            model (Prophet, optional): A pre-trained Prophet model. If None, attempts to train/load.

        Returns:
            dict: Dictionary containing signal info for current_date (signal, strength, t_stat, etc.).
                  Returns an empty dict if signal generation fails.
        """
        if df_hist.empty or len(df_hist) < 30: # Need minimum data
            self.logger.warning(f"Insufficient historical data for {ticker} on {current_date}")
            return {}

        # Ensure model is available
        if model is None:
            model = self._train_prophet_model(ticker, df_hist)
            if model is None:
                self.logger.error(f"Could not obtain Prophet model for {ticker} on {current_date}")
                return {}

        # Detect regime using data up to current date
        high_vol_regime = False
        if self.params['regime_override']:
            high_vol_regime = self._detect_market_regime(df_hist)

        # Prepare future dataframe for forecasting
        horizon_long = self.params['horizon_long']
        future = model.make_future_dataframe(periods=horizon_long, freq='D') # 'D' for daily

        # Populate future regressors
        regressors = []
        if self.params['use_regressors'] and hasattr(model, 'regressors') and model.regressors:
             regressors = list(model.regressors.keys())
             last_known_regressors = df_hist.iloc[-1][regressors]
             for reg in regressors:
                  if reg in future.columns:
                       # Fill future dates based on last known value
                       future.loc[future['ds'] > df_hist['ds'].max(), reg] = last_known_regressors[reg]
                  else:
                      # If regressor wasn't automatically added by make_future, add it
                       future[reg] = last_known_regressors[reg]

             # Forward fill any gaps (e.g. weekends if needed, though Prophet handles this)
             future[regressors] = future[regressors].ffill()

        # Predict
        try:
            forecast = model.predict(future)
        except Exception as e:
            self.logger.error(f"Prophet prediction failed for {ticker} on {current_date}: {e}")
            return {}

        # --- Extract Forecasts and Apply Logic ---
        current_price_info = df_hist.iloc[-1]
        current_price = current_price_info['y']

        # Find forecast rows closest to target horizons AFTER the current date
        forecast_after_current = forecast[forecast['ds'] > current_price_info['ds']].copy()
        if forecast_after_current.empty:
             self.logger.warning(f"No future forecast points generated for {ticker} on {current_date}")
             return {}


        def get_forecast_row(target_days):
            target_date = current_price_info['ds'] + pd.Timedelta(days=target_days)
            # Find the row with the minimum absolute difference in dates
            return forecast_after_current.iloc[(forecast_after_current['ds'] - target_date).abs().argsort()[:1]]


        row_short_df = get_forecast_row(self.params['horizon_short'])
        row_mid_df = get_forecast_row(self.params['horizon_mid'])
        row_long_df = get_forecast_row(self.params['horizon_long'])

        if row_short_df.empty or row_mid_df.empty or row_long_df.empty:
             self.logger.warning(f"Could not find forecast points for all horizons for {ticker} on {current_date}")
             return {}

        # Extract values from the single-row DataFrames
        row_short = row_short_df.iloc[0]
        row_mid = row_mid_df.iloc[0]
        row_long = row_long_df.iloc[0]


        f_short, f_short_lower, f_short_upper = row_short['yhat'], row_short['yhat_lower'], row_short['yhat_upper']
        f_mid, f_mid_lower, f_mid_upper = row_mid['yhat'], row_mid['yhat_lower'], row_mid['yhat_upper']
        f_long, f_long_lower, f_long_upper = row_long['yhat'], row_long['yhat_lower'], row_long['yhat_upper']

        # Compute slope t-test
        x_points = np.array([self.params['horizon_short'], self.params['horizon_mid'], self.params['horizon_long']])
        y_points = np.array([f_short, f_mid, f_long])
        slope, slope_std, t_stat, p_val = self._compute_slope_ttest(x_points, y_points)

        # Determine signal
        signal = 0
        if (f_short_lower > current_price and
            f_long_upper > f_mid_lower and # Check if CI trend is up
            t_stat > self.params['t_stat_threshold_long']):
            signal = 1
        elif (f_short_upper < current_price and
              f_long_lower < f_mid_upper and # Check if CI trend is down
              t_stat < self.params['t_stat_threshold_short']):
            signal = -1

        # Calculate signal strength
        signal_strength = 0.0
        if signal == 1 and current_price > 1e-6: # Avoid division by zero
            strength_factor = t_stat / self.params['t_stat_threshold_long'] if self.params['t_stat_threshold_long'] > 1e-6 else 1.0
            signal_strength = ((f_long - current_price) / current_price) * max(0, strength_factor) # Ensure non-negative scaling
        elif signal == -1 and current_price > 1e-6:
            strength_factor = abs(t_stat / self.params['t_stat_threshold_short']) if abs(self.params['t_stat_threshold_short']) > 1e-6 else 1.0
            signal_strength = ((current_price - f_long) / current_price) * max(0, strength_factor) # Ensure non-negative scaling


        # Apply regime override
        if high_vol_regime and self.params['regime_override']:
            signal = 0
            signal_strength = 0.0
            self.logger.debug(f"Signal for {ticker} on {current_date} overridden by high volatility regime.")


        # Return results for this date
        result = {
            'ticker': ticker,
            'date': current_date, # Use the actual date of signal generation
            'open': current_price_info.get('open', np.nan),
            'high': current_price_info.get('high', np.nan),
            'low': current_price_info.get('low', np.nan),
            'close': current_price, # Close price on the signal day
            'volume': current_price_info.get('volume', np.nan),
            'forecast_short': f_short,
            'forecast_mid': f_mid,
            'forecast_long': f_long,
            'signal': signal,
            'signal_strength': signal_strength,
            't_stat': t_stat,
            'p_val': p_val,
            'high_vol_regime': high_vol_regime,
            # Include CI bounds for potential analysis
            'f_short_lower': f_short_lower, 'f_short_upper': f_short_upper,
            'f_mid_lower': f_mid_lower, 'f_mid_upper': f_mid_upper,
            'f_long_lower': f_long_lower, 'f_long_upper': f_long_upper,
        }
        return result

    def _run_backtest(self, tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """Runs a rolling backtest simulation."""
        self.logger.info(f"Starting backtest from {start_date} to {end_date} for {len(tickers)} tickers...")

        # Fetch all potentially needed data upfront (with buffer)
        buffer_days = int(self.params['lookback_years'] * 365.25 + 90) # Lookback + extra buffer
        overall_start_date = (pd.to_datetime(start_date) - pd.Timedelta(days=buffer_days)).strftime('%Y-%m-%d')
        all_data = self.get_historical_prices(tickers, from_date=overall_start_date, to_date=end_date)

        if all_data.empty:
            self.logger.error("No historical data found for the specified tickers and date range.")
            return pd.DataFrame()

        results_list = []
        backtest_dates = pd.date_range(start=start_date, end=end_date, freq='B') # Business days

        # Determine retraining dates based on frequency
        retrain_freq = self.params.get('retrain_frequency')
        retrain_dates = set()
        if retrain_freq and retrain_freq != 'None':
            # Generate dates based on frequency within the backtest range
            retrain_schedule = pd.date_range(start=start_date, end=end_date, freq=f'{retrain_freq}S') # Start of period
            retrain_dates = set(retrain_schedule.normalize())


        # Dictionary to hold the current model for each ticker
        ticker_models: Dict[str, Optional[Prophet]] = {ticker: None for ticker in tickers}
        # Dictionary to hold the last signal generation date for each ticker to avoid redundant calculations
        last_signal_date: Dict[str, Optional[pd.Timestamp]] = {ticker: None for ticker in tickers}


        # Prepare data structure for parallel processing
        tasks = []
        for date in backtest_dates:
             for ticker in tickers:
                 # Check if data exists for this ticker/date combination
                 if isinstance(all_data.index, pd.MultiIndex):
                     if (ticker, date) not in all_data.index:
                          continue # Skip if no price data for this business day
                 elif date not in all_data.index: # Single ticker case
                     continue


                 # Determine if retraining is needed
                 needs_retrain = False
                 if retrain_freq and date.normalize() in retrain_dates:
                      needs_retrain = True


                 # Prepare task data
                 tasks.append({
                     'ticker': ticker,
                     'date': date,
                     'needs_retrain': needs_retrain
                 })


        # Function to process a single task (ticker-date)
        def process_task(task_info):
            ticker = task_info['ticker']
            current_date = task_info['date']
            needs_retrain = task_info['needs_retrain']

            # Avoid reprocessing if signal already generated for this date (can happen with 'B' freq)
            if last_signal_date.get(ticker) == current_date:
                 return None


            # Get historical data up to *before* current_date for training/prediction
            hist_end_date = current_date - pd.Timedelta(days=1)
            lookback_start = hist_end_date - pd.DateOffset(years=self.params['lookback_years'])

            if isinstance(all_data.index, pd.MultiIndex):
                 ticker_data = all_data.loc[ticker]
            else:
                 ticker_data = all_data


            df_hist_full = ticker_data[(ticker_data.index >= lookback_start) & (ticker_data.index <= hist_end_date)]


            if df_hist_full.empty or len(df_hist_full) < 252: # Min ~1 year data
                 self.logger.debug(f"Skipping {ticker} on {current_date}: Insufficient historical data ({len(df_hist_full)} days)")
                 return None


            df_prophet_hist = self._prepare_data_for_prophet(df_hist_full)

            current_model = ticker_models.get(ticker)

            # Retrain if needed or if no model exists yet
            if needs_retrain or current_model is None:
                 self.logger.debug(f"Training/Retraining Prophet for {ticker} on {current_date}")
                 current_model = self._train_prophet_model(ticker, df_prophet_hist)
                 ticker_models[ticker] = current_model # Update shared model dictionary
                 if current_model is None:
                      self.logger.warning(f"Training failed for {ticker} on {current_date}, cannot generate signal.")
                      return None


            # Generate signal using the current model and historical data
            signal_result = self._generate_signal_for_date(ticker, current_date, df_prophet_hist, current_model)

            last_signal_date[ticker] = current_date # Mark as processed


            # Add original price data for the signal date for RiskManager input
            if signal_result:
                 try:
                     price_info_signal_date = ticker_data.loc[current_date]
                     signal_result['open'] = price_info_signal_date['open']
                     signal_result['high'] = price_info_signal_date['high']
                     signal_result['low'] = price_info_signal_date['low']
                     signal_result['close'] = price_info_signal_date['close']
                     signal_result['volume'] = price_info_signal_date['volume']
                     return signal_result
                 except KeyError:
                      self.logger.warning(f"Could not find price data for {ticker} on signal date {current_date}. Signal might be inaccurate.")
                      # Fallback using the previous day's close? Or discard? Discard for now.
                      return None
            else:
                 return None


        # Run tasks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
             # Use tqdm for progress bar if installed
             if 'tqdm' in globals():
                 futures = [executor.submit(process_task, task) for task in tasks]
                 results_gen = [future.result() for future in tqdm(concurrent.futures.as_completed(futures), total=len(tasks), desc="Backtesting")]
             else:
                  results_gen = list(executor.map(process_task, tasks)) # Simpler execution without progress


        # Filter out None results and empty dicts
        results_list = [res for res in results_gen if res]

        if not results_list:
            self.logger.warning("Backtest generated no valid signals.")
            return pd.DataFrame()

        # Combine results into a DataFrame
        results_df = pd.DataFrame(results_list)
        results_df['date'] = pd.to_datetime(results_df['date'])
        results_df = results_df.sort_values(by=['ticker', 'date']).set_index(['ticker', 'date'])


        # --- Apply Risk Management ---
        # Apply RM per ticker
        rm_results = []
        for ticker in results_df.index.get_level_values('ticker').unique():
             ticker_signals = results_df.xs(ticker, level='ticker').copy()
             # Ensure required columns for RiskManager exist
             required_cols = ['signal', 'high', 'low', 'close']
             if all(col in ticker_signals.columns for col in required_cols):
                  rm_ticker_df = self.risk_manager.apply(ticker_signals) # Initial position = 0
                  rm_ticker_df['ticker'] = ticker
                  rm_results.append(rm_ticker_df.reset_index().set_index(['ticker', 'date']))
             else:
                  self.logger.warning(f"Skipping Risk Management for {ticker}: Missing required columns {required_cols}. Available: {ticker_signals.columns.tolist()}")
                  # Add ticker column back if RM skipped
                  ticker_signals['ticker'] = ticker
                  rm_results.append(ticker_signals.reset_index().set_index(['ticker', 'date']))


        if not rm_results:
            self.logger.warning("Risk Management could not be applied to any ticker.")
            # Return raw signals if RM failed entirely
            return results_df.reset_index()


        final_df = pd.concat(rm_results).sort_index()

        self.logger.info(f"Backtest completed. Generated {len(final_df)} rows.")
        return final_df.reset_index() # Return with ticker and date as columns

    def _tune_strategy_hyperparameters(self, tickers: List[str], start_date: str, end_date: str) -> Dict[str, Any]:
        """Tunes strategy-level hyperparameters using backtesting and hyperopt."""
        self.logger.info(f"Starting strategy hyperparameter tuning for ID '{self.params['strategy_params_id']}'...")

        # Define the search space for strategy parameters
        strategy_space = {
            't_stat_threshold_long': hp.uniform('t_stat_threshold_long', 1.0, 3.0),
            't_stat_threshold_short': hp.uniform('t_stat_threshold_short', -3.0, -1.0),
            'rsi_period': hp.quniform('rsi_period', 7, 28, 1),
            'vol_window': hp.quniform('vol_window', 10, 63, 1), # ~2 weeks to 3 months
            # Optional: Tune RiskManager params
            'stop_loss_pct': hp.uniform('stop_loss_pct', 0.01, 0.15),
            'take_profit_pct': hp.uniform('take_profit_pct', 0.02, 0.30),
            #'trailing_stop_pct': hp.uniform('trailing_stop_pct', 0.0, 0.10), # Uncomment to tune TSL
        }

        # Objective function for strategy tuning
        def strategy_objective(params):
            # Ensure integer types
            params['rsi_period'] = int(params['rsi_period'])
            params['vol_window'] = int(params['vol_window'])

            # Create a temporary instance or update current instance params
            # Creating a new instance avoids side effects if tuning fails
            temp_params = self.params.copy()
            temp_params.update(params)
            temp_params['mode'] = 'backtest' # Ensure backtest mode for evaluation

            # Re-initialize RiskManager with tuned params
            temp_risk_manager = RiskManager(
                 stop_loss_pct=temp_params['stop_loss_pct'],
                 take_profit_pct=temp_params['take_profit_pct'],
                 trailing_stop_pct=temp_params.get('trailing_stop_pct', self.params['trailing_stop_pct']), # Use tuned or original
                 slippage_pct=temp_params['slippage_pct'],
                 transaction_cost_pct=temp_params['transaction_cost_pct']
            )

            # Run backtest with these temporary parameters
            # Need to simulate running the backtest with modified params
            # This is complex as it involves passing params down.
            # A simpler approach: Update self.params temporarily
            original_params = self.params.copy()
            original_rm = self.risk_manager
            self.params.update(params)
            self.risk_manager = temp_risk_manager

            try:
                backtest_df = self._run_backtest(tickers, start_date, end_date)
            except Exception as e:
                 self.logger.error(f"Backtest failed during strategy tuning iteration: {e}")
                 backtest_df = pd.DataFrame() # Penalize failures


            # Restore original params and risk manager
            self.params = original_params
            self.risk_manager = original_rm


            if backtest_df.empty or 'cumulative_return' not in backtest_df.columns:
                 # Penalize heavily if backtest fails or yields no results
                 return {'loss': 10.0, 'status': STATUS_OK} # High loss


            # Calculate objective metric (e.g., negative Sharpe Ratio)
            # Aggregate returns across tickers for overall Sharpe
            # Ensure 'return' column exists from RiskManager
            if 'return' not in backtest_df.columns:
                 self.logger.warning("Backtest result missing 'return' column for Sharpe calculation.")
                 return {'loss': 10.0, 'status': STATUS_OK}


            # Calculate daily portfolio return (simple average across tickers for now)
            portfolio_returns = backtest_df.groupby('date')['return'].mean()


            # Calculate Sharpe Ratio (annualized)
            if portfolio_returns.std() > 1e-9 and not portfolio_returns.empty:
                sharpe_ratio = (portfolio_returns.mean() / portfolio_returns.std()) * np.sqrt(252) # Assuming daily returns
                loss = -sharpe_ratio # Minimize negative Sharpe = Maximize Sharpe
            else:
                loss = 10.0 # Penalize if std dev is zero or no returns


            # Optional: Add penalty for too few trades?
            # num_trades = backtest_df[backtest_df['exit_type'] != 'none'].shape[0]
            # if num_trades < 10: loss += (10 - num_trades) * 0.1


            self.logger.debug(f"Strategy Tuning Iteration: Params={params}, Sharpe={-loss:.4f}, Loss={loss:.4f}")
            # Prevent NaN/inf loss
            if not np.isfinite(loss):
                 loss = 10.0


            return {'loss': loss, 'status': STATUS_OK}


        trials = Trials()
        rstate = np.random.default_rng(43) # Different seed

        try:
            best = fmin(
                fn=strategy_objective,
                space=strategy_space,
                algo=tpe.suggest,
                max_evals=self.params['strategy_tuning_evals'],
                trials=trials,
                rstate=rstate,
                show_progressbar=False
            )
        except Exception as e:
             self.logger.error(f"Hyperopt failed during strategy tuning: {e}")
             return self._load_strategy_params() # Return previously saved or empty


        best_strategy_params = space_eval(strategy_space, best)
        # Ensure int types are correct after space_eval
        best_strategy_params['rsi_period'] = int(best_strategy_params['rsi_period'])
        best_strategy_params['vol_window'] = int(best_strategy_params['vol_window'])

        self.logger.info(f"Best strategy params found: {best_strategy_params} (Achieved Loss: {min(trials.losses()) if trials.losses() else 'N/A'})")

        # Save the best strategy parameters
        self._save_strategy_params(best_strategy_params)
        return best_strategy_params

    # --- Main Public Method ---

    def generate_signals(self,
                         tickers: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0, # Note: RiskManager applies its own initial pos logic
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generates trading signals based on the specified mode.

        Args:
            tickers (Union[str, List[str]]): Ticker symbol(s).
            start_date (str, optional): Start date ('YYYY-MM-DD'). Required for modes
                                         'backtest', 'tune_prophet', 'tune_strategy'.
            end_date (str, optional): End date ('YYYY-MM-DD'). Required for modes
                                       'backtest', 'tune_prophet', 'tune_strategy', 'train'.
                                       Defaults to latest available date if None for relevant modes.
            initial_position (int): Not directly used by Prophet strat, RiskManager handles position logic.
            latest_only (bool): If True and mode is 'backtest' or 'forecast', return only the
                                 most recent signal row(s). Overrides 'forecast' mode's default.

        Returns:
            pd.DataFrame: DataFrame containing signals and other metrics. Structure depends on mode.
                          - 'backtest': Full backtest results with prices, signals, RM outputs.
                          - 'tune_prophet': Empty DataFrame (results saved to files).
                          - 'tune_strategy': Empty DataFrame (results saved to files).
                          - 'train': Empty DataFrame (model saved to file).
                          - 'forecast': DataFrame with signal(s) for the latest date.
        """
        if isinstance(tickers, str):
            tickers = [tickers]

        # --- Mode Handling ---
        mode = self.params['mode']
        self.logger.info(f"Running ProphetMomentumStrategy in '{mode}' mode for tickers: {tickers}")

        # Parameter validation for modes
        if mode in ['backtest', 'tune_prophet', 'tune_strategy'] and (start_date is None or end_date is None):
            raise ValueError(f"Mode '{mode}' requires both start_date and end_date.")
        if mode == 'train' and end_date is None:
             self.logger.warning("Mode 'train' called without end_date. Using latest available data.")
             # Fetch latest date to use as end_date implicitly later

        # --- Mode Execution ---

        if mode == 'tune_prophet':
            # Tune Prophet params for each ticker (or globally) and save them.
            # Fetch data needed for tuning (e.g., last N years up to end_date)
            tune_hist_start = (pd.to_datetime(end_date) - pd.DateOffset(years=self.params['lookback_years'])).strftime('%Y-%m-%d')
            tuning_data = self.get_historical_prices(tickers, from_date=tune_hist_start, to_date=end_date)

            if tuning_data.empty:
                 self.logger.error("No data found for Prophet tuning period.")
                 return pd.DataFrame()

            def tune_single_ticker(ticker):
                if isinstance(tuning_data.index, pd.MultiIndex):
                     try:
                         ticker_data = tuning_data.xs(ticker, level='ticker')
                     except KeyError:
                          self.logger.warning(f"No tuning data for {ticker}.")
                          return None
                else:
                     ticker_data = tuning_data # Single ticker case

                if len(ticker_data) < 365: # Need at least a year
                     self.logger.warning(f"Skipping tuning for {ticker}: Insufficient data ({len(ticker_data)} days).")
                     return None

                df_prophet = self._prepare_data_for_prophet(ticker_data)
                self._tune_prophet_hyperparameters(ticker, df_prophet) # Saves params internally
                return ticker # Indicate completion


            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                 results = list(executor.map(tune_single_ticker, tickers))

            self.logger.info(f"Prophet tuning finished for {len([r for r in results if r])} tickers.")
            return pd.DataFrame() # Tuning mode saves files, returns empty DF

        elif mode == 'train':
            # Train final model using data up to end_date and best params, then save.
            train_start = (pd.to_datetime(end_date) - pd.DateOffset(years=self.params['lookback_years'])).strftime('%Y-%m-%d')
            training_data = self.get_historical_prices(tickers, from_date=train_start, to_date=end_date)

            if training_data.empty:
                 self.logger.error("No data found for training period.")
                 return pd.DataFrame()

            def train_single_ticker(ticker):
                 if isinstance(training_data.index, pd.MultiIndex):
                      try:
                           ticker_data = training_data.xs(ticker, level='ticker')
                      except KeyError:
                           self.logger.warning(f"No training data for {ticker}.")
                           return None
                 else:
                      ticker_data = training_data

                 if len(ticker_data) < 252: # Min ~1 year
                      self.logger.warning(f"Skipping training for {ticker}: Insufficient data ({len(ticker_data)} days).")
                      return None

                 df_prophet = self._prepare_data_for_prophet(ticker_data)
                 self._train_prophet_model(ticker, df_prophet) # Trains and saves model
                 return ticker

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                 results = list(executor.map(train_single_ticker, tickers))

            self.logger.info(f"Prophet training finished for {len([r for r in results if r])} tickers.")
            return pd.DataFrame() # Training mode saves model, returns empty DF


        elif mode == 'tune_strategy':
            # Tune strategy parameters using backtesting as the evaluation.
            best_params = self._tune_strategy_hyperparameters(tickers, start_date, end_date)
            self.logger.info(f"Strategy tuning finished. Best params saved for ID '{self.params['strategy_params_id']}'.")
            # Optionally return best params? For now, empty DF as params are saved.
            return pd.DataFrame()


        elif mode == 'backtest':
            # Run the full rolling backtest simulation.
            results_df = self._run_backtest(tickers, start_date, end_date)
            if latest_only and not results_df.empty:
                 # Get the last row for each ticker
                 latest_df = results_df.loc[results_df.groupby('ticker')['date'].idxmax()]
                 return latest_df
            return results_df


        elif mode == 'forecast':
            # Generate signal for the latest available data point.
            self.logger.info("Generating latest forecast/signal...")
            # Fetch recent data (lookback_years + buffer)
            lookback_days = int(self.params['lookback_years'] * 365.25 + 60) # Need enough for RSI/HMM + Prophet fit
            forecast_data = self.get_historical_prices(tickers, lookback=lookback_days)

            if forecast_data.empty:
                self.logger.error("No recent data found for forecast.")
                return pd.DataFrame()

            latest_signals = []

            def forecast_single_ticker(ticker):
                if isinstance(forecast_data.index, pd.MultiIndex):
                     try:
                          ticker_data = forecast_data.xs(ticker, level='ticker')
                     except KeyError:
                           self.logger.warning(f"No forecast data for {ticker}.")
                           return None
                else:
                     ticker_data = forecast_data

                if len(ticker_data) < 252: # Min ~1 year needed
                     self.logger.warning(f"Skipping forecast for {ticker}: Insufficient recent data ({len(ticker_data)} days).")
                     return None

                # Ensure data is sorted by date
                ticker_data = ticker_data.sort_index()
                current_date = ticker_data.index[-1] # The date of the last data point


                # Prepare data up to the latest date
                df_prophet_hist = self._prepare_data_for_prophet(ticker_data)

                # Load (or train if missing) the model - uses _train_prophet_model logic
                model = self._train_prophet_model(ticker, df_prophet_hist)
                if model is None:
                     self.logger.error(f"Cannot generate forecast for {ticker}: Model unavailable.")
                     return None


                # Generate signal based on the latest data
                # Note: _generate_signal_for_date expects history *up to* the date,
                # but for forecast we use *all* data including the latest point for model state.
                # The function forecasts *from* the latest point.
                signal_result = self._generate_signal_for_date(ticker, current_date, df_prophet_hist, model)


                # Add OHLCV for the current_date from original data
                if signal_result:
                     latest_price_info = ticker_data.loc[current_date]
                     signal_result['open'] = latest_price_info['open']
                     signal_result['high'] = latest_price_info['high']
                     signal_result['low'] = latest_price_info['low']
                     signal_result['close'] = latest_price_info['close']
                     signal_result['volume'] = latest_price_info['volume']
                     return signal_result
                else:
                     return None


            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results_gen = list(executor.map(forecast_single_ticker, tickers))

            latest_signals = [res for res in results_gen if res]

            if not latest_signals:
                 self.logger.warning("Forecast mode generated no valid signals.")
                 return pd.DataFrame()

            forecast_df = pd.DataFrame(latest_signals)
            # No risk management applied in pure forecast mode (it needs a timeseries)
            # We just return the raw signal for the latest point.
            return forecast_df

        else:
            # Should not happen due to initial validation
            raise ValueError(f"Unhandled mode: {mode}")