# src/strategies/advanced/enhanced_market_pressure_strat.py

""" TODO: implement 'gap_threshold' parameter- A gap threshold would be used to handle price gaps 
between trading sessions. In stock markets, especially those with limited trading hours like the 
Indian markets, prices can "gap" up or down between the close of one session and the open of the next.

This parameter could be implemented to:

Detect significant overnight gaps (when the opening price differs from the previous closing price by more than the threshold)
Adjust strategy behavior in response to gaps (e.g., avoid entering positions after a large gap or use special handling for stop-loss orders)
Filter out false signals that might occur due to gaps
"""

"""strat credit: https://www.jamessawyer.co.uk/market-pressure-analysis-page.html"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional, Union, List, Tuple
from scipy.stats import beta, kstest, norm, ksone
import warnings

try:
    from numpy.lib.stride_tricks import sliding_window_view
    _HAS_SLIDING_WINDOW = True
except ImportError:
    sliding_window_view = None
    _HAS_SLIDING_WINDOW = False

from src.strategies.base_strat import BaseStrategy
from src.database.config import DatabaseConfig
from src.strategies.risk_management import RiskManager

_SAFE_EPS = 1e-9


class EnhancedMarketPressureStrategy(BaseStrategy):
    """
    Enhanced Market Pressure Analysis Strategy.
    
    This strategy transforms ordinary price data into statistical insights by analyzing
    the normalized position of closing prices within their daily ranges. It models these
    positions using probability distributions to reveal underlying market forces that
    conventional indicators may miss.
    
    Key features:
    - Normalized position modeling: (close - low) / (high - low)
    - Statistical distribution fitting (Beta and optionally other distributions)
    - Volume-weighted analysis for higher confidence
    - Divergence detection between price trends and pressure trends
    - Statistical significance testing to validate signals
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Enhanced Market Pressure Analysis Strategy.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (Optional[Dict]): Strategy parameters. Expected keys include:
                - 'window' (default: 20): Rolling window size for analysis
                - 'volume_weighted' (default: True): Whether to weight by volume
                - 'confidence_threshold' (default: 0.95): Statistical confidence threshold
                - 'pressure_threshold' (default: 0.3): Threshold for pressure to generate signal
                - 'long_only' (default: True): Whether to only take long positions
                - 'use_multiple_dists' (default: False): Use multiple distributions
                - 'price_trend_window' (default: 5): Window for price trend calculation
                - 'pressure_trend_window' (default: 5): Window for pressure trend calculation
                - 'stop_loss_pct' (default: 0.05): Stop loss percentage
                - 'take_profit_pct' (default: 0.10): Take profit percentage
                - 'trailing_stop_pct' (default: 0): Fractional distance from the peak (for long) or trough (for short)
                                   that the price is allowed to reverse before the trade is exited.
                - 'slippage_pct' (default: 0.001): Slippage percentage
                - 'transaction_cost_pct' (default: 0.001): Transaction cost percentage
                - 'bull_div_threshold' (default: 0.01): Threshold for bullish divergence
                - 'bear_div_threshold' (default: 0.01): Threshold for bearish divergence
        """
        default_params = {
            'window': 20, 
            'volume_weighted': True,
            'confidence_threshold': 0.95,
            'pressure_threshold': 0.3,
            'long_only': True,
            'use_multiple_dists': False,
            'price_trend_window': 5,
            'pressure_trend_window': 5,
            'bull_div_threshold': 0.01,
            'bear_div_threshold': 0.01,
        }

        params = params or default_params
        super().__init__(db_config, params)
        
        # Initialize strategy parameters
        self.window = int(params.get('window', default_params['window']))
        self.volume_weighted = params.get('volume_weighted', default_params['volume_weighted'])
        self.confidence_threshold = params.get('confidence_threshold', default_params['confidence_threshold'])
        self.pressure_threshold = params.get('pressure_threshold', default_params['pressure_threshold'])
        self.long_only = params.get('long_only', default_params['long_only'])
        self.use_multiple_dists = params.get('use_multiple_dists', default_params['use_multiple_dists'])
        self.price_trend = int(params.get('price_trend_window', default_params['price_trend_window']))
        self.pressure_trend = int(params.get('pressure_trend_window', default_params['pressure_trend_window']))
        self.bull_div_threshold = params.get('bull_div_threshold', default_params['bull_div_threshold'])
        self.bear_div_threshold = params.get('bear_div_threshold', default_params['bear_div_threshold'])
        
        # Initialize logger
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize RiskManager with risk parameters
        risk_params = {
            'stop_loss_pct': params.get('stop_loss_pct', 0.05),
            'take_profit_pct': params.get('take_profit_pct', 0.10),
            'trailing_stop_pct': params.get('trailing_stop_pct', 0),
            'slippage_pct': params.get('slippage_pct', 0.001),
            'transaction_cost_pct': params.get('transaction_cost_pct', 0.001),
        }
        self.risk_manager = RiskManager(**risk_params)

    @staticmethod
    def _rolling_mean(series: pd.Series, window: int, min_periods: Optional[int] = None) -> pd.Series:
        """Attempt to compute rolling mean with Numba acceleration; fallback to default."""
        rolling_kwargs = {'window': window, 'min_periods': min_periods}
        try:
            return series.rolling(engine='numba', **rolling_kwargs).mean()
        except Exception:
            return series.rolling(**rolling_kwargs).mean()

    @staticmethod
    def _rolling_std(series: pd.Series, window: int, min_periods: Optional[int] = None) -> pd.Series:
        """Attempt to compute rolling std with Numba acceleration; fallback to default."""
        rolling_kwargs = {'window': window, 'min_periods': min_periods}
        try:
            return series.rolling(engine='numba', **rolling_kwargs).std()
        except Exception:
            return series.rolling(**rolling_kwargs).std()
    
    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False,
    ) -> pd.DataFrame:
        """
        Generate trading signals based on the Enhanced Market Pressure Analysis.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (default=0).
            latest_only (bool): If True, returns only the final row (per ticker for multi-ticker scenarios).
            
        Returns:
            pd.DataFrame: DataFrame containing signals and performance metrics.
        """
        try:
            lookback_buffer = 2 * self.window
            
            if start_date and end_date:
                data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date, lookback=lookback_buffer)
            else:
                data = self.get_historical_prices(ticker, lookback=lookback_buffer)
                data = data.sort_index()
            
            if isinstance(ticker, list):
                signals_list = []
                for t, group in data.groupby(level=0):
                    if not self._validate_data(group, min_records=self.window):
                        self.logger.warning(f"Insufficient data for {t}: required at least {self.window} records.")
                        continue
                    
                    sig = self._calculate_signals_single(group)
                    sig = self.risk_manager.apply(sig, initial_position)
                    
                    sig['daily_return'] = sig['close'].pct_change().fillna(0)
                    sig['strategy_return'] = sig['daily_return'] * sig['position'].shift(1).fillna(0)
                    
                    sig.rename(columns={
                        'return': 'rm_strategy_return',
                        'cumulative_return': 'rm_cumulative_return',
                        'exit_type': 'rm_action'
                    }, inplace=True)
                    
                    sig['ticker'] = t
                    signals_list.append(sig)
                
                if not signals_list:
                    return pd.DataFrame()
                    
                signals = pd.concat(signals_list)
                
                if latest_only:
                    signals = signals.groupby('ticker').tail(1)
            else:
                if not self._validate_data(data, min_records=self.window):
                    self.logger.warning(f"Insufficient data for {ticker}: required at least {self.window} records.")
                    return pd.DataFrame()
                
                signals = self._calculate_signals_single(data)
                signals = self.risk_manager.apply(signals, initial_position)
                
                signals['daily_return'] = signals['close'].pct_change().fillna(0)
                signals['strategy_return'] = signals['daily_return'] * signals['position'].shift(1).fillna(0)
                
                signals.rename(columns={
                    'return': 'rm_strategy_return',
                    'cumulative_return': 'rm_cumulative_return',
                    'exit_type': 'rm_action'
                }, inplace=True)
                
                if latest_only:
                    signals = signals.iloc[[-1]].copy()
            
            return signals
            
        except Exception as e:
            self.logger.error(f"Error generating signals for {ticker}: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_signals_single(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the Enhanced Market Pressure Analysis and corresponding trading signals for a single ticker.
        
        This method:
        1. Calculates normalized positions of closes within their ranges
        2. Applies statistical distribution modeling to these positions
        3. Computes buying and selling pressure metrics
        4. Detects divergences between price trends and pressure trends
        5. Generates trading signals based on pressure and divergences
        """
        df = data.sort_index().copy()
        window = self.window
        if df.empty:
            return df
        
        high = df['high'].to_numpy(copy=False)
        low = df['low'].to_numpy(copy=False)
        close = df['close'].to_numpy(copy=False)
        price_range = high - low
        df['range'] = price_range
        norm_pos = np.full_like(price_range, 0.5, dtype=np.float64)
        valid_range = price_range > 0
        norm_pos[valid_range] = (close[valid_range] - low[valid_range]) / price_range[valid_range]
        df['norm_pos'] = np.clip(norm_pos, 0.0, 1.0)
        
        volatility = self._rolling_std(df['range'], window=window, min_periods=window).fillna(0.0)
        df['volatility'] = volatility
        vol_mean = self._rolling_mean(volatility, window=window, min_periods=window)
        vol_mean = vol_mean.where(vol_mean.abs() > _SAFE_EPS, 1.0)
        df['vol_ratio'] = (df['volatility'] / vol_mean).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        df['vap'] = df['norm_pos'] * (1 + df['vol_ratio'])
        
        norm_mean = self._rolling_mean(df['norm_pos'], window=window, min_periods=window)
        norm_std = self._rolling_std(df['norm_pos'], window=window, min_periods=window)
        safe_norm_std = norm_std.where(norm_std.abs() > _SAFE_EPS, 1.0)
        df['z_pos'] = ((df['norm_pos'] - norm_mean) / safe_norm_std).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        
        df['buying_pressure'] = np.nan
        df['selling_pressure'] = np.nan
        df['market_pressure'] = np.nan
        df['pressure_significance'] = np.nan
        
        norm_positions = df['norm_pos'].to_numpy(dtype=np.float64, copy=False)
        volumes = None
        if self.volume_weighted and 'volume' in df.columns:
            volumes = df['volume'].to_numpy(dtype=np.float64, copy=False)
            volumes = np.nan_to_num(volumes, nan=0.0, posinf=0.0, neginf=0.0)
        
        vectorized_success = False
        if _HAS_SLIDING_WINDOW and len(df) > window:
            try:
                buy_arr, sell_arr, market_arr, sig_arr = self._compute_pressure_metrics_vectorized(
                    norm_positions,
                    volumes
                )
                idx_slice = df.index[window:]
                df.loc[idx_slice, 'buying_pressure'] = buy_arr
                df.loc[idx_slice, 'selling_pressure'] = sell_arr
                df.loc[idx_slice, 'market_pressure'] = market_arr
                df.loc[idx_slice, 'pressure_significance'] = sig_arr
                vectorized_success = True
            except Exception as err:
                self.logger.debug(f"Vectorized pressure computation failed, falling back to loop: {err}")
        
        if not vectorized_success:
            self._calculate_pressure_fallback(
                df,
                norm_positions,
                volumes
            )
        
        df['price_trend'] = df['close'].pct_change(5).rolling(window=self.price_trend).mean().fillna(0)
        df['pressure_trend'] = df['market_pressure'].diff(5).rolling(window=self.pressure_trend).mean().fillna(0)
        
        df['divergence'] = 0
        bull_div = (
            (df['price_trend'] < (-1 * self.bull_div_threshold)) &
            (df['pressure_trend'] > self.bull_div_threshold) &
            (df['pressure_significance'] > self.confidence_threshold * 0.8)
        )
        bear_div = (
            (df['price_trend'] > self.bear_div_threshold) &
            (df['pressure_trend'] < (-1 * self.bear_div_threshold)) &
            (df['pressure_significance'] > self.confidence_threshold * 0.8)
        )
        df.loc[bull_div, 'divergence'] = 1
        df.loc[bear_div, 'divergence'] = -1
        
        df['signal'] = 0
        buy_conditions = (
            (df['market_pressure'] > self.pressure_threshold) & 
            (df['pressure_significance'] > self.confidence_threshold) &
            (df['market_pressure'].shift(1) <= self.pressure_threshold)
        ) | (
            (df['divergence'] == 1) & 
            (df['pressure_significance'] > self.confidence_threshold * 0.9)
        )
        
        sell_conditions = (
            (df['market_pressure'] < -self.pressure_threshold) & 
            (df['pressure_significance'] > self.confidence_threshold) &
            (df['market_pressure'].shift(1) >= -self.pressure_threshold)
        ) | (
            (df['divergence'] == -1) & 
            (df['pressure_significance'] > self.confidence_threshold * 0.9)
        )
        
        df.loc[buy_conditions, 'signal'] = 1
        df.loc[sell_conditions, 'signal'] = -1
        
        if self.long_only:
            df.loc[df['signal'] == -1, 'signal'] = 0
        
        df['signal_strength'] = 0.0
        signal_mask = buy_conditions | sell_conditions
        df.loc[signal_mask, 'signal_strength'] = (
            df.loc[signal_mask, 'pressure_significance'] * 
            df.loc[signal_mask, 'market_pressure'].abs()
        )
        
        return df[['open', 'close', 'high', 'low', 'norm_pos', 'market_pressure', 
                   'pressure_significance', 'divergence', 'signal', 'signal_strength']]

    def _compute_pressure_metrics_vectorized(
        self,
        norm_positions: np.ndarray,
        volumes: Optional[np.ndarray]
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Compute buying/selling pressure and significance for each rolling window using vectorized operations.
        """
        window = self.window
        n = norm_positions.shape[0]
        if n <= window:
            return (
                np.empty(0, dtype=np.float64),
                np.empty(0, dtype=np.float64),
                np.empty(0, dtype=np.float64),
                np.empty(0, dtype=np.float64)
            )
        
        if not _HAS_SLIDING_WINDOW:
            raise RuntimeError("NumPy sliding_window_view is unavailable.")
        
        position_windows = sliding_window_view(norm_positions, window)
        if position_windows.shape[0] == 0:
            return (
                np.empty(0, dtype=np.float64),
                np.empty(0, dtype=np.float64),
                np.empty(0, dtype=np.float64),
                np.empty(0, dtype=np.float64)
            )
        
        # Drop the final window to match legacy behavior (len - window results)
        position_windows = position_windows[:-1]
        beta_positions = np.clip(position_windows, 1e-6, 1 - 1e-6)
        
        if volumes is not None:
            volume_windows = sliding_window_view(volumes, window)[:-1]
            weight_sums = volume_windows.sum(axis=1, keepdims=True)
            normalized_weights = np.divide(
                volume_windows,
                weight_sums,
                out=np.full_like(volume_windows, 1.0 / window, dtype=np.float64),
                where=weight_sums > 0
            )
        else:
            normalized_weights = np.full_like(beta_positions, 1.0 / window, dtype=np.float64)
        
        if volumes is None:
            means = beta_positions.mean(axis=1)
            centered = beta_positions - means[:, None]
            variances = np.mean(centered * centered, axis=1)
        else:
            means = np.sum(normalized_weights * beta_positions, axis=1)
            centered = beta_positions - means[:, None]
            variances = np.sum(normalized_weights * centered * centered, axis=1)
        
        variances = np.maximum(variances, 1e-9)
        factor = (means * (1 - means)) / variances
        valid_mask = factor > 1.0 + 1e-9
        
        alpha = np.empty_like(means)
        beta_param = np.empty_like(means)
        alpha[valid_mask] = means[valid_mask] * (factor[valid_mask] - 1)
        beta_param[valid_mask] = (1 - means[valid_mask]) * (factor[valid_mask] - 1)
        alpha[valid_mask] = np.maximum(alpha[valid_mask], 0.01)
        beta_param[valid_mask] = np.maximum(beta_param[valid_mask], 0.01)
        
        selling = np.empty_like(means)
        buying = np.empty_like(means)
        market = np.empty_like(means)
        
        # Fallback for invalid beta parameters
        fallback_mask = ~valid_mask
        selling[fallback_mask] = 1 - means[fallback_mask]
        buying[fallback_mask] = means[fallback_mask]
        market[fallback_mask] = buying[fallback_mask] - selling[fallback_mask]
        
        if valid_mask.any():
            selling_valid = beta.cdf(0.5, alpha[valid_mask], beta_param[valid_mask])
            buying_valid = 1.0 - selling_valid
            selling[valid_mask] = selling_valid
            buying[valid_mask] = buying_valid
            market[valid_mask] = buying_valid - selling_valid
        
        significance = self._ks_significance(position_windows)
        
        return buying, selling, market, significance

    def _ks_significance(self, windows: np.ndarray) -> np.ndarray:
        """
        Calculate KS-test based significance against Uniform[0,1] for each window using vectorized operations.
        """
        num_windows, window = windows.shape
        if window < 2 or num_windows == 0:
            return np.zeros(num_windows, dtype=np.float64)
        
        clipped = np.clip(windows, 0.0, 1.0)
        sorted_vals = np.sort(clipped, axis=1)
        idx = np.arange(1, window + 1, dtype=np.float64)
        
        d_plus = idx / window - sorted_vals
        d_minus = sorted_vals - (idx - 1) / window
        d_stat = np.maximum(d_plus.max(axis=1), d_minus.max(axis=1))
        
        try:
            p_values = ksone.sf(d_stat, window)
        except Exception:
            # Fallback to scalar kstest loop if ksone is unavailable
            p_values = np.empty(num_windows, dtype=np.float64)
            for i in range(num_windows):
                _, p_value = kstest(sorted_vals[i], 'uniform')
                p_values[i] = p_value
        
        p_values = np.clip(p_values, 0.0, 1.0)
        return 1.0 - p_values
    
    def _calculate_pressure_fallback(
        self,
        df: pd.DataFrame,
        norm_positions: np.ndarray,
        volumes: Optional[np.ndarray]
    ) -> None:
        """
        Fallback loop-based computation when the vectorized path is unavailable.
        """
        buying_pressure = []
        selling_pressure = []
        market_pressure = []
        pressure_significance = []
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for i in range(self.window, len(df)):
                start = i - self.window
                end = i
                window_positions = norm_positions[start:end]
                
                weights = None
                if volumes is not None:
                    window_weights = volumes[start:end]
                    weight_sum = np.sum(window_weights)
                    if weight_sum > 0:
                        weights = window_weights / weight_sum
                
                try:
                    buy_p, sell_p, pressure, sig = self._fit_beta_pressure(window_positions, weights)
                except Exception:
                    buy_p = sell_p = pressure = sig = np.nan
                
                buying_pressure.append(buy_p)
                selling_pressure.append(sell_p)
                market_pressure.append(pressure)
                pressure_significance.append(sig)
        
        if buying_pressure:
            idx_slice = df.index[self.window:]
            df.loc[idx_slice, 'buying_pressure'] = buying_pressure
            df.loc[idx_slice, 'selling_pressure'] = selling_pressure
            df.loc[idx_slice, 'market_pressure'] = market_pressure
            df.loc[idx_slice, 'pressure_significance'] = pressure_significance

    def _fit_beta_pressure(self, positions, weights=None):
        """
        Fit a Beta distribution to positions and calculate pressure metrics.
        The significance test is now against a Uniform[0,1] distribution
        to determine if the observed positions are non-random.
        """
        beta_positions = np.clip(positions, 1e-6, 1 - 1e-6)
        ks_positions = np.clip(positions, 0, 1)

        if weights is None or len(positions) == 0:
            if len(positions) == 0:
                return 0.5, 0.5, 0.0, 0.0
            weights = np.ones_like(beta_positions) / len(beta_positions)
        elif np.sum(weights) == 0:
            weights = np.ones_like(beta_positions) / len(beta_positions)

        mean = np.sum(weights * beta_positions)
        var = np.sum(weights * (beta_positions - mean)**2)
        var = max(var, 1e-9)

        factor = (mean * (1 - mean) / var)
        if factor <= 1:
            simple_buying_pressure = mean
            simple_selling_pressure = 1 - mean
            simple_market_pressure = simple_buying_pressure - simple_selling_pressure
            
            try:
                _stat, p_value = kstest(ks_positions, 'uniform')
                significance = 1 - p_value
            except Exception:
                if len(ks_positions) > 1:
                    std_dev_positions = np.std(ks_positions)
                    significance = 1 - np.min([1.0, std_dev_positions / 0.288675])
                else:
                    significance = 0.0
            return simple_buying_pressure, simple_selling_pressure, simple_market_pressure, significance

        alpha = mean * (factor - 1)
        beta_param = (1 - mean) * (factor - 1)
        alpha = max(alpha, 0.01)
        beta_param = max(beta_param, 0.01)

        buying_pressure = 1 - beta.cdf(0.5, alpha, beta_param)
        selling_pressure = beta.cdf(0.5, alpha, beta_param)
        market_pressure = buying_pressure - selling_pressure

        try:
            _stat, p_value = kstest(ks_positions, 'uniform')
            significance = 1 - p_value
        except Exception:
            if len(ks_positions) > 1:
                std_dev_positions = np.std(ks_positions)
                significance = 1 - np.min([1.0, std_dev_positions / 0.288675 if 0.288675 > 1e-9 else 1.0])
            else:
                significance = 0.0

        return buying_pressure, selling_pressure, market_pressure, significance
    
    def _fit_multiple_distributions(self, positions, weights=None):
        """
        Fit multiple distributions and select the best one based on fit quality.
        """
        try:
            beta_result = self._fit_beta_pressure(positions, weights)
            
            transformed = -np.log(1/np.clip(positions, 1e-6, 1-1e-6) - 1)
            
            if weights is None:
                weights = np.ones_like(positions) / len(positions)
            
            mean = np.sum(weights * transformed)
            var = np.sum(weights * (transformed - mean)**2)
            std = np.sqrt(max(var, 1e-9))
            
            norm_buying_pressure = 1 - norm.cdf(0, mean, std)
            norm_selling_pressure = norm.cdf(0, mean, std)
            norm_market_pressure = norm_buying_pressure - norm_selling_pressure
            
            try:
                ks_stat, p_value = kstest(transformed, 'norm', args=(mean, std))
                norm_significance = 1 - p_value
            except Exception:
                norm_significance = 0.5
            
            if norm_significance > beta_result[3]:
                return norm_buying_pressure, norm_selling_pressure, norm_market_pressure, norm_significance
            else:
                return beta_result
        except Exception:
            return None