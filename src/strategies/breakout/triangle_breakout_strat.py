import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List, Union
from src.strategies.base_strat import BaseStrategy
from src.database.config import DatabaseConfig
from src.strategies.risk_management import RiskManager

try:
    from numba import njit

    NUMBA_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    NUMBA_AVAILABLE = False


if NUMBA_AVAILABLE:
    @njit(cache=True, fastmath=True)
    def _linear_regression_numba(x_arr: np.ndarray, y_arr: np.ndarray) -> Tuple[float, float]:
        n = x_arr.shape[0]
        if n == 0:
            return 0.0, 0.0
        if n == 1:
            return 0.0, y_arr[0]

        x_mean = 0.0
        y_mean = 0.0
        for i in range(n):
            x_mean += x_arr[i]
            y_mean += y_arr[i]
        x_mean /= n
        y_mean /= n

        numerator = 0.0
        denominator = 0.0
        for i in range(n):
            dx = x_arr[i] - x_mean
            numerator += dx * (y_arr[i] - y_mean)
            denominator += dx * dx

        slope = numerator / denominator if denominator != 0.0 else 0.0
        intercept = y_mean - slope * x_mean
        return slope, intercept
else:
    def _linear_regression_numba(x_arr: np.ndarray, y_arr: np.ndarray) -> Tuple[float, float]:
        n = x_arr.size
        if n == 0:
            return 0.0, 0.0
        if n == 1:
            return 0.0, float(y_arr[0])

        x_mean = float(np.mean(x_arr))
        y_mean = float(np.mean(y_arr))
        numerator = float(np.dot(x_arr - x_mean, y_arr - y_mean))
        denominator = float(np.dot(x_arr - x_mean, x_arr - x_mean))
        slope = numerator / denominator if denominator != 0.0 else 0.0
        intercept = y_mean - slope * x_mean
        return slope, intercept


class TriangleBreakout(BaseStrategy):
    """
    Triangle Breakout Strategy with Integrated Risk Management.
    (Docstring unchanged for brevity)
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        default_params = {
            'min_points': 5,
            'max_lookback': 60,
            'breakout_threshold': 0.005,
            'volume_confirm': True,
            'min_pattern_size': 0.03,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }
        if params:
            default_params.update(params)

        default_params['min_points'] = int(default_params['min_points'])
        default_params['max_lookback'] = int(default_params['max_lookback'])
        super().__init__(db_config, default_params)

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        if start_date or end_date:
            price_data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
        else:
            price_data = self.get_historical_prices(ticker, lookback=self.params['max_lookback'] + 50)

        if price_data.empty:
            self.logger.warning("No historical price data retrieved for ticker(s): %s.", ticker)
            return pd.DataFrame()

        def process_single_ticker(df_single_ticker: pd.DataFrame) -> pd.DataFrame:
            ticker_name = getattr(df_single_ticker, 'name', 'UnknownTicker')
            required_len = self.params['max_lookback'] + 10
            if len(df_single_ticker) < required_len:
                self.logger.warning(
                    "Insufficient data for ticker %s to generate signals (need %s, have %s).",
                    ticker_name, required_len, len(df_single_ticker)
                )
                return pd.DataFrame()

            df_single_ticker = df_single_ticker.sort_index()
            result = df_single_ticker[['open', 'high', 'low', 'close', 'volume']].copy()

            n_rows = len(result)
            max_lb = self.params['max_lookback']
            min_pts = self.params['min_points']
            break_thresh = self.params['breakout_threshold']
            vol_confirm = self.params['volume_confirm']
            min_patt_size = self.params['min_pattern_size']
            long_only_flag = self.params['long_only']
            x_predict = max_lb - 1

            upper_line_values = np.full(n_rows, np.nan, dtype=np.float64)
            lower_line_values = np.full(n_rows, np.nan, dtype=np.float64)
            triangle_type_values = np.full(n_rows, None, dtype=object)
            in_pattern_values = np.zeros(n_rows, dtype=bool)
            signal_values = np.zeros(n_rows, dtype=np.int8)

            result['avg_volume'] = (
                df_single_ticker['volume']
                .rolling(window=20, min_periods=1)
                .mean()
            )

            high_values = result['high'].to_numpy(dtype=np.float64, copy=False)
            low_values = result['low'].to_numpy(dtype=np.float64, copy=False)
            close_values = result['close'].to_numpy(dtype=np.float64, copy=False)
            volume_values = result['volume'].to_numpy(dtype=np.float64, copy=False)
            avg_volume_values = result['avg_volume'].to_numpy(dtype=np.float64, copy=False)

            local_high_mask = np.zeros(n_rows, dtype=bool)
            local_low_mask = np.zeros(n_rows, dtype=bool)
            if n_rows > 2:
                local_high_mask[1:-1] = (
                    (high_values[1:-1] > high_values[:-2]) &
                    (high_values[1:-1] > high_values[2:])
                )
                local_low_mask[1:-1] = (
                    (low_values[1:-1] < low_values[:-2]) &
                    (low_values[1:-1] < low_values[2:])
                )

            local_high_indices = np.flatnonzero(local_high_mask)
            local_low_indices = np.flatnonzero(local_low_mask)

            for i in range(max_lb, n_rows):
                window_start = i - max_lb

                upper_left = np.searchsorted(local_high_indices, window_start, side='left')
                upper_right = np.searchsorted(local_high_indices, i, side='left')
                window_upper_idx = local_high_indices[upper_left:upper_right]

                lower_left = np.searchsorted(local_low_indices, window_start, side='left')
                lower_right = np.searchsorted(local_low_indices, i, side='left')
                window_lower_idx = local_low_indices[lower_left:lower_right]

                if window_upper_idx.size < min_pts or window_lower_idx.size < min_pts:
                    continue

                upper_rel = (window_upper_idx - window_start).astype(np.float64)
                lower_rel = (window_lower_idx - window_start).astype(np.float64)

                upper_vals = high_values[window_upper_idx]
                lower_vals = low_values[window_lower_idx]

                upper_slope, upper_intercept = _linear_regression_numba(upper_rel, upper_vals)
                lower_slope, lower_intercept = _linear_regression_numba(lower_rel, lower_vals)

                current_upper_line = upper_slope * x_predict + upper_intercept
                current_lower_line = lower_slope * x_predict + lower_intercept

                if not np.isfinite(current_upper_line) or not np.isfinite(current_lower_line):
                    continue

                triangle_type = self._determine_triangle_type(upper_slope, lower_slope)
                if not triangle_type or current_upper_line <= current_lower_line:
                    continue

                pattern_height = current_upper_line - current_lower_line
                current_close = close_values[i]
                if current_close == 0.0:
                    continue

                price_percentage = pattern_height / current_close
                if price_percentage < min_patt_size:
                    continue

                upper_line_values[i] = current_upper_line
                lower_line_values[i] = current_lower_line
                triangle_type_values[i] = triangle_type

                volume_condition = (not vol_confirm) or (volume_values[i] > avg_volume_values[i])
                is_currently_in_pattern = True

                breakout_up = current_close > current_upper_line * (1 + break_thresh)
                breakout_down = current_close < current_lower_line * (1 - break_thresh)

                if breakout_up and volume_condition:
                    signal_values[i] = 1
                    is_currently_in_pattern = False
                elif breakout_down and volume_condition:
                    signal_values[i] = 0 if long_only_flag else -1
                    is_currently_in_pattern = False

                in_pattern_values[i] = is_currently_in_pattern

            result['upper_line'] = upper_line_values
            result['lower_line'] = lower_line_values
            result['triangle_type'] = pd.Series(triangle_type_values, index=result.index, dtype=object)
            result['in_pattern'] = in_pattern_values
            result['signal'] = signal_values.astype(int)

            return result.dropna(subset=['close'])

        signals_list = []
        if isinstance(ticker, list):
            if price_data.index.nlevels > 1:
                groups = price_data.groupby(level=0)
                for ticker_name, group_df in groups:
                    group_df.name = ticker_name
                    res_single = process_single_ticker(group_df.copy())
                    if not res_single.empty:
                        res_single['ticker'] = ticker_name
                        signals_list.append(res_single)
            else:
                self.logger.warning(
                    "Price data for list of tickers was not a MultiIndex. Processing may be incorrect."
                )
                if 'ticker' in price_data.columns and len(price_data['ticker'].unique()) > 1:
                    groups = price_data.groupby('ticker')
                    for ticker_name, group_df in groups:
                        group_df.name = ticker_name
                        res_single = process_single_ticker(group_df.copy())
                        if not res_single.empty:
                            res_single['ticker'] = ticker_name
                            signals_list.append(res_single)
                else:
                    price_data.name = ticker[0] if ticker else "Unknown"
                    res_single = process_single_ticker(price_data.copy())
                    if not res_single.empty:
                        res_single['ticker'] = price_data.name
                        signals_list.append(res_single)

            if not signals_list:
                return pd.DataFrame()
            signals_df = pd.concat(signals_list)
            if isinstance(signals_df.index, pd.MultiIndex):
                idx_level0_name = signals_df.index.names[0] if signals_df.index.names[0] is not None else 'level_0'
                if idx_level0_name == 'ticker' and 'ticker' in signals_df.columns:
                    self.logger.info(
                        "TriangleBreakout: Correcting signals_df with conflicting 'ticker' index and column by dropping column."
                    )
                    signals_df = signals_df.drop(columns=['ticker'])
            elif 'ticker' not in signals_df.columns and not signals_df.empty:
                if len(ticker) == 1:
                    signals_df['ticker'] = ticker[0]
        else:
            price_data.name = ticker
            signals_df = process_single_ticker(price_data.copy())
            if not signals_df.empty:
                signals_df['ticker'] = ticker

        if signals_df.empty:
            self.logger.warning("No signals generated for ticker(s): %s.", ticker)
            return pd.DataFrame()

        if isinstance(signals_df.index, pd.DatetimeIndex):
            signals_df = signals_df.sort_index()
        elif isinstance(signals_df.index, pd.MultiIndex):
            signals_df = signals_df.sort_index(level=[0, 1])

        rm = RiskManager(
            stop_loss_pct=self.params.get("stop_loss_pct", 0.05),
            take_profit_pct=self.params.get("take_profit_pct", 0.10),
            trailing_stop_pct=self.params.get("trailing_stop_pct", 0.0),
            slippage_pct=self.params.get("slippage_pct", 0.001),
            transaction_cost_pct=self.params.get("transaction_cost_pct", 0.001)
        )
        signals_with_rm = rm.apply(signals_df, initial_position=initial_position)

        if latest_only:
            if not signals_with_rm.empty:
                if 'ticker' in signals_with_rm.columns:
                    signals_with_rm = signals_with_rm.groupby('ticker', group_keys=False).tail(1)
                elif (
                    isinstance(signals_with_rm.index, pd.MultiIndex) and
                    signals_with_rm.index.names[0] == 'ticker'
                ):
                    signals_with_rm = signals_with_rm.groupby(level=0, group_keys=False).tail(1)
                else:
                    signals_with_rm = signals_with_rm.tail(1)
            else:
                self.logger.warning("No signals available to select 'latest_only'.")
                return pd.DataFrame()

        return signals_with_rm

    def _get_triangle_points(self, window_data: pd.DataFrame) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
        if len(window_data) < 3:
            return [], []

        relative_indices = np.arange(len(window_data))
        highs = window_data['high'].values
        lows = window_data['low'].values

        local_max_mask = (highs[1:-1] > highs[:-2]) & (highs[1:-1] > highs[2:])
        upper_rel_indices = relative_indices[1:-1][local_max_mask]
        upper_abs_values = highs[1:-1][local_max_mask]
        upper_points = list(zip(upper_rel_indices, upper_abs_values))

        local_min_mask = (lows[1:-1] < lows[:-2]) & (lows[1:-1] < lows[2:])
        lower_rel_indices = relative_indices[1:-1][local_min_mask]
        lower_abs_values = lows[1:-1][local_min_mask]
        lower_points = list(zip(lower_rel_indices, lower_abs_values))

        return upper_points, lower_points

    def _linear_regression(self, points: List[Tuple[int, float]]) -> Tuple[float, float]:
        if not points:
            return 0.0, 0.0

        x_arr = np.fromiter((p[0] for p in points), dtype=np.float64, count=len(points))
        y_arr = np.fromiter((p[1] for p in points), dtype=np.float64, count=len(points))
        return _linear_regression_numba(x_arr, y_arr)

    def _determine_triangle_type(self, upper_slope: float, lower_slope: float) -> Optional[str]:
        flat_tolerance = 0.01
        if upper_slope < -flat_tolerance and lower_slope > flat_tolerance:
            return 'symmetrical'
        if abs(upper_slope) <= flat_tolerance and lower_slope > flat_tolerance:
            return 'ascending'
        if upper_slope < -flat_tolerance and abs(lower_slope) <= flat_tolerance:
            return 'descending'
        return None