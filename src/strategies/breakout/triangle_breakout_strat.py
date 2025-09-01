# trading_system/src/strategies/triangle_breakout.py

from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class TriangleBreakout(BaseStrategy):
    """
    Triangle Breakout Strategy with Integrated Risk Management.

    This strategy detects consolidation patterns (triangles) in a stock's price action
    by fitting two converging trend lines through local highs and lows within a rolling window.

    Mathematical Formulation:
      Consider a rolling window of 'max_lookback' bars. Within this window, identify local
      highs (peaks) and local lows (troughs). Apply linear regression to the set of highs and
      lows separately to obtain the equations of the upper and lower trend lines:

          Upper Line:   y = m_upper * x + c_upper
          Lower Line:   y = m_lower * x + c_lower

      where x is the relative index in the window (0 to max_lookback-1). The triangle pattern
      is defined by these two lines. Its significance is measured by the "pattern height" (the
      difference between the two lines at x = max_lookback-1) as a fraction of the current price.

      A breakout is signaled when:
          - Upward Breakout: current_close > upper_line * (1 + breakout_threshold)
          - Downward Breakout: current_close < lower_line * (1 - breakout_threshold)

      Optionally, the breakout is confirmed only if current volume exceeds a 20-day rolling average.

    Operating Modes:
      - Backtesting: When start_date and end_date are provided, signals are generated over the date range.
      - Forecasting: With latest_only=True, only the latest signal for each ticker is returned.
      - Multi-ticker: Accepts a list of tickers and processes each.

    Risk Management:
      After signal generation, the RiskManager module applies stop loss, take profit, slippage,
      and transaction cost adjustments. It then computes the realized return when an exit occurs
      and tracks the cumulative return.

    Strategy-specific Parameters (with defaults):
      - min_points (int): Minimum number of local extremes required to form a valid triangle (5).
      - max_lookback (int): Rolling window size in bars for triangle pattern detection (60).
      - breakout_threshold (float): Percentage buffer to confirm a breakout (0.005).
      - volume_confirm (bool): Use volume confirmation on breakout (True).
      - min_pattern_size (float): Minimum triangle height relative to price (0.03).
      - stop_loss_pct (float): Stop loss percentage (0.05).
      - take_profit_pct (float): Take profit percentage (0.10).
      - trailing_stop_pct (float): Trailing stop percentage (0.0).
      - slippage_pct (float): Estimated slippage percentage (0.001).
      - transaction_cost_pct (float): Estimated transaction cost percentage (0.001).
      - long_only (bool): If True, only long positions are allowed (True).
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Triangle Breakout strategy with the given parameters.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Dictionary of strategy-specific parameters.
        """
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

    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals based on triangle breakout patterns and apply risk management.

        Retrieves historical price data, scans for triangle patterns using a
        rolling window, and generates signals. RiskManager then applies adjustments.

        Args:
            ticker (str or List[str]): Single or multiple stock ticker symbols.
            start_date (str, optional): Start date (YYYY-MM-DD) for backtesting.
            end_date (str, optional): End date (YYYY-MM-DD) for backtesting.
            initial_position (int): Starting position (0 for no position, 1 for long, -1 for short).
            latest_only (bool): If True, returns only the most recent signal row(s).

        Returns:
            pd.DataFrame: DataFrame with price data, pattern info, signals, and risk management output.
        """
        if start_date or end_date:
            price_data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
        else:
            # Default lookback sufficient for max_lookback + avg_volume window + some buffer
            price_data = self.get_historical_prices(ticker, lookback=self.params['max_lookback'] + 50)

        if price_data.empty:
            self.logger.warning("No historical price data retrieved for ticker(s): %s.", ticker)
            return pd.DataFrame()

        # process_single_ticker is defined as a nested function to capture `self` (and its params) easily
        def process_single_ticker(df_single_ticker: pd.DataFrame) -> pd.DataFrame:
            ticker_name = df_single_ticker.name if hasattr(df_single_ticker, 'name') else 'UnknownTicker'
            if len(df_single_ticker) < self.params['max_lookback'] + 10: # Ensure enough data for lookback and initial calculations
                self.logger.warning("Insufficient data for ticker %s to generate signals (need %s, have %s).",
                                    ticker_name, self.params['max_lookback'] + 10, len(df_single_ticker))
                return pd.DataFrame()

            df_single_ticker = df_single_ticker.sort_index()
            result = pd.DataFrame(index=df_single_ticker.index)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                result[col] = df_single_ticker[col]

            # Initialize lists to store column data for performance
            n_rows = len(result)
            upper_line_values = [np.nan] * n_rows
            lower_line_values = [np.nan] * n_rows
            triangle_type_values = [None] * n_rows # Store as object type for strings/None
            in_pattern_values = [False] * n_rows
            signal_values = [0] * n_rows
            
            result['avg_volume'] = df_single_ticker['volume'].rolling(window=20, min_periods=1).mean() # min_periods=1 to avoid NaNs at start if needed

            max_lb = self.params['max_lookback']
            min_pts = self.params['min_points']
            break_thresh = self.params['breakout_threshold']
            vol_confirm = self.params['volume_confirm']
            min_patt_size = self.params['min_pattern_size']
            long_only_flag = self.params['long_only']

            for i in range(max_lb, n_rows):
                window = df_single_ticker.iloc[i - max_lb : i]
                current_close = result['close'].iloc[i]
                
                # Preserve previous state if still in pattern logic
                # Note: in_pattern_values[i] is default False
                # We determine in_pattern_values[i] based on conditions for current step 'i'
                # The check `in_pattern_values[i-1]` is to see if we *were* in a pattern previously.

                is_currently_in_pattern = False # Assume not in pattern or pattern breaks

                # Attempt to find or confirm a pattern
                upper_points, lower_points = self._get_triangle_points(window)

                if len(upper_points) >= min_pts and len(lower_points) >= min_pts:
                    upper_slope, upper_intercept = self._linear_regression(upper_points)
                    lower_slope, lower_intercept = self._linear_regression(lower_points)
                    
                    # x_predict is the last point of the window, relative index max_lookback - 1
                    x_predict = max_lb - 1 
                    current_upper_line = upper_slope * x_predict + upper_intercept
                    current_lower_line = lower_slope * x_predict + lower_intercept
                    
                    triangle_type = self._determine_triangle_type(upper_slope, lower_slope)

                    if triangle_type and current_upper_line > current_lower_line: # Ensure lines haven't crossed invalidly
                        upper_line_values[i] = current_upper_line
                        lower_line_values[i] = current_lower_line
                        triangle_type_values[i] = triangle_type
                        
                        pattern_height = current_upper_line - current_lower_line
                        price_percentage = pattern_height / current_close if current_close != 0 else 0

                        if price_percentage >= min_patt_size:
                            # Potential pattern is significant enough
                            is_currently_in_pattern = True # Mark as in pattern for this step

                            # Upward breakout check
                            if current_close > current_upper_line * (1 + break_thresh):
                                if not vol_confirm or result['volume'].iloc[i] > result['avg_volume'].iloc[i]:
                                    signal_values[i] = 1
                                    is_currently_in_pattern = False # Breakout occurred, no longer in pattern
                            # Downward breakout check
                            elif current_close < current_lower_line * (1 - break_thresh):
                                if not vol_confirm or result['volume'].iloc[i] > result['avg_volume'].iloc[i]:
                                    signal_values[i] = 0 if long_only_flag else -1
                                    is_currently_in_pattern = False # Breakout occurred
                
                in_pattern_values[i] = is_currently_in_pattern
            
            # Assign collected lists to DataFrame columns
            result['upper_line'] = upper_line_values
            result['lower_line'] = lower_line_values
            result['triangle_type'] = pd.Series(triangle_type_values, index=result.index, dtype=object)
            result['in_pattern'] = in_pattern_values
            result['signal'] = signal_values
            
            return result.dropna(subset=['close']) # Ensure essential data is present

        signals_list = []
        if isinstance(ticker, list):
            if price_data.index.nlevels > 1: # MultiIndex
                groups = price_data.groupby(level=0) # Group by 'ticker' index level
                for ticker_name, group_df in groups:
                    group_df.name = ticker_name # For logging/identification if needed
                    # group_df has DatetimeIndex
                    res_single = process_single_ticker(group_df.copy()) # Process copy to avoid SettingWithCopyWarning on group_df
                    if not res_single.empty:
                        res_single['ticker'] = ticker_name # Add ticker column
                        signals_list.append(res_single)
            else: # Single ticker in list, or price_data was not MultiIndex (should not happen with get_historical_prices for list)
                 self.logger.warning("Price data for list of tickers was not a MultiIndex. Processing may be incorrect.")
                 # Fallback or error, for now, try to process if it's a single df with 'ticker' column
                 if 'ticker' in price_data.columns and len(price_data['ticker'].unique()) > 1 : # multiple tickers in one df
                     groups = price_data.groupby('ticker')
                     for ticker_name, group_df in groups:
                         group_df.name = ticker_name
                         res_single = process_single_ticker(group_df.copy())
                         if not res_single.empty:
                             res_single['ticker'] = ticker_name
                             signals_list.append(res_single)
                 else: # treat as single df
                    price_data.name = ticker[0] if ticker else "Unknown"
                    res_single = process_single_ticker(price_data.copy())
                    if not res_single.empty:
                        res_single['ticker'] = price_data.name
                        signals_list.append(res_single)


            if not signals_list:
                return pd.DataFrame()
            signals_df = pd.concat(signals_list)
            # Ensure `signals_df` does not have a conflicting 'ticker' index level and 'ticker' column
            if isinstance(signals_df.index, pd.MultiIndex):
                idx_level0_name = signals_df.index.names[0] if signals_df.index.names[0] is not None else 'level_0'
                if idx_level0_name == 'ticker' and 'ticker' in signals_df.columns:
                    self.logger.info("TriangleBreakout: Correcting signals_df with conflicting 'ticker' index and column by dropping column.")
                    signals_df = signals_df.drop(columns=['ticker'])
            elif 'ticker' not in signals_df.columns and not signals_df.empty :
                 #This case means signals_df has a simple DatetimeIndex but lost the ticker column somehow.
                 #This indicates an issue in list processing. For safety, try to re-assign if possible (e.g. single ticker in list)
                 if len(ticker)==1:
                     signals_df['ticker'] = ticker[0]


        else: # Single ticker string
            price_data.name = ticker # Set name for process_single_ticker logging
            signals_df = process_single_ticker(price_data.copy())
            if not signals_df.empty:
                 signals_df['ticker'] = ticker # Add ticker column for single ticker case

        if signals_df.empty:
            self.logger.warning("No signals generated for ticker(s): %s.", ticker)
            return pd.DataFrame()
        
        # Sort index if it's a DatetimeIndex; if MultiIndex, it should be sorted by get_historical_prices or concat
        if isinstance(signals_df.index, pd.DatetimeIndex):
            signals_df = signals_df.sort_index()
        elif isinstance(signals_df.index, pd.MultiIndex):
            signals_df = signals_df.sort_index(level=[0,1])


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
                if 'ticker' in signals_with_rm.columns: # Group by ticker if it's a column
                    signals_with_rm = signals_with_rm.groupby('ticker', group_keys=False).tail(1)
                elif isinstance(signals_with_rm.index, pd.MultiIndex) and signals_with_rm.index.names[0] == 'ticker': # Group by ticker if it's index level 0
                     signals_with_rm = signals_with_rm.groupby(level=0, group_keys=False).tail(1)
                else: # single ticker dataframe
                    signals_with_rm = signals_with_rm.tail(1)
            else:
                self.logger.warning("No signals available to select 'latest_only'.")
                return pd.DataFrame()


        return signals_with_rm

    def _get_triangle_points(self, window_data: pd.DataFrame) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
        """
        Identify local upper (highs) and lower (lows) points for triangle patterns
        within the provided data window using vectorized operations.

        Args:
            window_data (pd.DataFrame): A subset of the price data with 'high' and 'low' columns.
                                       The index is expected to be a DatetimeIndex.

        Returns:
            Tuple:
                - List of (relative_index, high_value) tuples for local highs.
                - List of (relative_index, low_value) tuples for local lows.
        """
        if len(window_data) < 3:  # Need at least 3 points for peak/trough detection
            return [], []

        # Use a 0-based integer index for x-coordinates in regression
        relative_indices = np.arange(len(window_data))

        highs = window_data['high'].values
        lows = window_data['low'].values

        # Detect local highs (peak): value is greater than its immediate neighbors
        # Compare elements from index 1 to n-2 with their neighbors
        local_max_mask = (highs[1:-1] > highs[:-2]) & (highs[1:-1] > highs[2:])
        # Adjust indices to match original window_data positions for these masked points
        upper_rel_indices = relative_indices[1:-1][local_max_mask]
        upper_abs_values = highs[1:-1][local_max_mask]
        upper_points = list(zip(upper_rel_indices, upper_abs_values))

        # Detect local lows (trough): value is less than its immediate neighbors
        local_min_mask = (lows[1:-1] < lows[:-2]) & (lows[1:-1] < lows[2:])
        lower_rel_indices = relative_indices[1:-1][local_min_mask]
        lower_abs_values = lows[1:-1][local_min_mask]
        lower_points = list(zip(lower_rel_indices, lower_abs_values))
        
        return upper_points, lower_points

    def _linear_regression(self, points: List[Tuple[int, float]]) -> Tuple[float, float]:
        """
        Compute the least squares regression line (y = mx + c) for a set of points.

        Args:
            points (List[Tuple[int, float]]): List of (x, y) pairs.
                                             'x' is typically a relative integer index.

        Returns:
            Tuple (slope, intercept) of the best-fit line. Returns (0, y_val or 0) if regression cannot be performed.
        """
        if not points:
            return 0.0, 0.0
        
        x = np.array([p[0] for p in points], dtype=float)
        y = np.array([p[1] for p in points], dtype=float)

        if len(x) < 2: # Cannot compute slope with fewer than 2 points
            return 0.0, y[0] if len(y) > 0 else 0.0
        
        # Using formulas for simple linear regression
        x_mean = np.mean(x)
        y_mean = np.mean(y)
        
        numerator = np.sum((x - x_mean) * (y - y_mean))
        denominator = np.sum((x - x_mean)**2)
        
        if denominator == 0: # Avoid division by zero (all x values are the same)
            slope = 0.0
        else:
            slope = numerator / denominator
            
        intercept = y_mean - slope * x_mean
        return slope, intercept

    def _determine_triangle_type(self, upper_slope: float, lower_slope: float) -> Optional[str]:
        """
        Determine the triangle type based on the slopes of the trend lines.
        A valid triangle requires converging lines (upper descending, lower ascending, or one flat).

        Args:
            upper_slope (float): Slope of the upper trend line.
            lower_slope (float): Slope of the lower trend line.

        Returns:
            str or None: The triangle type ('symmetrical', 'ascending', 'descending') or None if invalid.
        """
        # Define a small tolerance for "nearly flat" lines
        flat_tolerance = 0.01 # Adjust based on price scale or normalize slopes if necessary

        # Lines must converge or one be flat while the other converges
        # Symmetrical: Upper descending, lower ascending
        if upper_slope < -flat_tolerance and lower_slope > flat_tolerance:
            return 'symmetrical'
        # Ascending Triangle: Upper nearly flat, lower ascending
        if abs(upper_slope) <= flat_tolerance and lower_slope > flat_tolerance:
            return 'ascending'
        # Descending Triangle: Upper descending, lower nearly flat
        if upper_slope < -flat_tolerance and abs(lower_slope) <= flat_tolerance:
            return 'descending'
        
        # Parallel lines or diverging lines are not valid triangles
        return None