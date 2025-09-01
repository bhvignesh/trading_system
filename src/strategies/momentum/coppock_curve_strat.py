# trading_system/src/strategies/momentum/coppock_curve_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager

logger = logging.getLogger(__name__)

class CoppockCurveStrategy(BaseStrategy):
    """
    Implements the Coppock Curve momentum strategy with integrated risk management.

    Strategy Logic:
    1. Calculates two Rate-of-Change (ROC) indicators based on specified monthly lookbacks.
    2. Sums the two ROC values to get a combined ROC.
    3. Smooths the combined ROC using a Weighted Moving Average (WMA) with linearly increasing weights
       over a specified lookback period. This smoothed value is the Coppock Curve (CC).
    4. Generates raw trading signals based on the CC using one of two methods:
        - 'zero_crossing': Buy (+1) when CC crosses above 0. Optionally requires CC to have been
                           negative for 'zc_sustain_days' periods prior (default: 4).
                           Sell (-1) when CC crosses below 0. Optionally requires CC to have been
                           positive for 'zc_sustain_days' periods prior (default: 4).
                           Controlled by 'zc_require_prior_state' parameter.
        - 'directional': Buy (+1) on sustained upward CC movement ('sustain_days'). Optionally
                         requires a confirmed prior downtrend ('dir_require_prior_trend',
                         'trend_strength_threshold').
                         Sell (-1) on sustained downward CC movement ('sustain_days'). Optionally
                         requires a confirmed prior uptrend ('dir_require_prior_trend',
                         'trend_strength_threshold').
    5. Optionally restricts signals to long-only (clips -1 signals to 0).
    6. Calculates signal strength based on the CC's divergence from its short-term moving average,
       optionally normalized.
    7. Integrates with the RiskManager component to apply stop-loss, take-profit, trailing stops,
       slippage, and transaction costs to the raw signals, determining final positions and returns.

    Output DataFrame includes:
    - Price data (open, high, low, close)
    - Coppock Curve indicator ('cc')
    - Raw signal ('signal')
    - Signal strength ('signal_strength')
    - Risk-managed position ('position')
    - Daily return of the asset ('daily_return')
    - Strategy return based on daily return and prior day's position ('strategy_return')
    - Risk-managed trade return ('rm_strategy_return')
    - Risk-managed cumulative return ('rm_cumulative_return')
    - Risk management exit reason ('rm_action')
    """
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Coppock Curve Strategy.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (Optional[Dict]): Strategy parameters dictionary. Expected keys:
                - 'roc1_months' (int, default: 14): Lookback months for the first ROC.
                - 'roc2_months' (int, default: 11): Lookback months for the second ROC.
                - 'wma_lookback' (int, default: 10): Lookback months for WMA smoothing.
                - 'method' (str, default: 'zero_crossing'): Signal generation method ('zero_crossing' or 'directional').
                - 'long_only' (bool, default: True): If True, only long positions are taken.
                - 'sustain_days' (int, default: 4): For 'directional' method, days of sustained movement.
                - 'trend_strength_threshold' (float, default: 0.75): For 'directional' method, threshold for prior trend confirmation.
                - 'strength_window' (int, default: 504): Lookback window (days) for strength normalization.
                - 'normalize_strength' (bool, default: True): Whether to normalize signal strength.
                - 'stop_loss_pct' (float, default: 0.05): Stop loss percentage.
                - 'take_profit_pct' (float, default: 0.10): Take profit percentage.
                - 'trailing_stop_pct' (float, default: 0.0): Trailing stop percentage (0 disables).
                - 'slippage_pct' (float, default: 0.001): Slippage per transaction.
                - 'transaction_cost_pct' (float, default: 0.001): Transaction cost per transaction.
                - 'zc_require_prior_state' (bool, default: True): For 'zero_crossing' method, if True, requires CC
                                                                to be below/above 0 for zc_sustain_days before crossing.
                                                                If False, triggers on simple zero cross.
                - 'zc_sustain_days' (int, default: 4): For 'zero_crossing' method, days CC must be below/above 0
                                                      before a crossing signal (only used if zc_require_prior_state is True).
                - 'dir_require_prior_trend' (bool, default: True): For 'directional' method, if True, requires the sustained
                                                                 move to follow a confirmed counter-trend. If False,
                                                                 only sustained direction is required.
        """
        default_params = {
            'roc1_months': 14, 'roc2_months': 11, 'wma_lookback': 10,
            'method': 'zero_crossing', 'long_only': True,
            'sustain_days': 4, 'trend_strength_threshold': 0.75,
            'strength_window': 504, 'normalize_strength': True,
            'stop_loss_pct': 0.05, 'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0, 'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'zc_require_prior_state': True,  # New parameter for zero_crossing strictness
            'zc_sustain_days': 4, # Renamed from sustain_days used internally in zero_crossing logic previously
            'dir_require_prior_trend': True # New parameter for directional strictness
        }
        # Ensure integer params from Hyperopt are correctly typed if passed as float
        int_params_from_search = [
            'roc1_months', 'roc2_months', 'wma_lookback', 'sustain_days',
            'strength_window', 'zc_sustain_days'
        ]

        params = params or default_params
        # Apply defaults for missing keys
        for key, value in default_params.items():
            params.setdefault(key, value)
        # Ensure correct typing for integer params
        for p_key in int_params_from_search:
             if p_key in params:
                  try:
                       params[p_key] = int(params[p_key])
                  except (ValueError, TypeError):
                       logger.warning(f"Could not convert param '{p_key}' to int. Using default: {default_params[p_key]}")
                       params[p_key] = int(default_params[p_key])

        super().__init__(db_config, params)
        self.logger = logging.getLogger(self.__class__.__name__)

        # Strategy-specific parameters
        self.roc1_months = max(1, params['roc1_months'])
        self.roc2_months = max(1, params['roc2_months'])
        self.wma_lookback_months = max(1, params['wma_lookback']) # Store the original month value
        self.roc1_days = self.roc1_months * 21  # Approx trading days/month
        self.roc2_days = self.roc2_months * 21
        self.wma_lookback_days = self.wma_lookback_months * 21

        self.method = params['method']
        self.long_only = params['long_only']
        self.sustain_days = params['sustain_days'] # For directional method
        self.trend_strength_threshold = params['trend_strength_threshold'] # For directional method
        self.strength_window = params['strength_window']
        self.normalize_strength = params['normalize_strength']

        # New parameters for signal generation flexibility
        self.zc_require_prior_state = params['zc_require_prior_state']
        self.zc_sustain_days = max(1, params['zc_sustain_days']) # Min 1 day for consistency check
        self.dir_require_prior_trend = params['dir_require_prior_trend']

        if min(self.roc1_days, self.roc2_days) <= self.wma_lookback_days:
            self.logger.warning("ROC periods should ideally exceed the WMA window for standard Coppock interpretation.")

        # Initialize RiskManager with parameters from the params dict
        risk_params = {
            'stop_loss_pct': params.get('stop_loss_pct', default_params['stop_loss_pct']),
            'take_profit_pct': params.get('take_profit_pct', default_params['take_profit_pct']),
            'trailing_stop_pct': params.get('trailing_stop_pct', default_params['trailing_stop_pct']),
            'slippage_pct': params.get('slippage_pct', default_params['slippage_pct']),
            'transaction_cost_pct': params.get('transaction_cost_pct', default_params['transaction_cost_pct'])
        }
        self.risk_manager = RiskManager(**risk_params)

    @staticmethod
    def generate_repeated_array(input_number: int) -> np.ndarray:
        """
        Generate a 1D numpy array for WMA weights. The sequence goes from
        input_number down to 1, with each number repeated 21 times.

        Example: input_number=2 -> [2, 2, ..., 2, 1, 1, ..., 1] (each 21 times)

        Args:
            input_number (int): The starting integer for the sequence (typically wma_lookback_months).

        Returns:
            np.ndarray: Array of repeated integer sequences.
        """
        if not isinstance(input_number, int) or input_number <= 0:
            raise ValueError("Input must be a positive integer for weight generation.")
        repetitions = 21
        number_sequence = np.arange(input_number, 0, -1)
        repeated_array = np.repeat(number_sequence, repetitions)
        return repeated_array

    def _calculate_signals_single(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the Coppock Curve indicator, raw signals, and strength for a single ticker.

        Args:
            data (pd.DataFrame): Historical price data for one ticker (OHLCV).
                                 Index must be DatetimeIndex.

        Returns:
            pd.DataFrame: DataFrame with columns: open, close, high, low, cc,
                          signal, signal_strength. Returns empty if calculation fails.
        """
        if not isinstance(data.index, pd.DatetimeIndex):
             logger.error("Input data for _calculate_signals_single must have a DatetimeIndex.")
             return pd.DataFrame()
        if 'close' not in data.columns or 'high' not in data.columns or 'low' not in data.columns:
             logger.error("Input data missing required price columns (close, high, low).")
             return pd.DataFrame()

        df = data[['open', 'high', 'low', 'close']].copy() # Work with essential columns

        # 1. Calculate Coppock Curve (CC)
        try:
            roc1 = df['close'].pct_change(self.roc1_days) * 100
            roc2 = df['close'].pct_change(self.roc2_days) * 100
            combined_roc = (roc1 + roc2)

            # Use the generate_repeated_array for WMA weights
            # input_number should be the original number of months
            weights = self.generate_repeated_array(self.wma_lookback_months)
            # Apply WMA using the generated weights
            cc = combined_roc.rolling(
                window=self.wma_lookback_days,
                min_periods=int(self.wma_lookback_days * 0.8) # Require 80% of window
            ).apply(
                lambda x: np.dot(x, weights[-len(x):]) / weights[-len(x):].sum() if weights[-len(x):].sum() != 0 else 0,
                raw=True
            )
            df['cc'] = cc
            df = df.dropna(subset=['cc']) # Drop rows where CC couldn't be calculated

        except Exception as e:
            logger.error(f"Error calculating Coppock Curve: {e}")
            return pd.DataFrame()

        if df.empty:
            logger.warning("DataFrame empty after Coppock Curve calculation and NaN drop.")
            return df

        # 2. Generate Raw Signals based on selected method
        shifted_cc = df['cc'].shift(1) # Used in both methods

        if self.method == 'zero_crossing':
            if self.zc_require_prior_state:
                # Original strict logic: Check for sustained state BEFORE the crossing bar
                sustain_periods = max(1, self.zc_sustain_days) # Ensure at least 1 period
                was_negative = (shifted_cc < 0).rolling(sustain_periods, min_periods=sustain_periods).sum() == sustain_periods
                was_positive = (shifted_cc > 0).rolling(sustain_periods, min_periods=sustain_periods).sum() == sustain_periods
                buy_mask = was_negative & (df['cc'] > 0)
                sell_mask = was_positive & (df['cc'] < 0)
            else:
                # Relaxed logic: Simple zero cross
                buy_mask = (shifted_cc < 0) & (df['cc'] > 0)
                sell_mask = (shifted_cc > 0) & (df['cc'] < 0)

        elif self.method == 'directional':
            df['direction'] = np.sign(df['cc'].diff())
            # Check for sustained directionality for 'sustain_days'
            sustained_up = (df['direction'].rolling(window=self.sustain_days, min_periods=self.sustain_days).min() > 0)
            sustained_down = (df['direction'].rolling(window=self.sustain_days, min_periods=self.sustain_days).max() < 0)

            if self.dir_require_prior_trend:
                # Original strict logic: Require a confirmed prior counter-trend
                lookback_trend = max(self.sustain_days * 2, 10) # Lookback for prior trend
                prev_direction_block = df['direction'].shift(self.sustain_days) # Direction before the sustained move started
                # Assess average direction in the lookback period *before* the sustained move
                prev_trend_mean = prev_direction_block.rolling(window=lookback_trend, min_periods=lookback_trend).mean()

                # Define prior trend state based on the average direction meeting the threshold
                prev_trend_state = np.select(
                    [prev_trend_mean > self.trend_strength_threshold,
                     prev_trend_mean < -self.trend_strength_threshold],
                    [1, -1], # 1 = prior uptrend, -1 = prior downtrend
                    default=0 # 0 = neutral/no clear trend meeting threshold
                )
                # Buy if sustained up after prior confirmed downtrend
                buy_mask = sustained_up & (prev_trend_state == -1)
                # Sell if sustained down after prior confirmed uptrend
                sell_mask = sustained_down & (prev_trend_state == 1)
            else:
                # Relaxed logic: Signal only on sustained direction
                buy_mask = sustained_up
                sell_mask = sustained_down
        else:
            logger.error(f"Invalid signal generation method: {self.method}")
            return pd.DataFrame()

        df['signal'] = 0
        df.loc[buy_mask, 'signal'] = 1
        df.loc[sell_mask, 'signal'] = -1

        # 3. Calculate Signal Strength
        # Use divergence from a short-term MA of CC (e.g., 21-day)
        ref_ma = df['cc'].rolling(21, min_periods=1).mean().shift() # Shift to avoid lookahead bias
        strength_raw = (df['cc'] - ref_ma) / ref_ma.abs().replace(0, 1e-6) # Avoid division by zero

        if self.normalize_strength:
            rolling_min = strength_raw.rolling(self.strength_window, min_periods=1).min()
            rolling_max = strength_raw.rolling(self.strength_window, min_periods=1).max()
            range_ = (rolling_max - rolling_min).replace(0, 1e-6) # Avoid division by zero
            df['signal_strength'] = 2 * ((strength_raw - rolling_min) / range_) - 1
        else:
            df['signal_strength'] = strength_raw

        # Apply long-only constraint if specified AFTER potential sell signals are generated
        if self.long_only:
            df.loc[df['signal'] == -1, 'signal'] = 0 # Clip sell signals to 0 (exit long)

        # Keep only necessary columns for the next step (RiskManager) + indicator value
        return df[['open', 'close', 'high', 'low', 'cc', 'signal', 'signal_strength']].copy()


    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False,
    ) -> pd.DataFrame:
        """
        Generate risk-managed trading signals for the Coppock Curve strategy.

        Fetches data, calculates indicators and raw signals, applies risk management,
        and computes performance metrics consistent with the AwesomeOscillatorStrategy output.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtest start date ('YYYY-MM-DD').
            end_date (str, optional): Backtest end date ('YYYY-MM-DD').
            initial_position (int): Starting trading position (0: flat, 1: long, -1: short). Default is 0.
                                      For multi-ticker, this applies to all unless RiskManager handles dict.
            latest_only (bool): If True, returns only the most recent signal row(s).

        Returns:
            pd.DataFrame: DataFrame containing prices, indicator, signals, positions,
                          returns, and risk management details. Structure matches
                          AwesomeOscillatorStrategy output. Returns empty DataFrame on failure.
        """
        try:
            # Determine lookback buffer needed for calculations
            # Needs longest ROC/WMA, plus strength window, plus MA for strength, plus buffer
            min_periods_indicator = max(self.roc1_days, self.roc2_days, self.wma_lookback_days)
            # Buffer needed depends on signal method conditions (e.g., zc_sustain_days, lookback_trend)
            signal_method_buffer = 0
            if self.method == 'zero_crossing' and self.zc_require_prior_state:
                 signal_method_buffer = self.zc_sustain_days
            elif self.method == 'directional' and self.dir_require_prior_trend:
                 signal_method_buffer = max(self.sustain_days * 2, 10) # lookback_trend

            min_periods = min_periods_indicator + signal_method_buffer + self.strength_window + 21 + 5 # Extra buffer

            # Retrieve historical price data using BaseStrategy method
            data = self.get_historical_prices(
                tickers=ticker,
                from_date=start_date,
                to_date=end_date,
                lookback=min_periods if not (start_date or end_date) else None
            )


            if data.empty:
                 logger.warning(f"No historical price data found for {ticker} within the specified parameters.")
                 return pd.DataFrame()

            data = data.sort_index() # Ensure data is sorted by date/ticker

            signals_list = []
            # Handle MULTI-TICKER input (data has MultiIndex)
            if isinstance(ticker, list):
                 if not isinstance(data.index, pd.MultiIndex):
                      logger.error("Expected MultiIndex for multiple tickers, but got single index.")
                      return pd.DataFrame()
                 # Assume level 0 of MultiIndex is the ticker identifier
                 ticker_level_name = data.index.names[0] if data.index.names[0] else 0
                 # Check if data is sorted appropriately for groupby
                 if not data.index.is_monotonic_increasing:
                     logger.warning("MultiIndex data is not sorted. Sorting by index.")
                     data = data.sort_index()

                 for tkr, group_data in data.groupby(level=ticker_level_name, sort=False): # Use sort=False if already sorted
                      # Drop the ticker level from index for single ticker processing
                     single_ticker_data = group_data.reset_index(level=ticker_level_name, drop=True)

                     if not self._validate_data(single_ticker_data, min_records=min_periods):
                         logger.warning(f"Insufficient data for {tkr} after fetching ({len(single_ticker_data)} records, need {min_periods}). Skipping.")
                         continue

                     # Calculate raw signals and strength for this ticker
                     sig_raw = self._calculate_signals_single(single_ticker_data)
                     if sig_raw.empty:
                         logger.warning(f"Signal calculation failed for ticker {tkr}. Skipping.")
                         continue

                     # Apply Risk Management
                     rm_input_cols = ['signal', 'close', 'high', 'low', 'open']
                     if not all(col in sig_raw.columns for col in rm_input_cols):
                          logger.error(f"Ticker {tkr}: Raw signals DataFrame missing columns required by RiskManager: {rm_input_cols}. Skipping.")
                          continue

                     # Pass initial position (could be a dict handled by RM)
                     # For now, assume RiskManager handles single int or dict if passed
                     rm_initial_pos = initial_position # Pass the original arg, let RM handle dict if needed
                     sig_rm = self.risk_manager.apply(sig_raw[rm_input_cols], rm_initial_pos) # Pass initial pos

                     if sig_rm.empty:
                         logger.warning(f"Risk management failed for ticker {tkr}. Skipping.")
                         continue

                     # Add back indicator and strength columns after RM
                     sig_rm = sig_rm.join(sig_raw[['cc', 'signal_strength']], how='left')

                     # Calculate additional metrics
                     sig_rm['daily_return'] = sig_rm['close'].pct_change().fillna(0)
                     sig_rm['strategy_return'] = sig_rm['daily_return'] * sig_rm['position'].shift(1).fillna(0)

                     # Rename RM columns for consistency
                     sig_rm.rename(columns={'return': 'rm_strategy_return',
                                         'cumulative_return': 'rm_cumulative_return',
                                         'exit_type': 'rm_action'}, inplace=True)

                     # Add ticker identifier column
                     sig_rm['ticker'] = tkr
                     signals_list.append(sig_rm)

                 if not signals_list:
                     logger.warning("No signals generated for any ticker in the list.")
                     return pd.DataFrame()

                 # Concatenate results for all tickers
                 signals = pd.concat(signals_list)
                 # Restore the original MultiIndex structure if needed (RM might return Ticker as column)
                 # This assumes RM output has date index and 'ticker' column if input was MultiIndex
                 if 'ticker' in signals.columns and signals.index.name == 'date': # Common RM output format
                    signals = signals.set_index(['ticker', signals.index]).sort_index()

                 # Handle latest_only for multi-ticker: get last row per ticker
                 if latest_only:
                     # Ensure index is sorted before tail
                     if not signals.index.is_monotonic_increasing: signals = signals.sort_index()
                     signals = signals.groupby(level=ticker_level_name, group_keys=False).tail(1) # Works on MultiIndex


            # Handle SINGLE-TICKER input
            else:
                if not isinstance(data.index, pd.DatetimeIndex):
                     logger.error("Expected DatetimeIndex for single ticker, but got different index type.")
                     return pd.DataFrame()
                if not self._validate_data(data, min_records=min_periods):
                    logger.warning(f"Insufficient data for {ticker} ({len(data)} records, need {min_periods}).")
                    return pd.DataFrame()

                # Calculate raw signals and strength
                sig_raw = self._calculate_signals_single(data)
                if sig_raw.empty:
                    logger.warning(f"Signal calculation failed for ticker {ticker}.")
                    return pd.DataFrame()

                # Apply Risk Management
                rm_input_cols = ['signal', 'close', 'high', 'low', 'open']
                if not all(col in sig_raw.columns for col in rm_input_cols):
                     logger.error(f"Ticker {ticker}: Raw signals DataFrame missing columns required by RiskManager: {rm_input_cols}.")
                     return pd.DataFrame()

                # Pass initial position (must be int for single ticker)
                if not isinstance(initial_position, int):
                     logger.warning(f"Received non-integer initial_position ({initial_position}) for single ticker {ticker}. Using 0.")
                     rm_initial_pos = 0
                else:
                     rm_initial_pos = initial_position

                signals = self.risk_manager.apply(sig_raw[rm_input_cols], rm_initial_pos) # Pass initial pos
                if signals.empty:
                     logger.warning(f"Risk management failed for ticker {ticker}.")
                     return pd.DataFrame()

                # Add back indicator and strength
                signals = signals.join(sig_raw[['cc', 'signal_strength']], how='left')

                # Calculate additional metrics
                signals['daily_return'] = signals['close'].pct_change().fillna(0)
                signals['strategy_return'] = signals['daily_return'] * signals['position'].shift(1).fillna(0)

                # Rename RM columns
                signals.rename(columns={'return': 'rm_strategy_return',
                                        'cumulative_return': 'rm_cumulative_return',
                                        'exit_type': 'rm_action'}, inplace=True)

                # Handle latest_only for single-ticker
                if latest_only:
                    signals = signals.iloc[[-1]].copy() # Return only the last row

            # Ensure standard column order if possible (adjust as needed)
            final_cols_base = [
                'open', 'high', 'low', 'close', 'cc', 'signal', 'signal_strength',
                'position', 'daily_return', 'strategy_return',
                'rm_strategy_return', 'rm_cumulative_return', 'rm_action'
            ]
            if isinstance(ticker, list):
                final_cols = final_cols_base # Ticker is in index
            else:
                final_cols = final_cols_base # Ticker not applicable

            # Add missing columns with NaN if they weren't generated
            for col in final_cols:
                if col not in signals.columns:
                    # Check if it's expected (e.g., 'ticker' for single)
                    if col == 'ticker' and not isinstance(ticker, list):
                         continue # Don't add ticker column for single ticker mode
                    signals[col] = np.nan

            # Return only the desired columns present in the DataFrame
            present_final_cols = [col for col in final_cols if col in signals.columns]
            return signals[present_final_cols].copy() # Return a copy with desired columns

        except DataRetrievalError as dre:
             logger.error(f"Data retrieval error for {ticker}: {dre}")
             return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error generating signals for {ticker}: {str(e)}", exc_info=True) # Log traceback
            return pd.DataFrame()

    def __repr__(self):
        """Return a string representation of the CoppockCurveStrategy instance."""
        risk_params = self.risk_manager # Access the initialized RM
        # Add new params to representation
        zc_cond = f"PriorState({self.zc_sustain_days}d)" if self.zc_require_prior_state else "SimpleX"
        dir_cond = f"PriorTrend({self.trend_strength_threshold:.2f})" if self.dir_require_prior_trend else "SustainOnly"
        method_detail = f"{self.method}({zc_cond})" if self.method == 'zero_crossing' else f"{self.method}({dir_cond}, Sustain={self.sustain_days}d)"

        repr_str = (
            f"CoppockCurveStrategy(ROC={self.roc1_months}mo/{self.roc2_months}mo, "
            f"WMA={self.wma_lookback_months}mo ({self.wma_lookback_days}d), "
            f"Method={method_detail}, LongOnly={self.long_only}, "
            f"SL={risk_params.stop_loss_pct:.1%}, TP={risk_params.take_profit_pct:.1%}, "
            f"TSL={'Disabled' if risk_params.trailing_stop_pct == 0 else f'{risk_params.trailing_stop_pct:.1%}'})"
        )
        return repr_str