# trading_system/src/strategies/adx_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from numba import (  # Added prange for potential parallel loops if beneficial
    njit, prange)

from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager

# Initialize module logger
logger = logging.getLogger(__name__)

@njit() # Enable parallel execution hint for Numba if loops are independent
def _calculate_wilder_smooth(series: np.ndarray, period: int) -> np.ndarray:
    """
    Calculates Wilder's smoothing (Recursive Moving Average).
    Parallel=True hint might help if Numba can optimize independent calculations.
    """
    n = len(series)
    smoothed = np.full(n, np.nan, dtype=np.float64)
    if n < period:
        return smoothed # Not enough data

    # Calculate initial average for the first 'period' valid values
    # Find the first valid value index if series starts with NaNs
    first_valid_idx = -1
    for i in range(n):
         if not np.isnan(series[i]):
              first_valid_idx = i
              break
    
    if first_valid_idx == -1 or first_valid_idx + period > n:
         return smoothed # Cannot calculate initial average

    initial_sum = 0.0
    valid_count_in_window = 0
    for i in range(first_valid_idx, first_valid_idx + period):
         if not np.isnan(series[i]):
            initial_sum += series[i]
            valid_count_in_window += 1

    if valid_count_in_window == 0: # Should not happen if first_valid_idx check works, but safety
         return smoothed

    smoothed[first_valid_idx + period - 1] = initial_sum / valid_count_in_window # Use valid_count if NaNs were present

    # Apply recursive smoothing
    for i in range(first_valid_idx + period, n):
        # Handle potential NaN in previous smoothed value or current series value
        prev_smoothed = smoothed[i-1]
        current_val = series[i]
        if np.isnan(prev_smoothed) or np.isnan(current_val):
             # If current is NaN, carry forward? Or reset? Wilder's assumes continuous data.
             # Let's carry forward the previous non-NaN smoothed value if current is NaN.
             # If prev_smoothed is NaN, we can't calculate, so result remains NaN.
             if np.isnan(prev_smoothed):
                  smoothed[i] = np.nan
             else: # Current value is NaN, carry previous smoothed value
                  smoothed[i] = prev_smoothed # Or np.nan if strict? Let's try carrying forward.
                  # Alternatively: smoothed[i] = np.nan # Safer if data gaps are significant
        else:
            smoothed[i] = (prev_smoothed * (period - 1) + current_val) / period

    return smoothed

@njit
def _calculate_adx_core_njit(high: np.ndarray, low: np.ndarray,
                             close: np.ndarray, lookback: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Core Numba-accelerated calculation of ADX, +DI, and -DI using Wilder's smoothing.
    This version adheres closely to the standard Wilder's methodology.

    Args:
        high (np.ndarray): Array of high prices (float64).
        low (np.ndarray): Array of low prices (float64).
        close (np.ndarray): Array of closing prices (float64).
        lookback (int): Lookback period for smoothing.

    Returns:
        tuple: NumPy arrays for +DI (plus_di), -DI (minus_di), and ADX.
               Arrays will have NaNs for the initial periods where calculation
               is not possible.
    """
    n = len(high)
    plus_di = np.full(n, np.nan, dtype=np.float64)
    minus_di = np.full(n, np.nan, dtype=np.float64)
    adx = np.full(n, np.nan, dtype=np.float64)
    epsilon = 1e-10 # Small constant to avoid division by zero

    if n <= lookback:
        return plus_di, minus_di, adx # Not enough data

    # 1. Calculate TR, +DM, -DM
    tr = np.zeros(n, dtype=np.float64)
    plus_dm = np.zeros(n, dtype=np.float64)
    minus_dm = np.zeros(n, dtype=np.float64)

    for i in range(1, n):
        high_diff = high[i] - high[i-1]
        low_diff = low[i-1] - low[i]

        tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
        tr[i] = max(tr[i], epsilon) # Ensure TR is not zero for ATR calculation

        if high_diff > low_diff and high_diff > 0:
            plus_dm[i] = high_diff
        else:
            plus_dm[i] = 0.0

        if low_diff > high_diff and low_diff > 0:
            minus_dm[i] = low_diff
        else:
            minus_dm[i] = 0.0

    # 2. Smooth TR, +DM, -DM using Wilder's smoothing
    atr = _calculate_wilder_smooth(tr, lookback)
    plus_dm_smoothed = _calculate_wilder_smooth(plus_dm, lookback)
    minus_dm_smoothed = _calculate_wilder_smooth(minus_dm, lookback)

    # 3. Calculate +DI and -DI
    # Start calculation from index 'lookback' where smoothed values become available
    for i in range(lookback, n):
         # Ensure ATR is not zero or NaN
         if not np.isnan(atr[i]) and atr[i] > epsilon:
             # Ensure smoothed DM values are not NaN
             if not np.isnan(plus_dm_smoothed[i]):
                 plus_di[i] = 100.0 * (plus_dm_smoothed[i] / atr[i])
             if not np.isnan(minus_dm_smoothed[i]):
                 minus_di[i] = 100.0 * (minus_dm_smoothed[i] / atr[i])
         # else: DI remains NaN if ATR is invalid


    # 4. Calculate DX = 100 * |+DI - -DI| / (+DI + -DI)
    dx = np.full(n, np.nan, dtype=np.float64)
    for i in range(lookback, n): # DX can be calculated where DI is available
        pdi = plus_di[i]
        mdi = minus_di[i]
        # Ensure both DI are valid numbers
        if not np.isnan(pdi) and not np.isnan(mdi):
            di_sum = pdi + mdi
            if di_sum > epsilon:
                dx[i] = 100.0 * (abs(pdi - mdi) / di_sum)
            else:
                dx[i] = 0.0 # If sum is zero, DX is zero

    # 5. Smooth DX series to get ADX
    # ADX calculation requires 'lookback' periods of DX.
    # So, ADX starts at index: (lookback - 1) [for first DI] + lookback [for DX smooth] = 2*lookback - 1
    adx = _calculate_wilder_smooth(dx, lookback)

    return plus_di, minus_di, adx


class ADXStrategy(BaseStrategy):
    """
    Implements an Average Directional Index (ADX) trend-following strategy.

    Strategy Logic:
    ---------------
    This strategy utilizes the ADX indicator along with the plus (+) and minus (-)
    Directional Indicators (DI) to identify potential trends and generate trading signals.

    1.  **Indicator Calculation:** Calculates ADX, +DI, and -DI using Wilder's smoothing via Numba.
    2.  **Signal Generation:** Generates signals based on ADX crossing a defined threshold
        (typically 25) and the relative position of +DI and -DI.
        - A new trade is considered *only* when ADX crosses *above* the threshold.
        - **Long Signal (1):** ADX crosses above threshold AND +DI > -DI.
        - **Short Signal (-1):** ADX crosses above threshold AND -DI > +DI. (Suppressed if long_only=True).
        - **Hold/Exit (0):** No ADX cross-above event or conditions not met.
    3.  **Signal Strength:** Calculated as `(DI+ - DI–) * ADX / 100`.
    4.  **Risk Management:** Raw signals are processed by `RiskManager` applying stops,
        targets, costs, etc., yielding the final 'position', realized 'return',
        'cumulative_return', and 'exit_type'.

    Integration & Compatibility:
    --------------------------
    - Inherits from `BaseStrategy`. Uses `RiskManager`.
    - Output columns ('signal', 'position', 'return', 'cumulative_return', 'exit_type', 'close', etc.)
      are structured for compatibility with the provided `StrategyOptimizer` and `PerformanceEvaluator`.
    - **Note:** Unlike a previous iteration, RiskManager output columns (`return`, `cumulative_return`, `exit_type`)
      are *not* renamed to avoid potential compatibility issues raised in feedback.

    Args:
        db_config: Database configuration object.
        params (Dict, optional): Strategy hyperparameters (see __init__ for details).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the ADX strategy with database configuration and parameters.
        """
        default_params = {
            'adx_lookback': 14,
            'adx_threshold': 25,
            'long_only': True,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.00,
            'slippage': 0.001, # Keep original param names used by RiskManager
            'transaction_cost': 0.001 # Keep original param names used by RiskManager
        }
        # Ensure numeric parameters from hyperopt/params are correctly typed
        resolved_params = {**default_params}
        if params:
             resolved_params.update(params) # Update defaults with provided params

             # Type casting / handling potential hyperopt structures
             resolved_params['adx_lookback'] = int(resolved_params['adx_lookback'])
             resolved_params['adx_threshold'] = int(resolved_params['adx_threshold'])
             resolved_params['long_only'] = bool(resolved_params['long_only'])
             resolved_params['stop_loss_pct'] = float(resolved_params['stop_loss_pct'])
             resolved_params['take_profit_pct'] = float(resolved_params['take_profit_pct'])
             resolved_params['slippage'] = float(resolved_params['slippage'])
             resolved_params['transaction_cost'] = float(resolved_params['transaction_cost'])

             # Handle trailing stop possibility (could be 0 or a float/uniform)
             tsl_val = resolved_params.get('trailing_stop_pct', 0.0)
             if isinstance(tsl_val, (list, tuple)) and len(tsl_val) > 1: # Check if it's hp.choice output like [0.0, value]
                 tsl_val = tsl_val[1] # Assume the second element is the actual value if TSL is enabled
             elif 'trailing_stop_pct_val' in resolved_params: # Handle structure like {'trailing_stop_pct': {'trailing_stop_pct_val': x}}
                 tsl_val = resolved_params['trailing_stop_pct_val']

             resolved_params['trailing_stop_pct'] = float(tsl_val)


        super().__init__(db_config, resolved_params) # Pass resolved params to BaseStrategy

        # Strategy specific parameters
        self.adx_lookback = self.params['adx_lookback'] # Get from self.params now
        self.adx_threshold = self.params['adx_threshold']
        self.long_only = self.params['long_only']

        # Minimum data validation requirement (ADX needs 2*lookback periods for first value)
        # Add buffer for stability
        self.min_records = (2 * self.adx_lookback) + 10

        # Initialize Risk Manager using parameters stored in self.params
        self.risk_manager = RiskManager(
            stop_loss_pct=self.params['stop_loss_pct'],
            take_profit_pct=self.params['take_profit_pct'],
            trailing_stop_pct=self.params['trailing_stop_pct'],
            slippage_pct=self.params['slippage'], # Use correct arg name for RiskManager
            transaction_cost_pct=self.params['transaction_cost'] # Use correct arg name
        )
        logger.info(f"ADXStrategy initialized with params: Lookback={self.adx_lookback}, "
                    f"Threshold={self.adx_threshold}, LongOnly={self.long_only}, "
                    f"SL={self.params['stop_loss_pct']:.2%}, TP={self.params['take_profit_pct']:.2%}, "
                    f"TSL={self.params['trailing_stop_pct']:.2%}")


    def _calculate_adx_and_signals(self, df_single: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates ADX indicators and generates raw trading signals for a single ticker.

        Args:
            df_single (pd.DataFrame): DataFrame for a single ticker containing
                                      'high', 'low', 'close' columns.

        Returns:
            pd.DataFrame: Original DataFrame augmented with 'plus_di', 'minus_di',
                          'adx', 'signal', and 'signal_strength' columns.
        """
        df_out = df_single.copy() # Work on a copy
        try:
            # Ensure input columns are float64 for Numba
            high = df_out['high'].values.astype(np.float64)
            low = df_out['low'].values.astype(np.float64)
            close = df_out['close'].values.astype(np.float64)

            # Calculate ADX, +DI, -DI using the corrected Numba function
            plus_di_vals, minus_di_vals, adx_vals = _calculate_adx_core_njit(
                high, low, close, self.adx_lookback
            )

            df_out['plus_di'] = plus_di_vals
            df_out['minus_di'] = minus_di_vals
            df_out['adx'] = adx_vals

            # --- Generate Signals ---
            # Shift values to compare current period's condition with previous period's state
            adx_prev = df_out['adx'].shift(1)
            adx_curr = df_out['adx']
            pdi_curr = df_out['plus_di']
            mdi_curr = df_out['minus_di']

            # Condition: ADX crosses *up* through the threshold
            # Check if current ADX is valid (not NaN) and previous was below threshold
            # and current is at or above threshold.
            adx_cross_up = (adx_prev < self.adx_threshold) & \
                           (adx_curr >= self.adx_threshold) & \
                           (adx_curr.notna())

            # Signal conditions based on DI dominance *at the time of the cross*
            long_signal_mask = adx_cross_up & (pdi_curr > mdi_curr)
            short_signal_mask = adx_cross_up & (mdi_curr > pdi_curr)

            # Initialize signal column
            df_out['signal'] = 0

            # Assign signals
            df_out.loc[long_signal_mask, 'signal'] = 1
            if not self.long_only:
                df_out.loc[short_signal_mask, 'signal'] = -1
            # If long_only, short signals remain 0

            # --- Calculate Signal Strength ---
            # Strength = (DI+ - DI-) * ADX / 100
            # Represents directional bias scaled by trend strength
            df_out['signal_strength'] = (pdi_curr - mdi_curr) * adx_curr / 100.0
            df_out['signal_strength'] = df_out['signal_strength'].fillna(0.0) # Fill NaNs

            # Note: NaNs will exist at the start due to lookback periods.
            # RiskManager and PerformanceEvaluator should handle these.

            return df_out

        except Exception as e:
            logger.error(f"Error calculating ADX and signals: {e}", exc_info=True)
            # Add NaN columns if they don't exist upon error
            for col in ['plus_di', 'minus_di', 'adx', 'signal', 'signal_strength']:
                if col not in df_out.columns:
                    df_out[col] = np.nan
            return df_out


    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generates trading signals and applies risk management for ADX strategy.

        Args:
            ticker (Union[str, List[str]]): Ticker symbol(s).
            start_date (str, optional): Start date (YYYY-MM-DD).
            end_date (str, optional): End date (YYYY-MM-DD).
            initial_position (int): Initial position (0, 1, -1). Default 0.
            latest_only (bool): If True, return only the last row per ticker. Default False.

        Returns:
            pd.DataFrame: DataFrame with prices, indicators, signals, positions,
                          and risk-managed returns (`return`, `cumulative_return`).
                          Index is DatetimeIndex (single) or MultiIndex ['ticker', 'date'] (multi).
        """
        try:
            # Fetch data using BaseStrategy method - includes lookback calculation
            data = self.get_historical_prices(
                tickers=ticker,
                from_date=start_date,
                to_date=end_date,
                lookback=self.min_records # Use calculated min_records for buffer
            )

            if data.empty:
                raise DataRetrievalError(f"No data returned for ticker(s): {ticker}")

            # --- Process based on returned data format ---

            # Case 1: Single Ticker (DataFrame with DatetimeIndex)
            if isinstance(data.index, pd.DatetimeIndex):
                ticker_symbol = ticker if isinstance(ticker, str) else ticker[0]
                logger.debug(f"Processing single ticker: {ticker_symbol}")

                if not self._validate_data(data, min_records=self.min_records):
                    logger.warning(f"Insufficient data for {ticker_symbol}. Skipping.")
                    return pd.DataFrame()

                # Calculate indicators and raw signals
                df_processed = self._calculate_adx_and_signals(data) # No copy needed here

                # Apply Risk Management - Keep original output column names
                df_processed = self.risk_manager.apply(df_processed, initial_position)

                # Calculate standard performance columns
                df_processed['daily_return'] = df_processed['close'].pct_change(fill_method=None).fillna(0)
                if 'position' in df_processed.columns:
                     df_processed['strategy_return'] = df_processed['daily_return'] * df_processed['position'].shift(1).fillna(0)
                else:
                     logger.warning("Risk Manager did not return 'position' column. Setting 'strategy_return' to 0.")
                     df_processed['strategy_return'] = 0.0

                # Output columns now include: ..., signal, signal_strength, position, return, cumulative_return, exit_type, daily_return, strategy_return

                return df_processed.iloc[-1:] if latest_only else df_processed

            # Case 2: Multiple Tickers (DataFrame with MultiIndex)
            elif isinstance(data.index, pd.MultiIndex):
                ticker_list = list(data.index.get_level_values(0).unique())
                logger.debug(f"Processing multiple tickers: {ticker_list}")
                all_results = []

                for ticker_name, group_df in data.groupby(level=0):
                    logger.debug(f"Processing group for ticker: {ticker_name}")
                    # Remove ticker index level for processing
                    group_df = group_df.reset_index(level=0, drop=True)

                    if not self._validate_data(group_df, min_records=self.min_records):
                        logger.warning(f"Insufficient data for {ticker_name}. Skipping.")
                        continue

                    # Calculate indicators and raw signals
                    df_processed = self._calculate_adx_and_signals(group_df)

                    # Apply Risk Management
                    ticker_initial_pos = initial_position if isinstance(initial_position, int) else initial_position.get(ticker_name, 0)
                    df_processed = self.risk_manager.apply(df_processed, ticker_initial_pos)

                    # Calculate standard performance columns
                    df_processed['daily_return'] = df_processed['close'].pct_change(fill_method=None).fillna(0)
                    if 'position' in df_processed.columns:
                        df_processed['strategy_return'] = df_processed['daily_return'] * df_processed['position'].shift(1).fillna(0)
                    else:
                         logger.warning(f"Risk Manager did not return 'position' column for {ticker_name}. Setting 'strategy_return' to 0.")
                         df_processed['strategy_return'] = 0.0


                    # Add ticker identifier back for concatenation
                    df_processed['ticker'] = ticker_name

                    if latest_only:
                        all_results.append(df_processed.iloc[[-1]])
                    else:
                        all_results.append(df_processed)

                if not all_results:
                    logger.warning("No tickers had sufficient data or processing failed.")
                    return pd.DataFrame()

                # Concatenate results
                final_df = pd.concat(all_results)

                # Set the final MultiIndex ['ticker', 'date']
                # Ensure 'date' exists (it should be the index from group processing)
                if isinstance(final_df.index, pd.DatetimeIndex):
                     final_df.index.name = 'date' # Name index if needed
                     final_df = final_df.reset_index() # Move date to column
                     final_df = final_df.set_index(['ticker', 'date']) # Set MultiIndex
                else:
                     # This case might occur if concat somehow loses the index
                     logger.error("Index type lost after concat. Attempting recovery for MultiIndex.")
                     if 'date' in final_df.columns and 'ticker' in final_df.columns:
                          final_df['date'] = pd.to_datetime(final_df['date'])
                          final_df = final_df.set_index(['ticker', 'date'])
                     else:
                          logger.error("Cannot set final MultiIndex.")
                          # Return potentially incorrectly indexed data
                          return final_df

                return final_df.sort_index() # Sort by ticker, then date

            else:
                raise TypeError(f"Unexpected index type from get_historical_prices: {type(data.index)}")

        except DataRetrievalError as dre:
            logger.error(f"Data retrieval error in ADXStrategy: {dre}")
            return pd.DataFrame()
        except ValueError as ve:
             logger.error(f"Value error during ADX processing: {ve}", exc_info=True)
             return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error in ADXStrategy generate_signals: {e}", exc_info=True)
            return pd.DataFrame()