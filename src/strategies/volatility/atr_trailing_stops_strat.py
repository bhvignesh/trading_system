# trading_system/src/strategies/volatality/atr_trailing_stops.py

from typing import Dict, List, Optional, Union

import numba  # For performance optimization
import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
# Assuming RiskManager is in this path based on the original import
from src.strategies.risk_management import RiskManager


@numba.njit
def _calculate_stops_and_positions_nb(
    close_prices: np.ndarray,
    high_prices: np.ndarray,
    low_prices: np.ndarray,
    atr_values: np.ndarray,
    trend_up_flags: np.ndarray,
    atr_multiplier: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Calculate ATR trailing stops and position states using Numba for performance.

    This function iterates through price data to determine long/short positions
    and their corresponding trailing stop levels based on ATR.

    Args:
        close_prices (np.ndarray): Array of closing prices.
        high_prices (np.ndarray): Array of high prices.
        low_prices (np.ndarray): Array of low prices.
        atr_values (np.ndarray): Array of ATR values.
        trend_up_flags (np.ndarray): Boolean array indicating if the trend is up.
        atr_multiplier (float): Multiplier for ATR to set stop distance.

    Returns:
        tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
            - trailing_stop_long_arr: Calculated long trailing stops.
            - trailing_stop_short_arr: Calculated short trailing stops.
            - in_long_arr: Boolean array indicating if in a long position.
            - in_short_arr: Boolean array indicating if in a short position.
    """
    n = len(close_prices)
    trailing_stop_long_arr = np.full(n, np.nan, dtype=np.float64)
    trailing_stop_short_arr = np.full(n, np.nan, dtype=np.float64)
    in_long_arr = np.full(n, False, dtype=np.bool_)
    in_short_arr = np.full(n, False, dtype=np.bool_)

    # State variables carried across iterations
    in_long_state = False
    in_short_state = False
    current_ts_long_state = np.nan
    current_ts_short_state = np.nan

    if n == 0:
        return trailing_stop_long_arr, trailing_stop_short_arr, in_long_arr, in_short_arr

    first_valid_atr_processed = False

    for i in range(n):
        current_close = close_prices[i]
        current_atr = atr_values[i]
        current_high = high_prices[i]
        current_low = low_prices[i]
        current_trend_is_up = trend_up_flags[i]

        # Skip if ATR is NaN, but carry forward existing position state and stop
        if np.isnan(current_atr):
            if in_long_state: # If already in a long position
                trailing_stop_long_arr[i] = current_ts_long_state # Carry forward the stop
                in_long_arr[i] = True
            elif in_short_state: # If already in a short position
                trailing_stop_short_arr[i] = current_ts_short_state # Carry forward the stop
                in_short_arr[i] = True
            # If not in a position and ATR is NaN, nothing to do for this bar
            continue

        # Initialize position on the first bar with valid ATR
        if not first_valid_atr_processed:
            first_valid_atr_processed = True
            if current_trend_is_up:
                in_long_state = True
                in_short_state = False
                current_ts_long_state = current_close - (current_atr * atr_multiplier)
            else: # Trend is down or flat (flat treated as down for entry)
                in_long_state = False
                in_short_state = True
                current_ts_short_state = current_close + (current_atr * atr_multiplier)
        else:
            # Logic for subsequent bars
            # Use the stop value that was set at the end of the previous bar for exit checks
            prev_bar_ts_long = trailing_stop_long_arr[i-1]
            prev_bar_ts_short = trailing_stop_short_arr[i-1]

            if in_long_state:
                # Check for exit from long position
                if current_low <= prev_bar_ts_long:
                    in_long_state = False
                    current_ts_long_state = np.nan # Clear stop state for long
                    # Check for immediate short entry if trend flipped
                    if not current_trend_is_up:
                        in_short_state = True
                        current_ts_short_state = current_close + (current_atr * atr_multiplier)
                else:
                    # Update (trail) long trailing stop
                    candidate_stop = current_close - (current_atr * atr_multiplier)
                    current_ts_long_state = max(current_ts_long_state, candidate_stop)
            elif in_short_state:
                # Check for exit from short position
                if current_high >= prev_bar_ts_short:
                    in_short_state = False
                    current_ts_short_state = np.nan # Clear stop state for short
                    # Check for immediate long entry if trend flipped
                    if current_trend_is_up:
                        in_long_state = True
                        current_ts_long_state = current_close - (current_atr * atr_multiplier)
                else:
                    # Update (trail) short trailing stop
                    candidate_stop = current_close + (current_atr * atr_multiplier)
                    current_ts_short_state = min(current_ts_short_state, candidate_stop)
            
            # If not in any position (e.g., after an exit without flip, or was initially flat and ATR just became valid)
            if not in_long_state and not in_short_state:
                if current_trend_is_up:
                    in_long_state = True
                    current_ts_long_state = current_close - (current_atr * atr_multiplier)
                else: # Trend is down or flat
                    in_short_state = True
                    current_ts_short_state = current_close + (current_atr * atr_multiplier)

        # Store current states for the current bar 'i'
        if in_long_state:
            trailing_stop_long_arr[i] = current_ts_long_state
            in_long_arr[i] = True
            trailing_stop_short_arr[i] = np.nan # Ensure other stop is NaN
            in_short_arr[i] = False
        elif in_short_state:
            trailing_stop_short_arr[i] = current_ts_short_state
            in_short_arr[i] = True
            trailing_stop_long_arr[i] = np.nan # Ensure other stop is NaN
            in_long_arr[i] = False
        else: # Not in any position
            in_long_arr[i] = False
            in_short_arr[i] = False
            trailing_stop_long_arr[i] = np.nan
            trailing_stop_short_arr[i] = np.nan
            
    return trailing_stop_long_arr, trailing_stop_short_arr, in_long_arr, in_short_arr


class ATRTrailingStops(BaseStrategy):
    """
    ATR Trailing Stops Strategy with Integrated Risk Management.

    This strategy uses the Average True Range (ATR) as a volatility measure to set
    dynamic trailing stop levels for trend-following trades. The main logic is as follows:

    1. ATR Calculation:
       - True Range (TR) is computed as:
             TR = max(high - low, |high - previous_close|, |low - previous_close|)
       - ATR is then calculated using an exponential moving average with smoothing factor
         alpha = 1/atr_period, replicating Wilder's smoothing method (adjust=False).

    2. Trend Determination:
       - A Simple Moving Average (SMA) of the close price over a specified trend_period acts as a trend filter.
       - When close > SMA, the trend is considered upward (favoring long positions).
       - Otherwise, a downward trend is assumed (favoring short positions).

    3. Trailing Stop Mechanism:
       - For long trades:
         - Entry is initiated when the trend becomes up.
         - The trailing stop is set as:
               trailing_stop_long = current_close - (atr × atr_multiplier)
         - On subsequent bars the stop is updated as the maximum of
               previous_stop and (current_close - (atr × atr_multiplier)).
         - An exit is triggered if the low price breaches the previous trailing stop.
       - For short trades:
         - Entry is initiated when the trend turns down.
         - The trailing stop is set as:
               trailing_stop_short = current_close + (atr × atr_multiplier)
         - On subsequent bars the stop is updated as the minimum of
               previous_stop and (current_close + (atr × atr_multiplier)).
         - An exit is triggered if the high price breaches the previous trailing stop.

    4. Signal Generation:
       - A buy signal (signal = 1) is generated when a new long is initiated.
       - A sell signal (signal = -1) is generated when a new short is initiated.
       - An exit signal (signal = 0) is generated when the trailing stop is breached.
       - A signal strength metric is computed based on the normalized difference between
         the close price and the corresponding trailing stop level.

    5. Integration with Risk Management:
       - After signal generation, the signals are passed to the RiskManager which applies
         stop-loss, take-profit, slippage, and transaction cost adjustments.
       - The final output DataFrame contains detailed performance metrics (such as position,
         realized returns, cumulative returns, and risk management actions) to allow further
         downstream calculations (e.g., Sharpe ratio, maximum drawdown).

    Args:
        ticker (str or List[str]): Stock ticker symbol or list of tickers.
        start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
        end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
        initial_position (int): Starting trading position (default is 0: no position,
                                  1 for long, -1 for short).
        latest_only (bool): If True, return only the latest signal per ticker for forecasting.

    Strategy-specific parameters via `params` (with defaults):
        - atr_period (int): Lookback period for ATR calculation (default: 14).
        - atr_multiplier (float): Multiplier for ATR to set the stop distance (default: 3.0).
        - trend_period (int): Period for computing SMA for trend detection (default: 20).
        - stop_loss_pct (float): Stop-loss percentage for risk management (default: 0.05).
        - take_profit_pct (float): Take-profit percentage for risk management (default: 0.10).
        - trailing_stop_pct (float): Trailing stop percentage for risk management (default: 0.0).
        - slippage_pct (float): Slippage percentage for risk management (default: 0.001).
        - transaction_cost_pct (float): Transaction cost percentage for risk management (default: 0.001).
        - long_only (bool): If True, restricts trading to long positions only (default: True).

    Returns:
        pd.DataFrame: DataFrame with the following key columns:
            - open, high, low, close, volume: Price data.
            - atr, sma: Calculated ATR and SMA.
            - trailing_stop_long, trailing_stop_short: Dynamic trailing stop levels.
            - signal: Trading signal (1 for buy, -1 for sell, 0 for exit).
            - signal_strength: Normalized indicator of signal confidence.
            - position: Updated position after applying risk management.
            - return: Realized trade return upon exit events.
            - cumulative_return: Cumulative return from closed trades.
            - rm_action: Indicator of the risk management exit action.
            - Additional risk management metrics provided by the RiskManager.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the ATR Trailing Stops strategy with specified parameters.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Strategy-specific parameters.
        """
        default_params = {
            'atr_period': 14,
            'atr_multiplier': 3.0,
            'trend_period': 20,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }
        if params:
            default_params.update(params)
        super().__init__(db_config, default_params)

    def generate_signals(self, ticker: Union[str, List[str]], start_date: Optional[str] = None,
                         end_date: Optional[str] = None, initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals using ATR-based trailing stops with integrated risk adjustments.

        Retrieves historical price data, calculates ATR and trend (SMA), computes dynamic
        trailing stop levels, and generates entry/exit signals. Signals are then processed
        by the RiskManager.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (default: 0).
            latest_only (bool): If True, returns only the latest signal per ticker.

        Returns:
            pd.DataFrame: DataFrame containing trading signals and risk-managed performance metrics.
        """
        # Determine required lookback for indicators
        # Add a buffer to ensure enough data for initial NaNs in SMA/ATR
        required_indicator_periods = max(int(self.params['atr_period']), int(self.params['trend_period']))
        min_records_needed = required_indicator_periods + 10 
        default_lookback_total = required_indicator_periods + 50 # Sufficient buffer for get_historical_prices

        if start_date or end_date:
            price_data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
        else:
            price_data = self.get_historical_prices(ticker, lookback=default_lookback_total)


        if isinstance(ticker, str):
            if not self._validate_data(price_data, min_records=min_records_needed):
                self.logger.warning(f"Insufficient data for {ticker} to generate signals. "
                                    f"Need at least {min_records_needed} records.")
                return pd.DataFrame()
            groups = [(ticker, price_data)]
        else:
            if price_data.empty:
                self.logger.warning("Price data is empty for provided tickers.")
                return pd.DataFrame()
            # GroupBy preserves the multi-index structure if level=0 is used.
            # list() is used to materialize the groups.
            groups = list(price_data.groupby(level=0, group_keys=False))


        signal_dfs = []

        for ticker, df_ticker_group in groups:
            # For multi-ticker, df_ticker_group is already data for a single ticker
            # It will have 'date' as index if groupby was on level=0 and original had MultiIndex
            # If original was single ticker, df_ticker_group is the original DataFrame
            
            # Ensure data is sorted by date if not already (should be from get_historical_prices)
            df_ticker_group = df_ticker_group.sort_index() 
            
            # Reset index if it's a MultiIndex extract; keeps 'date' as index for single ticker.
            # We need a simple DateTimeIndex for subsequent operations per ticker.
            if isinstance(df_ticker_group.index, pd.MultiIndex):
                 current_df = df_ticker_group.reset_index(level=0, drop=True)
            else:
                 current_df = df_ticker_group.copy() # Use a copy to avoid modifying original group data

            if not self._validate_data(current_df, min_records=min_records_needed):
                self.logger.warning(f"Insufficient data for {ticker} after grouping. "
                                    f"Need at least {min_records_needed} records. Skipping.")
                continue


            # Calculate indicators
            atr = self._calculate_atr(current_df, period=int(self.params['atr_period']))
            sma = current_df['close'].rolling(window=int(self.params['trend_period']), min_periods=int(self.params['trend_period'])).mean()
            # trend_up = (current_df['close'] > sma).astype(np.bool_) # Incorrect NaN handling
            
            # Corrected trend_up calculation
            trend_up_series = current_df['close'] > sma
            # Where sma is NaN, trend is undefined. Treat as 'not up-trend' for this logic.
            # Or, you could forward-fill sma for a short period if appropriate for your strategy.
            trend_up = trend_up_series.fillna(False).astype(np.bool_)

            # Initialize DataFrame for signals
            df_signals = pd.DataFrame(index=current_df.index)
            df_signals['open'] = current_df['open']
            df_signals['high'] = current_df['high']
            df_signals['low'] = current_df['low']
            df_signals['close'] = current_df['close']
            df_signals['volume'] = current_df['volume']
            df_signals['atr'] = atr
            df_signals['sma'] = sma

            # Use Numba-optimized function for core trailing stop logic
            (
                ts_long,
                ts_short,
                in_long_status, # Numba returns np.bool_ arrays
                in_short_status
            ) = _calculate_stops_and_positions_nb(
                df_signals['close'].to_numpy(dtype=np.float64),
                df_signals['high'].to_numpy(dtype=np.float64),
                df_signals['low'].to_numpy(dtype=np.float64),
                df_signals['atr'].to_numpy(dtype=np.float64),
                trend_up.to_numpy(dtype=np.bool_), # Already boolean
                float(self.params['atr_multiplier'])
            )
            df_signals['trailing_stop_long'] = ts_long
            df_signals['trailing_stop_short'] = ts_short
            
            # These are already boolean numpy arrays from Numba, direct assignment is fine
            # and should result in boolean dtype Series in pandas.
            df_signals['in_long'] = in_long_status
            df_signals['in_short'] = in_short_status


            # Generate signals from changes in position flags
            df_signals['signal'] = 0 # Default to no signal

            # Handle shifted series to avoid Downcasting FutureWarning from fillna
            shifted_in_long = df_signals['in_long'].shift(1)
            prev_in_long = shifted_in_long.copy() # Make a copy to safely modify
            prev_in_long[shifted_in_long.isna()] = False # Fill NA with False (bool)
            prev_in_long = prev_in_long.astype(bool)

            shifted_in_short = df_signals['in_short'].shift(1)
            prev_in_short = shifted_in_short.copy()
            prev_in_short[shifted_in_short.isna()] = False
            prev_in_short = prev_in_short.astype(bool)

            # Entry signals
            long_entry_mask = df_signals['in_long'] & ~prev_in_long
            short_entry_mask = df_signals['in_short'] & ~prev_in_short
            df_signals.loc[long_entry_mask, 'signal'] = 1
            df_signals.loc[short_entry_mask, 'signal'] = -1

            # Strategy-driven exit signals (signal=0)
            long_exit_mask = ~df_signals['in_long'] & prev_in_long
            short_exit_mask = ~df_signals['in_short'] & prev_in_short
            df_signals.loc[long_exit_mask | short_exit_mask, 'signal'] = 0

            # Compute signal strength
            atr_multiplied = (df_signals['atr'] * self.params['atr_multiplier'])
            # Replace 0 with NaN in divisor to prevent ZeroDivisionError, result will be NaN
            divisor_long = atr_multiplied.replace(0, np.nan)
            divisor_short = atr_multiplied.replace(0, np.nan)
            
            df_signals['signal_strength'] = 0.0 # Initialize with float

            mask_long_signal = df_signals['signal'] == 1
            df_signals.loc[mask_long_signal, 'signal_strength'] = \
                (df_signals.loc[mask_long_signal, 'close'] - df_signals.loc[mask_long_signal, 'trailing_stop_long']) / divisor_long[mask_long_signal]
            
            mask_short_signal = df_signals['signal'] == -1
            df_signals.loc[mask_short_signal, 'signal_strength'] = \
                (df_signals.loc[mask_short_signal, 'trailing_stop_short'] - df_signals.loc[mask_short_signal, 'close']) / divisor_short[mask_short_signal]

            # Fix for chained assignment warning and fill NaNs that arose from division
            df_signals['signal_strength'] = df_signals['signal_strength'].fillna(0.0)
            df_signals['signal_strength'] = df_signals['signal_strength'].clip(lower=0.0) # Strength is non-negative


            if self.params['long_only']:
                short_signal_indices = df_signals['signal'] == -1
                df_signals.loc[short_signal_indices, 'signal_strength'] = 0.0
                df_signals.loc[short_signal_indices, 'signal'] = 0


            df_signals.drop(columns=['in_long', 'in_short'], inplace=True)
            df_signals['ticker'] = ticker
            signal_dfs.append(df_signals)

        if not signal_dfs:
            self.logger.warning("No signals generated for any ticker.")
            return pd.DataFrame()

        final_signals = pd.concat(signal_dfs)
        # Ensure 'date' is part of the index for multi-ticker, or just index for single.
        # Original code did: final_signals.reset_index().set_index(['ticker', 'date']).sort_index()
        # If 'date' was 'index' from reset_index(), it gets renamed to 'date'.
        # This assumes the original index from current_df.index was the date.
        if 'date' not in final_signals.columns and final_signals.index.name == 'date':
            # This can happen if only one ticker, 'ticker' column added, index is 'date'
             final_signals = final_signals.reset_index().set_index(['ticker', 'date']).sort_index()
        elif 'ticker' in final_signals.index.names and 'date' in final_signals.index.names:
            # Already has ['ticker', 'date'] index from multi-ticker processing
             final_signals = final_signals.sort_index()
        else: # General case to ensure consistency
             final_signals = final_signals.reset_index()
             # df_signals originally has a 'date' index. After concat, 'date' is the index.
             # 'ticker' is a column. We need to make ['ticker', 'date'] the index.
             if 'index' in final_signals.columns and 'date' not in final_signals.columns : # from reset_index() default
                 final_signals = final_signals.rename(columns={'index': 'date'})
             final_signals = final_signals.set_index(['ticker', 'date']).sort_index()


        if latest_only:
            if not final_signals.empty:
                # .tail(1) per group is robust for getting the last row
                final_signals = final_signals.groupby(level='ticker', group_keys=False).tail(1)
            else:
                self.logger.info("Final signals DataFrame is empty before 'latest_only' filter.")
                return pd.DataFrame()

        if final_signals.empty:
             self.logger.info("Final signals DataFrame is empty after 'latest_only' filter or initially.")
             return pd.DataFrame()

        rm = RiskManager(
            stop_loss_pct=self.params['stop_loss_pct'],
            take_profit_pct=self.params['take_profit_pct'],
            slippage_pct=self.params['slippage_pct'],
            transaction_cost_pct=self.params['transaction_cost_pct']
        )
        final_signals_with_rm = rm.apply(final_signals, initial_position=initial_position)

        return final_signals_with_rm

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate the Average True Range (ATR) from historical OHLC data.

        True Range (TR) = max(high - low, |high - previous_close|, |low - previous_close|)
        ATR is an exponential moving average of TR (Wilder's smoothing).

        Args:
            price_data (pd.DataFrame): DataFrame with 'high', 'low', 'close' prices.
            period (int): Lookback period for ATR calculation.

        Returns:
            pd.Series: Calculated ATR values.
        """
        if not all(col in price_data.columns for col in ['high', 'low', 'close']):
            self.logger.error("Price data for ATR calculation must contain 'high', 'low', and 'close' columns.")
            # Return an empty series or series of NaNs with the same index to prevent downstream errors
            return pd.Series(np.nan, index=price_data.index, name='atr') 
        
        if len(price_data) < 1: # Not period, because shift(1) needs at least 1
             self.logger.warning(f"Not enough data for ATR calculation (need at least 1 row for shift, have {len(price_data)}).")
             return pd.Series(np.nan, index=price_data.index, name='atr')


        high = price_data['high']
        low = price_data['low']
        # Ensure prev_close calculation doesn't fail on very short data
        prev_close = price_data['close'].shift(1) if len(price_data) > 0 else pd.Series(np.nan, index=price_data.index)


        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        true_range = pd.concat([tr1.astype(float), tr2.astype(float), tr3.astype(float)], axis=1).max(axis=1, skipna=False)
        
        # min_periods for ewm should be `period` for standard ATR.
        # If len(true_range) < period, ewm will produce all NaNs, which is correct.
        atr = true_range.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        
        return atr.rename('atr')