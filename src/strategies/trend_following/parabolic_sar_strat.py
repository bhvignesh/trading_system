# trading_system/src/strategies/trend_following/parabolic_sar.py

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class ParabolicSARStrategy(BaseStrategy):
    """
    Parabolic SAR Strategy with Integrated Risk Management Component.

    This strategy implements the Parabolic Stop and Reverse (SAR) indicator to generate
    trading signals for identifying trend reversals. The PSAR indicator is computed
    recursively.

    Core PSAR Logic:
        - For an uptrend:
            PSAR[i] = PSAR[i-1] + AF[i-1]*(EP[i-1] - PSAR[i-1])
            PSAR is then bounded by the minimum of the previous two lows.
            A trend reversal to downtrend occurs if the current low falls below the PSAR.
        - For a downtrend:
            PSAR[i] = PSAR[i-1] - AF[i-1]*(PSAR[i-1] - EP[i-1])
            PSAR is then bounded by the maximum of the previous two highs.
            A trend reversal to uptrend occurs if the current high exceeds the PSAR.
    Where:
        - AF (acceleration factor) starts at 'initial_af', increments by 'af_step'
          on new extreme price (EP), up to 'max_af'.
        - EP is the highest high (uptrend) or lowest low (downtrend) during the current trend.

    An optional ATR (Average True Range) filter can be applied to validate signal significance.
    Signals are generated on trend reversals:
        - Buy signal (1): Trend reverses from downtrend to uptrend (and passes ATR filter if enabled).
        - Sell signal (-1): Trend reverses from uptrend to downtrend (and passes ATR filter if enabled).
                          If 'long_only' is True, sell signals become 0 (exit).

    Signal strength is computed as:
        signal_strength = signal * (|close - sar| / (atr + 1e-9))
    This normalizes the distance from close to SAR by ATR. ATR is always calculated for this purpose.

    Risk management (stop loss, take profit, slippage, transaction costs) is applied
    via the integrated RiskManager class.

    Args:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict, optional): Strategy-specific parameters. Refer to `default_params`
                                 in `__init__` for available options and defaults.

    Outputs from `generate_signals`:
        A DataFrame (DatetimeIndex, with 'ticker' column if multiple tickers) containing:
            - open, high, low, close, volume: Original price/volume data.
            - sar: Calculated Parabolic SAR value.
            - trend: Trend indicator (1 for uptrend, -1 for downtrend).
            - atr: Average True Range value.
            - signal: Trading signal (1 for buy, -1 for sell, 0 for hold/exit).
            - signal_strength: Normalized signal strength.
            - And columns added by RiskManager: 'position', 'return', 'cumulative_return', 'exit_type'.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Parabolic SAR strategy.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Strategy-specific parameters.
        """
        default_params = {
            'initial_af': 0.02,
            'max_af': 0.2,
            'af_step': 0.02,
            'use_atr_filter': False,
            'atr_period': 14,
            'atr_threshold': 1.0,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0, # Disabled by default
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }
        # Update default_params with any user-provided params
        # self.params will be set by BaseStrategy.__init__
        current_params = default_params.copy()
        if params:
            current_params.update(params)
        
        super().__init__(db_config, current_params)

        # Minimum data length required for calculations (primarily driven by ATR period)
        self.min_data_len = max(2, self.params.get('atr_period', 14))


    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals based on the Parabolic SAR indicator.

        Retrieves historical price data, computes PSAR, trend, ATR, generates signals,
        computes signal strength, and applies risk management rules.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Backtest start date ('YYYY-MM-DD').
            end_date (str, optional): Backtest end date ('YYYY-MM-DD').
            initial_position (int): Starting trading position (default=0).
            latest_only (bool): If True, returns only the most recent data row(s).

        Returns:
            pd.DataFrame: DataFrame containing price data, indicators, signals,
                          signal strength, and risk-managed trade metrics.
                          For multiple tickers, includes a 'ticker' column.
                          Index is DatetimeIndex ('date').
        """
        lookback = None
        if not (start_date or end_date): # If no date range, use lookback
            lookback = max(252, self.min_data_len + 50) # Ensure enough data plus buffer

        price_data = self.get_historical_prices(
            ticker,
            lookback=lookback,
            from_date=start_date,
            to_date=end_date
        )

        if price_data.empty:
            self.logger.warning("No price data retrieved for Parabolic SAR. Returning empty DataFrame.")
            return pd.DataFrame()

        # --- SINGLE TICKER PROCESSING ---
        if isinstance(ticker, str):
            if not self._validate_data(price_data, min_records=self.min_data_len):
                self.logger.warning(f"Insufficient data for ticker {ticker} (need {self.min_data_len} records).")
                return pd.DataFrame()
            result_df = self._process_single_ticker(price_data.copy(), initial_position) # Pass copy

        # --- MULTI-TICKER PROCESSING ---
        else:
            all_results_list = []
            if not isinstance(price_data.index, pd.MultiIndex):
                self.logger.error("Price data for multiple ticker is not MultiIndexed as expected. Ticker: %s", ticker)
                return pd.DataFrame()

            for ticker_name, group_df in price_data.groupby(level=0): # Group by first level (ticker)
                # group_df has DatetimeIndex, name of group_df is ticker_name
                group_df = group_df.droplevel(0) if isinstance(group_df.index, pd.MultiIndex) else group_df # Ensure simple DatetimeIndex

                if not self._validate_data(group_df, min_records=self.min_data_len):
                    self.logger.warning(f"Insufficient data for ticker {ticker_name} (need {self.min_data_len} records). Skipping.")
                    continue
                
                processed_df = self._process_single_ticker(group_df.copy(), initial_position) # Pass copy
                
                if not processed_df.empty:
                    processed_df['ticker'] = ticker_name # Add ticker column
                    all_results_list.append(processed_df)
            
            if not all_results_list:
                self.logger.warning("No signals generated for any ticker.")
                return pd.DataFrame()

            result_df = pd.concat(all_results_list)
            result_df.index.name = 'date' # Ensure index is named 'date'
            
            result_df = result_df.reset_index().set_index(['ticker', 'date']).sort_index()
            
            # Sort by ticker first, then date, for consistent output order, though optimizer re-groups
            if 'ticker' in result_df.columns:
                 result_df = result_df.sort_values(by=['ticker', 'date'])
            else: # Should not happen if multiple tickers were processed
                 result_df = result_df.sort_index()


        # --- HANDLE LATEST_ONLY ---
        if latest_only:
            if result_df.empty:
                return result_df # Return empty if no results
            if isinstance(ticker, str): # Single ticker input
                result_df = result_df.iloc[[-1]]
            else: # List of tickers input, result_df has 'ticker' column
                result_df = result_df.groupby('ticker', group_keys=False).tail(1)
        
        return result_df


    def _process_single_ticker(self, df: pd.DataFrame, initial_position: int) -> pd.DataFrame:
        """
        Process a single ticker's data: calculate PSAR, ATR, signals, and apply risk management.
        
        Args:
            df (pd.DataFrame): DataFrame with DatetimeIndex and OHLCV columns.
            initial_position (int): Starting position for RiskManager.
            
        Returns:
            pd.DataFrame: DataFrame with indicators, signals, and risk-managed outputs.
        """
        # Ensure DataFrame has enough records for internal calculations (already validated before call)
        if len(df) < self.min_data_len:
             self.logger.warning(f"DataFrame for single ticker processing has {len(df)} rows, less than required {self.min_data_len}. PSAR/ATR might be unreliable.")
             # Potentially return empty or handle, but _validate_data should catch this earlier.
             # If it gets here, proceed with caution.

        # 1. Calculate Parabolic SAR and Trend
        sar_values, trend_indicator = self._calculate_psar(
            high=df['high'].values,
            low=df['low'].values,
            initial_af=self.params['initial_af'],
            max_af=self.params['max_af'],
            af_step=self.params['af_step']
        )

        # 2. Calculate ATR (Average True Range) - always calculate for signal_strength
        atr_values = self._calculate_atr(df, period=self.params['atr_period'])

        # Create result DataFrame
        result = df.copy()
        result['sar'] = sar_values
        result['trend'] = trend_indicator
        result['atr'] = atr_values # Store actual ATR

        # 3. Apply ATR Filter (if enabled)
        if self.params['use_atr_filter']:
            # Price movement relative to close, compared against ATR relative to close
            price_movement_pct = (df['high'] - df['low']) / (df['close'] + 1e-9) # Avoid div by zero
            atr_pct = result['atr'] / (df['close'] + 1e-9)
            atr_filter_active = price_movement_pct > (atr_pct * self.params['atr_threshold'])
            # Where atr_filter_active is NaN (e.g. due to NaN in ATR), treat as not passing filter
            atr_filter = atr_filter_active.fillna(False)
        else:
            atr_filter = pd.Series(True, index=df.index) # No ATR filter, all signals pass

        # 4. Generate Trading Signals
        result['signal'] = 0
        # Trend change: 1 to -1 is diff of -2. -1 to 1 is diff of 2.
        trend_change = result['trend'].diff() # First value will be NaN

        # Buy signal: trend flips from -1 to 1 (diff = 2) AND passes ATR filter
        buy_condition = (trend_change == 2) & atr_filter
        result.loc[buy_condition, 'signal'] = 1

        # Sell signal: trend flips from 1 to -1 (diff = -2) AND passes ATR filter
        sell_condition = (trend_change == -2) & atr_filter
        if self.params['long_only']:
            # For long_only, a "sell" signal event (trend reversal downwards) means exit current long.
            # The signal '0' will instruct RiskManager to exit.
            # If already neutral, signal remains 0.
            pass # Signal remains 0 for sell_condition if long_only=True
        else: # Allow short selling
            result.loc[sell_condition, 'signal'] = -1
        
        # Ensure initial signal is 0 if trend_change is NaN
        result['signal'] = result['signal'].fillna(0)


        # 5. Compute Signal Strength
        # Normalized by ATR. Add epsilon to ATR to prevent division by zero if ATR is momentarily zero.
        # signal_strength can be NaN if ATR is NaN for initial periods.
        signal_strength_val = np.abs(result['close'] - result['sar']) / (result['atr'] + 1e-9)
        result['signal_strength'] = result['signal'] * signal_strength_val
        result['signal_strength'] = result['signal_strength'].fillna(0.0) # Fill NaN strength with 0

        # 6. Apply Risk Management
        # RiskManager expects 'signal', 'open', 'high', 'low', 'close'
        # Ensure 'signal' column is int type for RiskManager if it makes assumptions
        result['signal'] = result['signal'].astype(int)

        risk_manager = RiskManager(
            stop_loss_pct=self.params['stop_loss_pct'],
            take_profit_pct=self.params['take_profit_pct'],
            trailing_stop_pct=self.params['trailing_stop_pct'],
            slippage_pct=self.params['slippage_pct'],
            transaction_cost_pct=self.params['transaction_cost_pct']
        )
        # RiskManager returns a DataFrame with additional columns:
        # 'position', 'return', 'cumulative_return', 'exit_type'
        result_with_rm = risk_manager.apply(result, initial_position=initial_position)

        return result_with_rm


    def _calculate_psar(self,
                        high: np.ndarray,
                        low: np.ndarray,
                        initial_af: float = 0.02,
                        max_af: float = 0.2,
                        af_step: float = 0.02) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate Parabolic SAR values and trend.

        Args:
            high (np.ndarray): Array of high prices.
            low (np.ndarray): Array of low prices.
            initial_af (float): Initial acceleration factor.
            max_af (float): Maximum acceleration factor.
            af_step (float): Step for acceleration factor.

        Returns:
            tuple: (sar_values (np.ndarray), trend (np.ndarray: 1 for uptrend, -1 for downtrend))
        """
        length = len(high)
        if length == 0:
            return np.array([]), np.array([])
        
        sar = np.zeros(length)
        trend = np.zeros(length)  # 1 for uptrend, -1 for downtrend
        # ep (extreme point) and af (acceleration factor) are per-bar, so need arrays
        ep_arr = np.zeros(length) 
        af_arr = np.zeros(length)

        # Initial values for the first bar (index 0)
        # Assume downtrend initially, common practice. SAR at high, EP at low.
        trend[0] = -1
        sar[0] = high[0]
        ep_arr[0] = low[0]
        af_arr[0] = initial_af

        # Loop from the second bar
        for i in range(1, length):
            # Carry forward previous values
            current_sar = sar[i-1]
            current_trend = trend[i-1]
            prev_ep = ep_arr[i-1]
            prev_af = af_arr[i-1]

            if current_trend == 1:  # Uptrend
                # Calculate SAR for today
                sar[i] = current_sar + prev_af * (prev_ep - current_sar)
                # SAR cannot be higher than the low of the previous two periods
                if i >= 2:
                    sar[i] = min(sar[i], low[i-1], low[i-2])
                else: # i == 1
                    sar[i] = min(sar[i], low[i-1])


                # Check for trend reversal
                if low[i] < sar[i]:
                    trend[i] = -1  # Switch to downtrend
                    sar[i] = prev_ep  # SAR is the previous EP (highest high of uptrend)
                    ep_arr[i] = low[i] # New EP is current low
                    af_arr[i] = initial_af
                else:  # Continue uptrend
                    trend[i] = 1
                    ep_arr[i] = prev_ep
                    af_arr[i] = prev_af
                    if high[i] > prev_ep:  # New extreme high
                        ep_arr[i] = high[i]
                        af_arr[i] = min(prev_af + af_step, max_af)
            
            else:  # Downtrend (trend == -1)
                # Calculate SAR for today
                sar[i] = current_sar - prev_af * (current_sar - prev_ep)
                # SAR cannot be lower than the high of the previous two periods
                if i >= 2:
                    sar[i] = max(sar[i], high[i-1], high[i-2])
                else: # i == 1
                    sar[i] = max(sar[i], high[i-1])

                # Check for trend reversal
                if high[i] > sar[i]:
                    trend[i] = 1  # Switch to uptrend
                    sar[i] = prev_ep  # SAR is the previous EP (lowest low of downtrend)
                    ep_arr[i] = high[i] # New EP is current high
                    af_arr[i] = initial_af
                else:  # Continue downtrend
                    trend[i] = -1
                    ep_arr[i] = prev_ep
                    af_arr[i] = prev_af
                    if low[i] < prev_ep:  # New extreme low
                        ep_arr[i] = low[i]
                        af_arr[i] = min(prev_af + af_step, max_af)
        
        return sar, trend

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate Average True Range (ATR).

        Args:
            price_data (pd.DataFrame): DataFrame with 'high', 'low', 'close' columns.
            period (int): Lookback period for ATR.

        Returns:
            pd.Series: ATR values.
        """
        if not all(col in price_data.columns for col in ['high', 'low', 'close']):
            self.logger.error("Missing HLC columns for ATR calculation.")
            return pd.Series(np.nan, index=price_data.index)
            
        high_s = price_data['high']
        low_s = price_data['low']
        close_s = price_data['close']
        
        prev_close = close_s.shift(1)
        
        tr1 = high_s - low_s
        tr2 = (high_s - prev_close).abs()
        tr3 = (low_s - prev_close).abs()
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1, skipna=False)
        
        # Using Wilder's smoothing for ATR (alpha = 1/period)
        # min_periods=period ensures that we only get ATR values after enough data
        atr = true_range.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        
        return atr