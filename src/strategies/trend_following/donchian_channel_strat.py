"""
Donchian Channel Breakout Strategy with Integrated Risk Management

This strategy calculates entry and exit channels based on historical HIGH and LOW prices.
Mathematically, for each time t (for a given ticker):

  • Entry channels:
      Upper entry level: Uₑ(t) = max(high[t-nₑ:t-1])
      Lower entry level: Lₑ(t) = min(low[t-nₑ:t-1])
  • Exit channels:
      Upper exit level: Uₓ(t) = max(high[t-nₓ:t-1])
      Lower exit level: Lₓ(t) = min(low[t-nₓ:t-1])

Signals are generated as follows:
  - Long entry (signal = 1): if close(t) > Uₑ(t) and the previous position is flat or short.
  - Short entry (signal = -1): if close(t) < Lₑ(t) and the previous position is flat or long.
  - Exit long: if previously long (position == 1) and close(t) < Lₓ(t).
  - Exit short: if previously short (position == -1) and close(t) > Uₓ(t).

An optional ATR filter is applied so that new entries occur only if the channel width,
W(t) = Uₑ(t) - Lₑ(t), is greater than an ATR-based threshold.
  
Risk management is integrated via an external RiskManager component. When raw signals
are generated, the RiskManager adjusts the entry price to account for slippage and transaction
costs and then applies stop-loss and take-profit rules. The output DataFrame includes both the 
raw and risk-managed signals, positions, realized returns, and cumulative returns, allowing for 
further downstream analysis such as Sharpe ratio and maximum drawdown computations.

The strategy supports both backtesting (using provided start and end dates) and forecasting
(by returning only the latest signal when latest_only=True). It also handles a list of tickers as inputs,
processing them in a vectorized groupwise fashion.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Union, List
from src.strategies.base_strat import BaseStrategy
from src.database.config import DatabaseConfig
from src.strategies.risk_management import RiskManager
import numba  # Assumed to be available in the environment


class DonchianChannelStrategy(BaseStrategy):
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Donchian Channel strategy with parameters for channel and risk management.

        Args:
            db_config (DatabaseConfig): Database configuration for retrieving data.
            params (dict, optional): Dictionary of strategy-specific parameters. Available keys:
                - 'lookback_period': Overall lookback period for the strategy (default: 20).
                - 'entry_lookback': Lookback period for calculating entry channels (default: lookback_period).
                - 'exit_lookback': Lookback period for calculating exit channels (default: lookback_period).
                - 'use_atr_filter': Whether to filter entries using ATR (default: False).
                - 'atr_period': Period for ATR calculation (default: 14).
                - 'atr_threshold': Multiplier to compare the channel width against ATR (default: 1.0).
                - 'stop_loss_pct': Stop loss percentage for risk management (default: 0.05).
                - 'take_profit_pct': Take profit percentage for risk management (default: 0.10).
                - 'trail_stop_pct': Trailing stop percentage for risk management (default: 0.0).
                - 'slippage_pct': Estimated slippage percentage (default: 0.001).
                - 'transaction_cost_pct': Transaction cost percentage per trade (default: 0.001).
                - 'long_only': If True, the strategy will only generate long signals (default: True).
        """
        default_params = {
            'lookback_period': 20,
            'entry_lookback': None,
            'exit_lookback': None,
            'use_atr_filter': False,
            'atr_period': 14,
            'atr_threshold': 1.0,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trail_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }

        if params:
            default_params.update(params)

        # If entry or exit lookback is not specified, use the overall lookback period.
        if default_params['entry_lookback'] is None:
            default_params['entry_lookback'] = int(default_params['lookback_period'])
        if default_params['exit_lookback'] is None:
            default_params['exit_lookback'] = int(default_params['lookback_period'])

        super().__init__(db_config, default_params)

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate trading signals using the Donchian Channel breakout method with risk management.

        The method performs the following steps:
          1. Retrieves historical price data (OHLC and volume) from the database for one or more tickers.
          2. Computes rolling channels:
             - Entry levels: highest high and lowest low over 'entry_lookback' periods (shifted by 1 period).
             - Exit levels: highest high and lowest low over 'exit_lookback' periods (shifted by 1 period).
          3. Optionally calculates the Average True Range (ATR) and applies a filter to allow entry only
             if the channel width is sufficiently wide.
          4. Using a stateful procedure (iterating in a vectorized manner per ticker), determines daily signals:
             - Long entry (signal = 1) when close > upper entry level and previous position ≤ 0.
             - Short entry (signal = -1) when close < lower entry level and previous position ≥ 0.
             - Exits are triggered if a long position’s price falls below the lower exit level or a short
               position’s price rises above the upper exit level.
          5. Applies the RiskManager to adjust for stop loss, take profit, slippage, and transaction costs.
          6. For backtesting, the full processed DataFrame is returned. If latest_only is True, only the last
             row (per ticker) is returned to facilitate forecasting.

        Args:
            ticker (str or List[str]): Ticker symbol or list of ticker symbols.
            start_date (str, optional): Start date in 'YYYY-MM-DD' format for the backtest.
            end_date (str, optional): End date in 'YYYY-MM-DD' format for the backtest.
            initial_position (int): Starting position (default 0 means flat).
            latest_only (bool): If True, returns only the last row per ticker for forecasting purposes.

        Returns:
            pd.DataFrame: DataFrame that includes OHLC prices, calculated channels, raw trading signals, risk-managed
            signals, positions, returns, and exit action types. The full historical series facilitates
            downstream performance metric calculations (Sharpe ratio, max drawdown, etc.).
        """
        # Define the minimum number of records needed to compute the rolling channels
        min_required = max(
            int(self.params['lookback_period']),
            int(self.params['entry_lookback']),
            int(self.params['exit_lookback'])
        ) + 10

        # Retrieve price data from the database (vectorized retrieval for one or multiple tickers)
        price_data = self.get_historical_prices(
            ticker,
            lookback=252,  # pulling sufficient historical data (about one trading year)
            from_date=start_date,
            to_date=end_date
        )

        # Process data for single or multiple tickers.
        if isinstance(ticker, str):
            if not self._validate_data(price_data, min_records=min_required):
                self.logger.warning(f"Insufficient data for {ticker} to generate Donchian signals")
                return pd.DataFrame()
            signals = self._generate_signals_for_df(price_data, initial_position)
        else:
            # Expecting a multi-index (ticker, date)
            def process_group(group):
                if not self._validate_data(group, min_records=min_required):
                    return pd.DataFrame()
                return self._generate_signals_for_df(group, initial_position)
            signals = price_data.groupby(level='ticker', group_keys=False).apply(process_group)

        # Integrate risk management adjustments.
        # The RiskManager applies stop-loss, take-profit, slippage, and transaction cost adjustments.
        risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05),
            take_profit_pct=self.params.get('take_profit_pct', 0.10),
            trailing_stop_pct=self.params.get('trail_stop_pct', 0.0),
            slippage_pct=self.params.get('slippage_pct', 0.001),
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001)
        )
        signals = risk_manager.apply(signals, initial_position=initial_position)

        # If forecasting is desired (latest_only), return only the most recent signal row(s)
        if latest_only:
            if 'ticker' in signals.index.names:
                signals = signals.groupby(level='ticker', group_keys=False).apply(lambda df: df.iloc[[-1]])
            else:
                signals = signals.iloc[[-1]]

        return signals

    def _generate_signals_for_df(self, price_data: pd.DataFrame, initial_position: int = 0) -> pd.DataFrame:
        """
        Compute the raw trading signals for a single ticker using the Donchian Channel approach.

        For each date, the function computes:
          - Upper/Lower channels for entry and exit (using shifted rolling windows).
          - The optional ATR (Average True Range) if filtering is enabled.
          - A stateful signal generation that checks:
              • Long entry: if close > upper entry level and previous position ≤ 0.
              • Short entry: if close < lower entry level and previous position ≥ 0.
              • Exit long: if previously long and close < lower exit level.
              • Exit short: if previously short and close > upper exit level.
          The stateful logic is computed by iterating (per ticker) using NumPy arrays for speed.

        Args:
            price_data (pd.DataFrame): DataFrame with historical OHLC and volume data.
            initial_position (int): The starting position (default is 0 for no position).

        Returns:
            pd.DataFrame: A DataFrame with additional columns:
                'upper_channel', 'middle_channel', 'lower_channel', 'atr',
                'signal' (raw trading signal), and 'position' (the updated position).
        """
        entry_lookback = int(self.params['entry_lookback'])
        exit_lookback = int(self.params['exit_lookback'])

        # Extract arrays for Numba computations
        index = price_data.index
        high_array = price_data['high'].values
        low_array = price_data['low'].values
        close_array = price_data['close'].values

        # Calculate rolling entry and exit levels using Numba (replicates rolling.max/min().shift(1))
        upper_entry_array = self._rolling_max_numba(high_array, entry_lookback)
        lower_entry_array = self._rolling_min_numba(low_array, entry_lookback)
        upper_exit_array = self._rolling_max_numba(high_array, exit_lookback)
        lower_exit_array = self._rolling_min_numba(low_array, exit_lookback)

        # Convert back to Series for consistency
        upper_entry = pd.Series(upper_entry_array, index=index)
        lower_entry = pd.Series(lower_entry_array, index=index)
        upper_exit = pd.Series(upper_exit_array, index=index)
        lower_exit = pd.Series(lower_exit_array, index=index)

        # Compute the middle channel (average of upper and lower entry levels)
        middle = (upper_entry + lower_entry) / 2

        # Calculate ATR and set an entry filter if enabled
        if self.params['use_atr_filter']:
            atr_array = self._calculate_atr_numba(high_array, low_array, close_array, period=int(self.params['atr_period']))
            atr = pd.Series(atr_array, index=index)
            channel_width = upper_entry - lower_entry
            atr_filter = channel_width > (atr * self.params['atr_threshold'])
        else:
            atr = pd.Series(0, index=price_data.index)
            atr_filter = pd.Series(True, index=price_data.index)

        # Initialize result DataFrame, copying available price columns and appending new ones.
        result = price_data.copy()
        result['upper_channel'] = upper_entry
        result['middle_channel'] = middle
        result['lower_channel'] = lower_entry
        result['atr'] = atr
        result['signal'] = 0
        result['position'] = 0

        # Convert series to NumPy arrays for fast indexing.
        upper_entry_array = upper_entry.values
        lower_entry_array = lower_entry.values
        upper_exit_array = upper_exit.values
        lower_exit_array = lower_exit.values
        atr_filter_array = atr_filter.values.astype(bool)  # Ensure boolean for Numba

        # Compute positions and signals using Numba-accelerated loop
        positions, signals = self._compute_signals_numba(
            close_array, upper_entry_array, lower_entry_array,
            upper_exit_array, lower_exit_array, atr_filter_array, initial_position
        )

        result['position'] = positions
        result['signal'] = signals
        if self.params['long_only']:
            result['signal'] = result['signal'].clip(lower=0) 
        # Remove any rows with NaN values (due to rolling window calculations)
        return result.dropna()

    @staticmethod
    @numba.jit(nopython=True)
    def _rolling_max_numba(arr: np.ndarray, window: int) -> np.ndarray:
        """
        Numba-optimized rolling max, replicating Pandas rolling(window).max().shift(1).
        For each i, if i >= window, max(arr[i-window : i]), else NaN.
        """
        n = len(arr)
        result = np.full(n, np.nan, dtype=np.float64)
        for i in range(window, n):
            max_val = arr[i - window]
            for j in range(i - window + 1, i):
                if arr[j] > max_val:
                    max_val = arr[j]
            result[i] = max_val
        return result

    @staticmethod
    @numba.jit(nopython=True)
    def _rolling_min_numba(arr: np.ndarray, window: int) -> np.ndarray:
        """
        Numba-optimized rolling min, replicating Pandas rolling(window).min().shift(1).
        For each i, if i >= window, min(arr[i-window : i]), else NaN.
        """
        n = len(arr)
        result = np.full(n, np.nan, dtype=np.float64)
        for i in range(window, n):
            min_val = arr[i - window]
            for j in range(i - window + 1, i):
                if arr[j] < min_val:
                    min_val = arr[j]
            result[i] = min_val
        return result

    @staticmethod
    @numba.jit(nopython=True)
    def _compute_signals_numba(
        close: np.ndarray,
        upper_entry: np.ndarray,
        lower_entry: np.ndarray,
        upper_exit: np.ndarray,
        lower_exit: np.ndarray,
        atr_filter: np.ndarray,
        initial_position: int
    ) -> tuple:
        """
        Numba-accelerated function to compute positions and signals from arrays.
        """
        n = len(close)
        positions = np.empty(n, dtype=numba.int32)
        signals = np.zeros(n, dtype=numba.int32)
        positions[0] = initial_position

        for i in range(1, n):
            prev_pos = positions[i - 1]
            if (close[i] > upper_entry[i] and prev_pos <= 0 and atr_filter[i]):
                positions[i] = 1
                signals[i] = 1
            elif (close[i] < lower_entry[i] and prev_pos >= 0 and atr_filter[i]):
                positions[i] = -1
                signals[i] = -1
            elif (prev_pos == 1 and close[i] < lower_exit[i]):
                positions[i] = 0
                signals[i] = 0
            elif (prev_pos == -1 and close[i] > upper_exit[i]):
                positions[i] = 0
                signals[i] = 0
            else:
                positions[i] = prev_pos
                signals[i] = 0

        return positions, signals

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate the Average True Range (ATR) using Wilder's smoothing method.

        Args:
            price_data (pd.DataFrame): DataFrame with columns 'high', 'low', 'close'.
            period (int): Lookback period for ATR calculation.

        Returns:
            pd.Series: ATR values.
        """
        high = price_data['high'].values
        low = price_data['low'].values
        close = price_data['close'].values
        atr_array = self._calculate_atr_numba(high, low, close, period)
        return pd.Series(atr_array, index=price_data.index)

    @staticmethod
    @numba.jit(nopython=True)
    def _calculate_atr_numba(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
        """
        Numba-optimized ATR calculation using Wilder's method.
        """
        n = len(high)
        tr = np.empty(n, dtype=np.float64)
        atr = np.full(n, np.nan, dtype=np.float64)

        # Compute True Range (TR)
        for i in range(n):
            if i == 0:
                tr[i] = high[i] - low[i]  # No prev_close for i=0
            else:
                tr1 = high[i] - low[i]
                tr2 = abs(high[i] - close[i-1])
                tr3 = abs(low[i] - close[i-1])
                tr[i] = max(tr1, tr2, tr3)

        # Compute ATR using Wilder's EMA (starts after period)
        alpha = 1.0 / period
        if n >= period:
            atr[period-1] = np.mean(tr[:period])  # Initial SMA
            for i in range(period, n):
                atr[i] = atr[i-1] * (1 - alpha) + tr[i] * alpha

        return atr