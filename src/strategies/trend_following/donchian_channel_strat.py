# trading_system/src/stategies/trend_following/donchian_channel_strat.py

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

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


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
             - Long entry (signal = 1) when close > upper entry level and the previous position is ≤ 0.
             - Short entry (signal = -1) when close < lower entry level and the previous position is ≥ 0.
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

        # Calculate rolling entry and exit levels (shifted by 1 period to avoid lookahead bias)
        upper_entry = price_data['high'].rolling(window=entry_lookback).max().shift(1)
        lower_entry = price_data['low'].rolling(window=entry_lookback).min().shift(1)
        upper_exit = price_data['high'].rolling(window=exit_lookback).max().shift(1)
        lower_exit = price_data['low'].rolling(window=exit_lookback).min().shift(1)

        # Compute the middle channel (average of upper and lower entry levels)
        middle = (upper_entry + lower_entry) / 2

        # Calculate ATR and set an entry filter if enabled
        if self.params['use_atr_filter']:
            atr = self._calculate_atr(price_data, period=int(self.params['atr_period']))
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
        close_array = result['close'].values
        upper_entry_array = upper_entry.values
        lower_entry_array = lower_entry.values
        upper_exit_array = upper_exit.values
        lower_exit_array = lower_exit.values
        atr_filter_array = atr_filter.values

        # Prepare arrays to store computed positions and signals.
        positions = np.empty(len(result), dtype=int)
        signals = np.zeros(len(result), dtype=int)
        positions[0] = initial_position

        # Iterate through each row (starting from the second row) to update position and signal.
        for i in range(1, len(result)):
            prev_pos = positions[i - 1]
            if (close_array[i] > upper_entry_array[i] and prev_pos <= 0 and atr_filter_array[i]):
                positions[i] = 1
                signals[i] = 1
            elif (close_array[i] < lower_entry_array[i] and prev_pos >= 0 and atr_filter_array[i]):
                positions[i] = -1
                signals[i] = -1
            elif (prev_pos == 1 and close_array[i] < lower_exit_array[i]):
                positions[i] = 0
                signals[i] = 0
            elif (prev_pos == -1 and close_array[i] > upper_exit_array[i]):
                positions[i] = 0
                signals[i] = 0
            else:
                positions[i] = prev_pos
                signals[i] = 0

        result['position'] = positions
        result['signal'] = signals
        if self.params['long_only']:
            result['signal'] = result['signal'].clip(lower=0) 
        # Remove any rows with NaN values (due to rolling window calculations)
        return result.dropna()

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate the Average True Range (ATR) using Wilder's smoothing method.

        Args:
            price_data (pd.DataFrame): DataFrame with columns 'high', 'low', 'close'.
            period (int): Lookback period for ATR calculation.

        Returns:
            pd.Series: ATR values.
        """
        high = price_data['high']
        low = price_data['low']
        prev_close = price_data['close'].shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1/period, min_periods=period).mean()
        return atr