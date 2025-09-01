# src/strategies/momentum/awesome_oscillator_strat.py

"""
Awesome Oscillator Strategy with Integrated Risk Management Component.

This strategy implements the Awesome Oscillator indicator for generating trading signals,
position simulation, and profit metric computation. The Awesome Oscillator (AO) is defined as:
    AO = SMA_short - SMA_long,
where:
    SMA_short = Simple Moving Average of the median price over a short period,
    SMA_long  = Simple Moving Average of the median price over a long period,
    median_price = (high + low) / 2.

Trading signals:
    - A buy signal (signal = 1) is triggered when AO crosses upward through zero.
    - A sell signal (signal = -1) is triggered when AO crosses downward through zero.
      If operating in "long only" mode, sell signals are replaced with 0 (exit signals).

Signal strength is determined by normalizing the absolute AO value by the rolling standard
deviation of AO, i.e.:
    normalized_strength = |AO| / (rolling_std(AO) + 1e-6).

Risk Management is integrated via the external RiskManager component. When signals are generated,
the RiskManager adjusts the entry price by incorporating slippage and transaction costs and then:
    - Calculates stop-loss and take-profit thresholds.
    - Identifies exit events (due to stop-loss, take-profit, or signal reversal).
    - Computes the risk–managed (realized) trade return and cumulative return.

The strategy supports both backtesting (using a specific start and end date)
and forecasting (using a minimal lookback and returning only the latest signal).
It also supports vectorized processing for a list of tickers.

Outputs:
    A DataFrame containing:
      - 'open', 'close', 'high', 'low'    : Price references for risk management.
      - 'ao'                              : Awesome Oscillator value.
      - 'signal'                          : Trading signal (1 for buy, -1 for sell, 0 for hold/exit).
      - 'signal_strength'                 : Normalized signal strength.
      - 'daily_return'                    : Daily percentage change in close price.
      - 'strategy_return'                 : Return computed as daily_return multiplied by the previous day's position.
      - 'position'                        : Updated trading position after applying risk management.
      - 'rm_strategy_return'              : Realized trade return after risk management adjustments.
      - 'rm_cumulative_return'            : Cumulative return from the risk-managed trades.
      - 'rm_action'                       : Indicator of the risk management action taken.

Args:
    ticker (str or List[str]): Stock ticker symbol or list of tickers.
    start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
    end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
    initial_position (int): Starting trading position (default=0, meaning no position).
    latest_only (bool): If True, returns only the final row (per ticker in multi-ticker scenarios).

Strategy-specific parameters provided in `params` (with defaults):
    - 'short_period': Period for short SMA (default: 5).
    - 'long_period' : Period for long SMA (default: 34).
    - 'long_only'   : If True, only long positions are allowed (default: True).
    - 'stop_loss_pct'         : Stop loss percentage (default: 0.05).
    - 'take_profit_pct'       : Take profit percentage (default: 0.10).
    - 'trailing_stop_pct'     : Trailing stop percentage (default: 0.0).
    - 'slippage_pct'          : Slippage percentage (default: 0.001).
    - 'transaction_cost_pct'  : Transaction cost percentage (default: 0.001).
"""

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class AwesomeOscillatorStrategy(BaseStrategy):
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Awesome Oscillator Strategy.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (Optional[Dict]): Strategy parameters. Expected keys include:
                - 'short_period' (default: 5)
                - 'long_period' (default: 34)
                - 'long_only' (default: True)
                - 'stop_loss_pct' (default: 0.05)
                - 'take_profit_pct' (default: 0.10)
                - 'trail_stop_pct' (default: 0.0)
                - 'slippage_pct' (default: 0.001)
                - 'transaction_cost_pct' (default: 0.001)
        """
        default_params = {'short_period': 5, 'long_period': 34, 'long_only': True}
        params = params or default_params
        super().__init__(db_config, params)
        self.short_period = int(params.get('short_period', default_params['short_period']))
        self.long_period = int(params.get('long_period', default_params['long_period']))
        # Validate that short period is less than long period.
        if self.short_period >= self.long_period:
            raise ValueError("Short SMA period must be less than long SMA period.")
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize the RiskManager with risk parameters.
        risk_params = {
            'stop_loss_pct': params.get('stop_loss_pct', 0.05),
            'take_profit_pct': params.get('take_profit_pct', 0.10),
            'trailing_stop_pct': params.get('trailing_stop_pct', 0.0),
            'slippage_pct': params.get('slippage_pct', 0.001),
            'transaction_cost_pct': params.get('transaction_cost_pct', 0.001)
        }
        self.risk_manager = RiskManager(**risk_params)
        # Save long_only flag.
        self.long_only = params.get('long_only', True)

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False,
    ) -> pd.DataFrame:
        """
        Generate trading signals, compute performance metrics, and apply risk management.

        This function operates in two modes:
          - Backtesting: When start_date and end_date are provided, a full historical data slice is used.
          - Forecasting: When latest_only is True, only the most recent signal for each ticker is returned.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (default=0).
            latest_only (bool): If True, returns only the final row (per ticker for multi-ticker scenarios).

        Returns:
            pd.DataFrame: DataFrame containing signals and performance metrics, including:
                open, close, high, low, ao, signal, signal_strength,
                daily_return, strategy_return, position,
                rm_strategy_return, rm_cumulative_return, rm_action.
        """
        try:
            # Determine lookback buffer based on max SMA period.
            lookback_buffer = 2 * max(self.short_period, self.long_period)
            # Retrieve historical price data via the base class' get_historical_prices method.
            if start_date and end_date:
                data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date, lookback=lookback_buffer)
            else:
                data = self.get_historical_prices(ticker, lookback=lookback_buffer)
                data = data.sort_index()

            # If multiple tickers are provided, process each ticker group separately.
            if isinstance(ticker, list):
                signals_list = []
                for t, group in data.groupby(level=0):
                    if not self._validate_data(group, min_records=max(self.short_period, self.long_period)):
                        self.logger.warning(f"Insufficient data for {t}: required at least {max(self.short_period, self.long_period)} records.")
                        continue
                    # Calculate oscillator and signals for a single ticker.
                    sig = self._calculate_signals_single(group)
                    # Apply risk management to compute positions and returns.
                    sig = self.risk_manager.apply(sig, initial_position)
                    # Compute additional performance metrics.
                    sig['daily_return'] = sig['close'].pct_change().fillna(0)
                    sig['strategy_return'] = sig['daily_return'] * sig['position'].shift(1).fillna(0)
                    # Rename risk-managed return columns for clarity.
                    sig.rename(columns={'return': 'rm_strategy_return',
                                        'cumulative_return': 'rm_cumulative_return',
                                        'exit_type': 'rm_action'}, inplace=True)
                    # Insert ticker identifier as a column.
                    sig['ticker'] = t
                    signals_list.append(sig)
                if not signals_list:
                    return pd.DataFrame()
                signals = pd.concat(signals_list)
                # If latest_only is True, take only the last row per ticker.
                if latest_only:
                    signals = signals.groupby('ticker').tail(1)
            else:
                if not self._validate_data(data, min_records=max(self.short_period, self.long_period)):
                    self.logger.warning(f"Insufficient data for {ticker}: required at least {max(self.short_period, self.long_period)} records.")
                    return pd.DataFrame()
                # For a single ticker, calculate signals and apply risk management.
                signals = self._calculate_signals_single(data)
                signals = self.risk_manager.apply(signals, initial_position)
                signals['daily_return'] = signals['close'].pct_change().fillna(0)
                signals['strategy_return'] = signals['daily_return'] * signals['position'].shift(1).fillna(0)
                signals.rename(columns={'return': 'rm_strategy_return',
                                        'cumulative_return': 'rm_cumulative_return',
                                        'exit_type': 'rm_action'}, inplace=True)
                if latest_only:
                    signals = signals.iloc[[-1]].copy()
            return signals

        except Exception as e:
            self.logger.error(f"Error generating signals for {ticker}: {str(e)}")
            return pd.DataFrame()

    def _calculate_signals_single(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the Awesome Oscillator and corresponding trading signals for a single ticker.

        The Awesome Oscillator (AO) is computed as:
            ao = SMA(short_period) - SMA(long_period)
        where SMA is calculated using the median price:
            median_price = (high + low) / 2.

        Trading signals are generated based on AO zero-crossing:
          - Buy signal (1): When AO crosses upward through zero.
          - Sell signal (-1): When AO crosses downward through zero.
            If long_only is enabled, sell signals are replaced with 0 to only allow long positions.
        Signal strength is computed as:
            normalized_strength = |AO| / (rolling_std(AO) + 1e-6).

        Args:
            data (pd.DataFrame): Historical price data for a single ticker, with the columns:
                ['open', 'high', 'low', 'close'].

        Returns:
            pd.DataFrame: DataFrame with columns:
                ['open', 'close', 'high', 'low', 'ao', 'signal', 'signal_strength'].
        """
        # Ensure data is sorted by date.
        data = data.sort_index()
        df = data.copy()
        # Compute the median price and corresponding short and long SMAs.
        df['median_price'] = (df['high'] + df['low']) / 2
        df['short_sma'] = df['median_price'].rolling(window=self.short_period, min_periods=self.short_period).mean()
        df['long_sma'] = df['median_price'].rolling(window=self.long_period, min_periods=self.long_period).mean()
        # Calculate the Awesome Oscillator.
        df['ao'] = df['short_sma'] - df['long_sma']
        df = df.dropna(subset=['ao'])
        # Compute rolling standard deviation of AO for normalization.
        df['ao_std'] = df['ao'].rolling(window=self.short_period, min_periods=self.short_period).std()
        df['normalized_strength'] = df['ao'].abs() / (df['ao_std'] + 1e-6)
        # Generate trading signals at the zero-crossing points.
        buy_mask = (df['ao'] > 0) & (df['ao'].shift(1) <= 0)
        sell_mask = (df['ao'] < 0) & (df['ao'].shift(1) >= 0)
        df['signal'] = 0
        df.loc[buy_mask, 'signal'] = 1
        df.loc[sell_mask, 'signal'] = -1
        # Enforce long-only mode if specified.
        if self.long_only:
            df.loc[sell_mask, 'signal'] = 0
        # Set signal strength only at the crossing points.
        df['signal_strength'] = 0.0
        df.loc[buy_mask, 'signal_strength'] = df.loc[buy_mask, 'normalized_strength']
        df.loc[sell_mask, 'signal_strength'] = df.loc[sell_mask, 'normalized_strength']
        return df[['open', 'close', 'high', 'low', 'ao', 'signal', 'signal_strength']]