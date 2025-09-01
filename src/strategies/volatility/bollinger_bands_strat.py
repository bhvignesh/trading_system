# trading_system/src/strategies/volatility/bollinger_bands_strat.py

"""
Bollinger Bands Strategy with Integrated Risk Management Component.

This strategy implements a mean-reversion trading strategy using Bollinger Bands.
For each ticker the following steps are performed in a fully vectorized fashion:

1. Retrieve historical price data (close, high, low) from the database.  
2. Compute rolling indicators:
   - SMA = rolling simple moving average of the close price.
   - STD = rolling standard deviation of the close price.
   - Upper Band = SMA + (_std_dev_ × STD)
   - Lower Band = SMA - (_std_dev_ × STD)
   - %B indicator = (close - Lower Band) / (Upper Band - Lower Band)
3. Generate raw trading signals:
   - Long entry (signal = 1): if yesterday’s close > yesterday’s lower band and today’s close < today’s lower band.
   - Short entry (signal = -1): if yesterday’s close < yesterday’s upper band and today’s close > today’s upper band.
     When in long-only mode short signals are removed.
4. Filter out duplicate consecutive signals to ensure only distinct trade entries.
5. Apply risk management via an external RiskManager component that integrates stop-loss, 
   take-profit, slippage, and transaction cost adjustments, and computes the realized and cumulative returns.
6. The strategy supports both backtesting (by providing start_date and end_date) and forecasting (using a minimal lookback and latest_only option).
7. When multiple tickers are provided, the calculations are grouped by ticker and the latest signal is computed per ticker.

Outputs:
    A DataFrame with the following columns:
      - 'close', 'high', 'low'     : Price references.
      - 'sma'                      : Rolling simple moving average.
      - 'upper_bb'                 : Upper Bollinger Band.
      - 'lower_bb'                 : Lower Bollinger Band.
      - 'signal_strength'          : Normalized indicator (%B).
      - 'signal'                   : Raw trading signal (1, -1, or 0).
      - 'position'                 : Updated trading position after risk management.
      - 'return'                   : Realized return on exit (if an exit event occurs).
      - 'cumulative_return'        : Cumulative return via risk-managed trading.
      - 'exit_type'                : Reason for exiting a trade (stop_loss, take_profit, signal_exit, or none).

Args:
    ticker (str or List[str]): Stock ticker symbol or list of tickers.
    start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
    end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
    initial_position (int): Starting trading position (default=0).
    latest_only (bool): If True, returns only the last bar (per ticker for multi-ticker scenarios).

Strategy-specific parameters provided via `params` (with defaults):
    - window (int): Lookback period for SMA and standard deviation (default: 20).
    - std_dev (float): Multiplier for STD to set the Bollinger Bands (default: 2.0).
    - long_only (bool): If True, only long trades are allowed (default: True).
    - stop_loss_pct (float): Stop loss percentage (default: 0.05).
    - take_profit_pct (float): Take profit percentage (default: 0.10).
    - trailing_stop_pct (float): Trailing stop percentage (default: 0.0).
    - slippage_pct (float): Slippage percentage for execution adjustments (default: 0.001).
    - transaction_cost_pct (float): Transaction cost as a fraction (default: 0.001).

Outputs:
    pd.DataFrame: DataFrame containing price data, computed indicators, raw signals, 
    and risk-managed performance metrics for backtesting or forecasting.
"""

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class BollingerBandsStrategy(BaseStrategy):
    """
    BollingerBandsStrategy implements a mean-reversion trading strategy based on Bollinger Bands.
    
    It calculates the rolling mean (SMA), rolling standard deviation (STD), and sets 
    the Bollinger Bands as SMA ± (std_dev × STD). The %B indicator is computed as a normalized
    measure of the closing price's distance from the lower band. Trading signals are triggered 
    on crossovers of the price with the Bollinger Bands. Risk management is applied via the RiskManager
    to adjust for slippage, transaction costs, stop-loss, and take-profit rules.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the BollingerBandsStrategy class with risk management and indicator parameters.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Dictionary of strategy-specific parameters.
        """
        super().__init__(db_config, params)
        self.window = int(self.params.get('window', 20))
        self.std_dev = self.params.get('std_dev', 2.0)
        self.long_only = self.params.get('long_only', True)
        # Risk management parameters:
        self.stop_loss_pct = self.params.get('stop_loss_pct', 0.05)
        self.take_profit_pct = self.params.get('take_profit_pct', 0.10)
        self.trailing_stop_pct = self.params.get('trailing_stop_pct', 0.0)
        self.slippage_pct = self.params.get('slippage_pct', 0.001)
        self.transaction_cost_pct = self.params.get('transaction_cost_pct', 0.001)

    def generate_signals(self, 
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals and apply risk management adjustments for the specified ticker(s).

        This function retrieves historical daily prices, computes Bollinger Bands indicators 
        (SMA, STD, upper & lower bands), and the %B indicator (normalized signal strength).  
        Signals are generated when the close price crosses below the lower band (triggering a long entry)
        or above the upper band (triggering a short entry) while filtering out duplicate consecutive signals.
        The external RiskManager is then applied to adjust the signals for stop-loss, take-profit, slippage, 
        and transaction costs. The result is a DataFrame with risk-managed performance metrics.

        Args:
            ticker (str or List[str]): A single stock ticker or a list of tickers.
            start_date (str, optional): Backtesting start date (YYYY-MM-DD).
            end_date (str, optional): Backtesting end date (YYYY-MM-DD).
            initial_position (int): Starting position (default 0; 1 for long, -1 for short).
            latest_only (bool): If True, returns only the last bar for each ticker (or overall, for a single ticker).

        Returns:
            pd.DataFrame: DataFrame including price data, computed indicators, raw signals, and 
                        risk-managed performance metrics (e.g., 'position', 'return', 'cumulative_return', 'exit_type').
        """
        # Retrieve historical price data; use a lookback that is at least 2×window if no dates provided.
        prices = self.get_historical_prices(
            ticker,
            from_date=start_date,
            to_date=end_date,
            lookback=self.window * 2 if not (start_date or end_date) else None
        )
        
        if not self._validate_data(prices, min_records=self.window + 1):
            self.logger.error("Not enough data for computing indicators")
            return pd.DataFrame()

        # Check if processing single ticker (index is DatetimeIndex) or multiple tickers (MultiIndex)
        multi_ticker = isinstance(ticker, list) or ('ticker' in prices.index.names)

        if multi_ticker:
            # Compute rolling indicators for each ticker.
            prices['sma'] = prices.groupby(level='ticker')['close'].transform(
                lambda x: x.rolling(self.window, min_periods=self.window).mean()
            )
            prices['std'] = prices.groupby(level='ticker')['close'].transform(
                lambda x: x.rolling(self.window, min_periods=self.window).std()
            )
            prices['upper_bb'] = prices['sma'] + prices['std'] * self.std_dev
            prices['lower_bb'] = prices['sma'] - prices['std'] * self.std_dev
            prices['percent_b'] = (prices['close'] - prices['lower_bb']) / (prices['upper_bb'] - prices['lower_bb'])
            # Compute shifted values for each ticker group.
            prices['close_shift'] = prices.groupby(level='ticker')['close'].shift(1)
            prices['lower_bb_shift'] = prices.groupby(level='ticker')['lower_bb'].shift(1)
            prices['upper_bb_shift'] = prices.groupby(level='ticker')['upper_bb'].shift(1)
            # Generate raw signals across tickers.
            crossed_below = (prices['close_shift'] > prices['lower_bb_shift']) & (prices['close'] < prices['lower_bb'])
            crossed_above = (prices['close_shift'] < prices['upper_bb_shift']) & (prices['close'] > prices['upper_bb'])
            raw_signals = pd.Series(0, index=prices.index)
            raw_signals[crossed_below] = 1
            raw_signals[crossed_above] = -1
            # Filter out duplicate consecutive signals for each ticker.
            prev_signals = raw_signals.replace(0, np.nan).groupby(level='ticker').ffill().groupby(level='ticker').shift(1).fillna(0)
            signals = raw_signals.where(raw_signals != prev_signals, 0)
        else:
            # Single ticker processing.
            prices['sma'] = prices['close'].rolling(self.window, min_periods=self.window).mean()
            prices['std'] = prices['close'].rolling(self.window, min_periods=self.window).std()
            prices['upper_bb'] = prices['sma'] + prices['std'] * self.std_dev
            prices['lower_bb'] = prices['sma'] - prices['std'] * self.std_dev
            prices['percent_b'] = (prices['close'] - prices['lower_bb']) / (prices['upper_bb'] - prices['lower_bb'])
            prices['close_shift'] = prices['close'].shift(1)
            prices['lower_bb_shift'] = prices['lower_bb'].shift(1)
            prices['upper_bb_shift'] = prices['upper_bb'].shift(1)
            crossed_below = (prices['close_shift'] > prices['lower_bb_shift']) & (prices['close'] < prices['lower_bb'])
            crossed_above = (prices['close_shift'] < prices['upper_bb_shift']) & (prices['close'] > prices['upper_bb'])
            raw_signals = pd.Series(0, index=prices.index)
            raw_signals[crossed_below] = 1
            raw_signals[crossed_above] = -1
            # Remove duplicate consecutive signals.
            prev_signals = raw_signals.replace(0, np.nan).ffill().shift(1).fillna(0)
            signals = raw_signals.where(raw_signals != prev_signals, 0)

        # Build the signals DataFrame that holds indicators and prices.
        signals_df = prices[['close', 'high', 'low', 'sma', 'upper_bb', 'lower_bb', 'percent_b']].copy()
        signals_df.rename(columns={'percent_b': 'signal_strength'}, inplace=True)
        if self.long_only:
            # Remove short signals if long-only mode is enabled.
            signals[signals == -1] = 0
        signals_df['signal'] = signals

        # Drop rows with missing indicator values.
        signals_df = signals_df.dropna()

        if signals_df.empty:
            self.logger.warning("Signals DataFrame is empty after indicator computation")
            return pd.DataFrame()

        # Apply risk management adjustments (stop-loss, take profit, slippage, transaction costs).
        risk_manager = RiskManager(
            stop_loss_pct=self.stop_loss_pct,
            take_profit_pct=self.take_profit_pct,
            trailing_stop_pct=self.trailing_stop_pct,
            slippage_pct=self.slippage_pct,
            transaction_cost_pct=self.transaction_cost_pct
        )
        results = risk_manager.apply(signals_df, initial_position)

        # If latest signal is required, return only the last row per ticker (or overall for single ticker).
        if latest_only:
            if multi_ticker:
                # For multi-ticker, group by 'ticker' (assumed to be part of the MultiIndex) and take the last date.
                results = results.reset_index(level='date')
                results = results.groupby('ticker', group_keys=False).last()
            else:
                results = results.tail(1)
        return results