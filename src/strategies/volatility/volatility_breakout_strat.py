# trading_system/src/strategies/volatality/volatility_breakout.py

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class VolatilityBreakout(BaseStrategy):
    """
    Volatility Breakout Strategy with Integrated Risk Management.

    This strategy generates trading signals based on price breakouts above and below
    volatility-defined bands computed from historical price data. For a given price series Pₜ, 
    the following are calculated over a lookback period (L):

        Center Line (μₜ) = Moving average of close prices over L days.
        Volatility (σₜ) = Rolling standard deviation of close prices over L days, or the Average 
                         True Range (ATR) computed over an alternative period if 'use_atr' is True.
        Upper Band (Uₜ) = μₜ + (volatility_multiplier × σₜ)
        Lower Band (Lₜ) = μₜ - (volatility_multiplier × σₜ)

    Trading signals are defined as:
        • Buy Signal:  Generate signal = 1 if the close price exceeds the upper band (Pₜ > Uₜ).
        • Sell Signal: Generate signal = –1 if the close price falls below the lower band (Pₜ < Lₜ).
        • Otherwise, signal = 0.

    Signal Strength is computed to quantify the breakout magnitude. Specifically:
        • For a buy signal, signal_strength = (Pₜ – Uₜ) / (σₜ + ε)
        • For a sell signal, signal_strength = (Pₜ – Lₜ) / (σₜ + ε)
    where ε is a small constant (e.g. 1e-6) to avoid division by zero.

    Risk Management:
        Risk management is applied via the RiskManager component. After generating raw signals, 
        the RiskManager adjusts entry prices for slippage and transaction costs, determines stop-loss 
        and take-profit thresholds, and calculates the realized and cumulative returns.

    The strategy supports both backtesting (using start_date and end_date parameters) and forecasting 
    (by using latest_only to return only the most recent signal) and can handle a list of ticker symbols 
    with vectorized calculations.

    Args:
        db_config (DatabaseConfig): Database configuration instance.
        params (dict, optional): Strategy-specific parameters. Supported parameters include:
            - 'lookback_period' (int): Lookback period for volatility and center line calculation (default: 20).
            - 'volatility_multiplier' (float): Multiplier for the volatility to compute bands (default: 2.0).
            - 'use_atr' (bool): Use ATR as the volatility measure instead of standard deviation (default: False).
            - 'atr_period' (int): Lookback period for ATR calculation (default: 14).
            - 'stop_loss_pct' (float): Stop-loss percentage (default: 0.05).
            - 'take_profit_pct' (float): Take-profit percentage (default: 0.10).
            - 'trailing_stop_pct' (float): Trailing stop percentage (default: 0.0).
            - 'slippage_pct' (float): Slippage percentage (default: 0.001).
            - 'transaction_cost_pct' (float): Transaction cost percentage (default: 0.001).
            - 'long_only' (bool): If True, restricts trading to long positions only (default: True).

    Returns:
        pd.DataFrame: DataFrame containing the following columns:
            • 'open', 'high', 'low', 'close', 'volume'       : Price data for risk management.
            • 'center_line'                                   : Rolling mean of the close price.
            • 'volatility'                                    : Rolling standard deviation or ATR.
            • 'upper_band', 'lower_band'                      : Volatility bands.
            • 'signal'                                        : Raw trading signal (1, -1 or 0).
            • 'signal_strength'                               : Normalized breakout magnitude.
            • Additional risk management columns: 'position', 'return',
               'cumulative_return', 'exit_type', etc.
        The index is a datetime index for a single ticker or a MultiIndex (ticker, date) for multiple tickers.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """Initialize the Volatility Breakout strategy with default and user-provided parameters."""
        default_params = {
            'lookback_period': 20,
            'volatility_multiplier': 2.0,
            'use_atr': False,
            'atr_period': 14,
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

    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals based on volatility breakout and apply risk management.

        Mathematically, for each price series Pₜ:
            • Center Line (μₜ) = rolling mean(Pₜ, L)
            • Volatility (σₜ) = rolling std(Pₜ, L)   [or ATR if 'use_atr' is True]
            • Upper Band (Uₜ) = μₜ + (volatility_multiplier × σₜ)
            • Lower Band (Lₜ) = μₜ – (volatility_multiplier × σₜ)

        Trading signals:
            • Signal = 1 (Buy)  if Pₜ > Uₜ.
            • Signal = –1 (Sell) if Pₜ < Lₜ.
            • Otherwise, signal = 0.

        Signal strength quantifies the relative breakout:
            • For a buy:  (Pₜ – Uₜ) / (σₜ + ε)
            • For a sell: (Pₜ – Lₜ) / (σₜ + ε)
        where ε is a small constant (1e-6).

        Risk management is then applied to adjust the raw signals using stop-loss, take-profit,
        slippage, and transaction cost adjustments via the RiskManager.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Backtesting start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtesting end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (default: 0).
            latest_only (bool): If True, returns only the most recent row for each ticker.

        Returns:
            pd.DataFrame: DataFrame with price data, computed technical indicators, raw signals,
            normalized signal strength, and risk-managed trading performance metrics.
        """
        # Retrieve historical price data from the database.
        price_data = self.get_historical_prices(ticker, lookback=252, from_date=start_date, to_date=end_date)

        # Validate that there is sufficient data for computation.
        if ("ticker" in price_data.index.names and 
            price_data.groupby(level='ticker').size().min() < self.params['lookback_period'] + 10) or \
           ("ticker" not in price_data.index.names and 
            len(price_data) < self.params['lookback_period'] + 10):
            self.logger.warning("Insufficient data to generate volatility breakout signals for provided ticker(s).")
            return pd.DataFrame()

        lb = int(self.params['lookback_period'])
        vm = self.params['volatility_multiplier']
        eps = 1e-6  # To prevent division by zero

        # Calculate indicators in a vectorized fashion.
        if "ticker" in price_data.index.names:
            # Multi-ticker: use groupby transform to align indices.
            if self.params['use_atr']:
                volatility = price_data.groupby(level='ticker').apply(
                    lambda group: self._calculate_atr(group, period=self.params['atr_period'])
                )
                # Ensure volatility has the same (ticker, date) index.
                volatility = volatility.sort_index()
            else:
                volatility = price_data.groupby(level='ticker')['close'].transform(
                    lambda x: x.rolling(lb, min_periods=lb).std()
                )
            center_line = price_data.groupby(level='ticker')['close'].transform(
                lambda x: x.rolling(lb, min_periods=lb).mean()
            )
        else:
            # Single ticker processing.
            if self.params['use_atr']:
                volatility = self._calculate_atr(price_data, period=int(self.params['atr_period']))
            else:
                volatility = price_data['close'].rolling(window=lb, min_periods=lb).std()
            center_line = price_data['close'].rolling(window=lb, min_periods=lb).mean()

        # Compute the upper and lower volatility bands.
        upper_band = center_line + (vm * volatility)
        lower_band = center_line - (vm * volatility)

        # Construct the result DataFrame with price and indicator data.
        result = price_data.copy()
        result['center_line'] = center_line
        result['volatility'] = volatility
        result['upper_band'] = upper_band
        result['lower_band'] = lower_band

        # Generate raw trading signals based on price breakouts.
        result['signal'] = 0
        result.loc[result['close'] > result['upper_band'], 'signal'] = 1
        result.loc[result['close'] < result['lower_band'], 'signal'] = -1

        # Compute the signal strength (normalized breakout magnitude).
        result['signal_strength'] = 0.0
        result.loc[result['signal'] == 1, 'signal_strength'] = (
            (result['close'] - result['upper_band']) / (result['volatility'] + eps)
        )
        result.loc[result['signal'] == -1, 'signal_strength'] = (
            (result['close'] - result['lower_band']) / (result['volatility'] + eps)
        )

        # Apply long-only constraint if specified.
        if self.params['long_only']:
            result.loc[result['signal'] == -1, 'signal'] = 0

        # Remove observations with NaN values due to the rolling calculations.
        result = result.dropna()

        # Integrate risk management (stop-loss, take-profit, slippage, transaction cost) using RiskManager.
        risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05),
            take_profit_pct=self.params.get('take_profit_pct', 0.10),
            trailing_stop_pct=self.params.get('trailing_stop_pct', 0.0),
            slippage_pct=self.params.get('slippage_pct', 0.001),
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001)
        )
        result = risk_manager.apply(result, initial_position=initial_position)

        # For forecasting, if latest_only is True, return only the most recent row per ticker.
        if latest_only:
            if "ticker" in result.index.names:
                result = result.groupby(level='ticker').tail(1)
            else:
                result = result.tail(1)

        return result

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate the Average True Range (ATR) as a measure of volatility.

        ATR is computed as the exponential moving average of the True Range (TR), where TR is defined as:
            TR = max[
                (current high - current low),
                abs(current high - previous close),
                abs(current low - previous close)
            ]

        Args:
            price_data (pd.DataFrame): DataFrame with columns 'high', 'low', and 'close'.
            period (int): Lookback period for the ATR calculation (default: 14).

        Returns:
            pd.Series: Series of ATR values computed using Wilder's smoothing method.
        """
        high = price_data['high']
        low = price_data['low']
        prev_close = price_data['close'].shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = true_range.ewm(alpha=1/period, min_periods=period).mean()
        return atr