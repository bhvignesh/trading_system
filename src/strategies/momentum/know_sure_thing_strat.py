"""
Enhanced Know Sure Thing (KST) Momentum Strategy

This module implements the KST strategy with integrated risk management,
optimized vectorized calculations, and flexible backtesting/forecasting capabilities.

Mathematically, the KST indicator is computed as a weighted sum of smoothed
Rate-of-Change (ROC) values:
    KST = Σ (w_i * SMA(ROC(close, period_i), sma_period_i))

A signal line is computed as a simple moving average of the KST indicator, and
a trade signal is triggered when the KST line crosses its signal line. The signal
strength (KST minus signal line) is optimized for use in downstream portfolio
optimization.

The strategy integrates a RiskManager that applies stop-loss, take-profit,
slippage, and transaction cost adjustments in a vectorized manner. The complete
output DataFrame is provided for further performance metric calculation (e.g.,
Sharpe ratio, maximum drawdown).
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager
from src.database.config import DatabaseConfig

class KSTStrategy(BaseStrategy):
    """
    Know Sure Thing (KST) strategy using multiple ROC components with risk management.

    Hyperparameters:
        roc_periods : List of integers representing lookback periods for ROC calculations.
        sma_periods : List of integers representing the periods for smoothing each ROC.
        signal_period : Integer period to compute the signal line (SMA of KST).
        kst_weights : List of numeric weights to apply to each smoothed ROC component.
        risk_params : Dictionary containing risk management parameters (stop_loss_pct, take_profit_pct,
                      slippage_pct, transaction_cost_pct).
        long_only : Boolean flag to restrict trading to long positions only.

    The strategy computes the KST indicator, its signal line, and then generates a position:
      - +1 for a bullish crossover
      - -1 for a bearish crossover
      - The continuous signal is generated by forward-filling the most recent nonzero signal.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the KSTStrategy with database settings and hyperparameters.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Strategy-specific parameters. If not provided,
                                     default hyperparameters are used.
        """
        default_params = {
            'roc_periods': [10, 15, 20, 30],
            'sma_periods': [10, 10, 10, 15],
            'signal_period': 9,
            'kst_weights': [1, 2, 3, 4],
            'long_only': True,
            'risk_params': {
                'stop_loss_pct': 0.05,
                'take_profit_pct': 0.10,
                'slippage_pct': 0.001,
                'transaction_cost_pct': 0.001
            }
        }
        params = params or {}
        for key, value in default_params.items():
            params.setdefault(key, value)
            
        super().__init__(db_config, params)
        self._validate_parameters()
        self.risk_manager = RiskManager(**params['risk_params'])

    def _validate_parameters(self):
        """
        Validate that all required parameters are provided and are positive integers where applicable.
        
        Raises:
            ValueError: If required parameters are missing or invalid.
        """
        required = ['roc_periods', 'sma_periods', 'signal_period', 'kst_weights', 'risk_params']
        for param in required:
            if param not in self.params:
                raise ValueError(f"Missing required parameter: {param}")
                
        if len(self.params['roc_periods']) != 4 or len(self.params['sma_periods']) != 4:
            raise ValueError("roc_periods and sma_periods must contain exactly 4 values")
            
        if any(p <= 0 for p in self.params['roc_periods'] + self.params['sma_periods'] + [self.params['signal_period']]):
            raise ValueError("All periods must be positive integers")

    def generate_signals(self, 
                         ticker: str,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate the KST trading signals, apply risk management, and return the performance data.

        Args:
            ticker (str): Stock ticker symbol.
            start_date (str, optional): Backtest start date (YYYY-MM-DD).
            end_date (str, optional): Backtest end date (YYYY-MM-DD).
            initial_position (int): Starting position (0 for none, 1 for long, -1 for short).
            latest_only (bool): If True, only the latest signal (EOD forecast) is returned.

        Returns:
            pd.DataFrame: DataFrame containing price, indicator, risk-managed position,
                          returns, cumulative return, exit event type, KST indicator,
                          signal line, and signal strength.
        """
        try:
            # Determine the necessary lookback to ensure all rolling calculations are valid.
            max_roc = max(self.params['roc_periods'])
            max_sma = max(self.params['sma_periods'])
            lookback = max_roc + max_sma + self.params['signal_period']

            # Fetch historical price data from the database.
            prices = self.get_historical_prices(
                ticker,
                lookback=lookback if latest_only else None,
                from_date=start_date,
                to_date=end_date
            )
            if not self._validate_data(prices, min_records=lookback):
                return pd.DataFrame()

            close = prices['close']

            # Compute the KST components using vectorized operations.
            kst_components = []
            for roc_len, sma_len, weight in zip(self.params['roc_periods'],
                                                self.params['sma_periods'],
                                                self.params['kst_weights']):
                roc = close.pct_change(roc_len) * 100
                smoothed = roc.rolling(sma_len, min_periods=sma_len).mean()
                kst_components.append(smoothed * weight)

            kst = pd.concat(kst_components, axis=1).sum(axis=1)
            signal_line = kst.rolling(self.params['signal_period'], min_periods=self.params['signal_period']).mean()

            # Generate raw trading signals based on KST crossing its signal line.
            signals = self._generate_signals(prices, kst, signal_line)
            signals['signal_strength'] = kst - signal_line

            # Apply risk management and compute trade returns.
            risk_managed = self.risk_manager.apply(signals, initial_position)

            return risk_managed.iloc[[-1]] if latest_only else risk_managed

        except Exception as e:
            self.logger.error(f"Error processing {ticker}: {str(e)}")
            raise

    def _generate_signals(self, prices: pd.DataFrame, kst: pd.Series, signal_line: pd.Series) -> pd.DataFrame:
        """
        Compute raw trading signals based on the crossover between KST and its signal line.
        
        Args:
            prices (pd.DataFrame): DataFrame containing price data including 'close', 'high', and 'low'.
            kst (pd.Series): The computed KST indicator.
            signal_line (pd.Series): The moving average of the KST indicator.
            
        Returns:
            pd.DataFrame: DataFrame with price data, KST indicator, signal line, and trading signals.
        """
        signals = pd.DataFrame({
            'close': prices['close'],
            'high': prices['high'],
            'low': prices['low'],
            'kst': kst,
            'signal_line': signal_line
        }, index=prices.index)

        signals['position'] = 0
        cross_above = (kst > signal_line) & (kst.shift(1) <= signal_line.shift(1))
        cross_below = (kst < signal_line) & (kst.shift(1) >= signal_line.shift(1))
        
        signals.loc[cross_above, 'position'] = 1

        if self.long_only:
            signals.loc[cross_below, 'position'] = 0
        else:
            signals.loc[cross_below, 'position'] = -1
        
        # Generate a continuous trading signal by forward-filling the nonzero signals.
        signals['signal'] = signals['position'].replace(0, np.nan).ffill().fillna(0)
        return signals