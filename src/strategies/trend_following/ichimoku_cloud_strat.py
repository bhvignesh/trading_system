# trading_system/src/strategies/trend_following/ichimoku_cloud.py

import pandas as pd
import numpy as np
from typing import Dict, Optional, Union, List
from src.strategies.base_strat import BaseStrategy
from src.database.config import DatabaseConfig
from src.strategies.risk_management import RiskManager
from numba import njit, prange

@njit
def numba_rolling_max(arr, window):
    """Numba-accelerated rolling max for a 1D array."""
    n = len(arr)
    result = np.full(n, np.nan)
    for i in prange(window - 1, n):
        max_val = arr[i - window + 1]
        for j in prange(i - window + 2, i + 1):
            if arr[j] > max_val:
                max_val = arr[j]
        result[i] = max_val
    return result

@njit
def numba_rolling_min(arr, window):
    """Numba-accelerated rolling min for a 1D array."""
    n = len(arr)
    result = np.full(n, np.nan)
    for i in prange(window - 1, n):
        min_val = arr[i - window + 1]
        for j in prange(i - window + 2, i + 1):
            if arr[j] < min_val:
                min_val = arr[j]
        result[i] = min_val
    return result

class IchimokuCloudStrategy(BaseStrategy):
    """
    Ichimoku Cloud Strategy with Integrated Risk Management Component.

    This strategy implements the Ichimoku Cloud (Ichimoku Kinko Hyo)
    indicator system. The indicator components are defined mathematically as:
    
        Tenkan-sen    = (Highest High + Lowest Low) / 2 over `tenkan_period`
                      (default 9)
        Kijun-sen     = (Highest High + Lowest Low) / 2 over `kijun_period`
                      (default 26)
        Senkou Span A = (Tenkan-sen + Kijun-sen) / 2, shifted forward by `displacement`
                      (default 26)
        Senkou Span B = (Highest High + Lowest Low) / 2 over `senkou_b_period`
                      (default 52), shifted forward by `displacement`
        Chikou Span   = Close price shifted backward by `displacement`
    
    Trading signals are generated as follows:
    
      - A long (buy) signal (signal = 1) is triggered when any of the following
        conditions occur (if enabled):
          • Tenkan-sen crosses above Kijun-sen.
          • Price crosses above Kijun-sen.
          • Price crosses above the upper bound of the cloud (max(Senkou Span A, Senkou Span B}}.
    
      - A short (sell) signal (signal = -1) is triggered when:
          • Tenkan-sen crosses below Kijun-sen.
          • Price crosses below Kijun-sen.
          • Price crosses below the lower bound of the cloud (min(Senkou Span A, Senkou Span B)).
    
    Risk Management is applied via the RiskManager class. It adjusts the entry price
    (accounting for slippage and transaction costs), computes stop-loss and take-profit
    thresholds, identifies exit events (also on signal reversal), and computes realized and
    cumulative returns.
    
    The strategy supports both backtesting (through start_date and end_date selections)
    and forecasting (using the latest available data). It also efficiently processes a list
    of tickers in a vectorized fashion.
    
    Args:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict, optional): Strategy-specific parameters with defaults:
            - 'tenkan_period': int, period for Tenkan-sen (default: 9)
            - 'kijun_period': int, period for Kijun-sen (default: 26)
            - 'senkou_b_period': int, period for Senkou Span B (default: 52)
            - 'displacement': int, shift period for Senkou Span A/B and Chikou Span (default: 26)
            - 'use_cloud_breakouts': bool, whether to use cloud breakout signals (default: True)
            - 'use_tk_cross': bool, whether to use TK cross signals (default: True)
            - 'use_price_cross': bool, whether to use price-Kijun cross signals (default: False)
            - 'stop_loss_pct': float, stop loss percentage (default: 0.05)
            - 'take_profit_pct': float, take profit percentage (default: 0.10)
            - 'trailing_stop_pct': float, trailing stop percentage (default: 0.0)
            - 'slippage_pct': float, estimated slippage as a fraction (default: 0.001)
            - 'transaction_cost_pct': float, transaction cost as a fraction (default: 0.001)
            - 'long_only': bool, whether to allow only long positions (default: True)
    
    Outputs:
        A pandas DataFrame containing, at a minimum:
            - 'open', 'high', 'low', 'close', 'volume': Price data
            - 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span': Ichimoku components
            - 'cloud_bullish': Boolean indicator if Senkou Span A > Senkou Span B
            - 'signal': Trading signal (1, -1, or 0)
            - Additional columns from RiskManager including 'position', 'return',
              'cumulative_return', and 'exit_type'.
    
        This output is designed to support downstream metrics computation such as the Sharpe ratio,
        maximum drawdown, and to provide a stable final daily signal for portfolio decisions.
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        default_params = {
            'tenkan_period': 9,
            'kijun_period': 26,
            'senkou_b_period': 52,
            'displacement': 26,
            'use_cloud_breakouts': True,
            'use_tk_cross': True,
            'use_price_cross': False,
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
    
    def _compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute Ichimoku indicators and signals on a single-ticker DataFrame with DatetimeIndex."""
        # Handle MultiIndex case (from groupby.apply)
        is_multi = isinstance(df.index, pd.MultiIndex)
        if is_multi:
            # Drop the 'ticker' level to get DatetimeIndex
            df = df.droplevel('ticker')
        
        # Ensure index is DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # Calculate Tenkan-sen (Conversion Line)
        tenkan_high = pd.Series(numba_rolling_max(df['high'].values, self.tenkan_period), index=df.index)
        tenkan_low = pd.Series(numba_rolling_min(df['low'].values, self.tenkan_period), index=df.index)
        df['tenkan_sen'] = (tenkan_high + tenkan_low) / 2
        
        # Calculate Kijun-sen (Base Line)
        kijun_high = pd.Series(numba_rolling_max(df['high'].values, self.kijun_period), index=df.index)
        kijun_low = pd.Series(numba_rolling_min(df['low'].values, self.kijun_period), index=df.index)
        df['kijun_sen'] = (kijun_high + kijun_low) / 2
        
        # Calculate Senkou Span A (Leading Span A)
        senkou_a = (df['tenkan_sen'] + df['kijun_sen']) / 2
        df['senkou_span_a'] = senkou_a.shift(self.displacement)
        
        # Calculate Senkou Span B (Leading Span B)
        senkou_b_high = pd.Series(numba_rolling_max(df['high'].values, self.senkou_b_period), index=df.index)
        senkou_b_low = pd.Series(numba_rolling_min(df['low'].values, self.senkou_b_period), index=df.index)
        senkou_b = (senkou_b_high + senkou_b_low) / 2
        df['senkou_span_b'] = senkou_b.shift(self.displacement)
        
        # Calculate Chikou Span (Lagging Span)
        df['chikou_span'] = df['close'].shift(-self.displacement)
        
        # Store previous values for crossover detection
        df['prev_tenkan'] = df['tenkan_sen'].shift(1)
        df['prev_kijun'] = df['kijun_sen'].shift(1)
        df['prev_close'] = df['close'].shift(1)
        
        # Calculate cloud boundaries
        cloud_top = df[['senkou_span_a', 'senkou_span_b']].max(axis=1)
        cloud_bottom = df[['senkou_span_a', 'senkou_span_b']].min(axis=1)
        df['prev_cloud_top'] = cloud_top.shift(1)
        df['prev_cloud_bottom'] = cloud_bottom.shift(1)
        
        # Compute cloud bullishness
        df['cloud_bullish'] = df['senkou_span_a'] > df['senkou_span_b']
        
        # Initialize signal column
        df['signal'] = 0
        
        # Generate TK Cross signals
        if self.params.get('use_tk_cross', True):
            tk_cross_up = (df['tenkan_sen'] > df['kijun_sen']) & (df['prev_tenkan'] <= df['prev_kijun'])
            tk_cross_down = (df['tenkan_sen'] < df['kijun_sen']) & (df['prev_tenkan'] >= df['prev_kijun'])
            df.loc[tk_cross_up, 'signal'] = 1
            if not self.params.get('long_only', True):
                df.loc[tk_cross_down, 'signal'] = -1
            else:
                df.loc[tk_cross_down, 'signal'] = 0
        
        # Generate Price-Kijun Cross signals
        if self.params.get('use_price_cross', False):
            price_cross_up = (df['close'] > df['kijun_sen']) & (df['prev_close'] <= df['prev_kijun'])
            price_cross_down = (df['close'] < df['kijun_sen']) & (df['prev_close'] >= df['prev_kijun'])
            df.loc[price_cross_up, 'signal'] = 1
            if not self.params.get('long_only', True):
                df.loc[price_cross_down, 'signal'] = -1
            else:
                df.loc[price_cross_down, 'signal'] = 0
        
        # Generate Cloud Breakout signals
        if self.params.get('use_cloud_breakouts', True):
            cloud_breakout_up = (df['close'] > cloud_top) & (df['prev_close'] <= df['prev_cloud_top'])
            cloud_breakout_down = (df['close'] < cloud_bottom) & (df['prev_close'] >= df['prev_cloud_bottom'])
            df.loc[cloud_breakout_up, 'signal'] = 1
            if not self.params.get('long_only', True):
                df.loc[cloud_breakout_down, 'signal'] = -1
            else:
                df.loc[cloud_breakout_down, 'signal'] = 0
        
        return df
    
    def generate_signals(self, ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate risk-managed trading signals using the Ichimoku Cloud indicator.

        This method retrieves required historical data (including extra periods for rolling
        calculations), computes Ichimoku components in a vectorized fashion (supporting multiple
        tickers via group operations), generates buy/sell signals based on TK crosses, price-Kijun
        crosses and cloud breakout rules, and then applies risk management (stop-loss, take-profit,
        slippage, transaction costs).

        Args:
            ticker (str or List[str]): A single ticker symbol or a list of ticker symbols.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format. When provided,
                                        additional historical data is fetched for indicator calculation.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): The starting market position (0 for flat, 1 for long, -1 for short).
            latest_only (bool): If True, only the most recent signal row is returned (per ticker in multi-ticker mode).

        Returns:
            pd.DataFrame: DataFrame that includes price data, Ichimoku components, raw trading signals,
            and risk management outputs (including positions and returns) ready for backtest analysis
            and downstream optimization.
        """
        # Convert to list if a single ticker is provided
        if isinstance(ticker, str):
            ticker = [ticker]
        
        # Define necessary indicator periods from parameters
        self.displacement = int(self.params['displacement'])
        self.tenkan_period = int(self.params['tenkan_period'])
        self.kijun_period = int(self.params['kijun_period'])
        self.senkou_b_period = int(self.params['senkou_b_period'])
        extra_periods = self.displacement + max(self.tenkan_period, self.kijun_period, self.senkou_b_period)
        
        # If a start_date is provided, adjust it backwards to supply extra data for rolling calculations
        if start_date:
            start_dt = pd.to_datetime(start_date)
            adjusted_start_date = (start_dt - pd.Timedelta(days=extra_periods)).strftime('%Y-%m-%d')
        else:
            adjusted_start_date = None
        
        # Retrieve historical price data; when start_date is not provided, use a lookback period (e.g., 252 trading days)
        lookback = None if start_date else (252 + extra_periods)
        price_data = self.get_historical_prices(
            ticker,
            lookback=lookback,
            from_date=adjusted_start_date,
            to_date=end_date
        )
        if price_data.empty:
            self.logger.warning("No price data available for tickers: %s", ticker)
            return pd.DataFrame()
        
        # Determine whether the data is for multiple tickers (MultiIndex) or single ticker
        multi_ticker = isinstance(price_data.index, pd.MultiIndex)
        
        # Compute indicators and signals
        if multi_ticker:
            # Ensure index levels are named correctly
            if price_data.index.names != ['ticker', 'date']:
                price_data.index.names = ['ticker', 'date']
            # Apply computation per group
            price_data = price_data.groupby(level='ticker').apply(self._compute_indicators)
        else:
            # Single ticker: direct computation
            price_data = self._compute_indicators(price_data)
        
        # Drop rows with NA values that may result from rolling calculations
        price_data = price_data.dropna()
        
        # Apply risk management adjustments (stop loss, take profit, slippage, transaction cost)
        rm = RiskManager(
            stop_loss_pct=self.params.get("stop_loss_pct", 0.05),
            take_profit_pct=self.params.get("take_profit_pct", 0.10),
            trailing_stop_pct=self.params.get("trailing_stop_pct", 0.0),
            slippage_pct=self.params.get("slippage_pct", 0.001),
            transaction_cost_pct=self.params.get("transaction_cost_pct", 0.001)
        )
        result = rm.apply(price_data, initial_position=initial_position)
        
        # If latest_only flag is set, return only the most recent signal row for each ticker
        if latest_only:
            if multi_ticker:
                result = result.groupby(level='ticker').tail(1)
            else:
                result = result.tail(1)
        
        return result