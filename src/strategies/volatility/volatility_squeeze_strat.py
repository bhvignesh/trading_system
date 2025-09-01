# trading_system/src/strategies/volatality/volatility_squeeze.py

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class VolatilitySqueezeStrategy(BaseStrategy):
    """
    Volatility Squeeze Strategy with Integrated Risk Management.

    This strategy identifies periods of low volatility (a "squeeze") by comparing Bollinger Bands (BB)
    with Keltner Channels (KC). The mathematical logic is as follows:

      Bollinger Bands (BB):
        - BB Middle = Moving Average of closing prices over a window of N days.
        - BB Std    = Standard deviation of closing prices over the same window.
        - BB Upper  = BB Middle + (bb_std * BB Std)
        - BB Lower  = BB Middle - (bb_std * BB Std)

      Keltner Channels (KC):
        - KC Middle = Moving Average of closing prices over a window of N days.
        - ATR       = Average True Range, computed as the exponentially smoothed average of the true range,
                      where the true range is the maximum of: (high - low), |high - previous close|, and |low - previous close|.
        - KC Upper  = KC Middle + (kc_multiplier * ATR)
        - KC Lower  = KC Middle - (kc_multiplier * ATR)

      Squeeze Condition:
        A squeeze is said to occur when the Bollinger Bands lie completely inside the Keltner Channels:
            squeeze_on = (BB Lower > KC Lower) and (BB Upper < KC Upper)
        When this condition is released (i.e. squeeze_on turns False after being True), a breakout is likely.

      Momentum:
        Computed as the slope of a linear regression on the closing prices over a lookback (momentum_period);
        mathematically, given x = [0, 1, ..., period-1] and y the price series, the slope is:
        
            slope = (n * Σ(x*y) - Σ(x)*Σ(y)) / (n * Σ(x²) - [Σ(x)]²)

      Trading Signals:
        - Long Signal (signal = 1): Produced when a squeeze is released (squeeze_off is True) and momentum > 0.
        - Short Signal (signal = -1): Produced when a squeeze is released and momentum < 0.
        - Otherwise, the signal is 0.

      Risk Management:
        The strategy integrates with the RiskManager to apply stop-loss, take-profit, slippage and transaction cost
        adjustments on each signal. The risk-managed returns and positions are then appended to the final DataFrame.

    The strategy supports backtesting over a specified date range and/or forecasting the latest signal,
    and can process multiple tickers in a vectorized fashion.

    Args:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict, optional): Strategy-specific parameters with defaults:
            - 'bb_period'            : Period for Bollinger Bands (default = 20).
            - 'bb_std'               : Std multiplier for Bollinger Bands (default = 2.0).
            - 'kc_period'            : Period for Keltner Channels (default = 20).
            - 'kc_atr_period'        : Period for ATR in Keltner Channels (default = 20).
            - 'kc_multiplier'        : Multiplier for channel width (default = 1.5).
            - 'momentum_period'      : Lookback period for momentum slope calculation (default = 12).
            - 'stop_loss_pct'        : Stop loss threshold (default = 0.05).
            - 'take_profit_pct'      : Take profit threshold (default = 0.10).
            - 'trailing_stop_pct'    : Trailing stop percentage (default = 0.00).
            - 'slippage_pct'         : Slippage percentage (default = 0.001).
            - 'transaction_cost_pct' : Transaction cost percentage (default = 0.001).
            - 'long_only'            : If True, generate long-only signals (default = True).

    Returns:
        pd.DataFrame: DataFrame with columns including price data, computed indicators,
                      signal, normalized signal strength, and risk-managed trade metrics:
                        - Price: 'open', 'high', 'low', 'close', 'volume'
                        - Indicators: 'bb_middle', 'bb_upper', 'bb_lower', 'kc_middle', 'kc_upper', 'kc_lower', 'momentum', 
                                      'squeeze_on', 'squeeze_off'
                        - Trading Signal: 'signal', 'signal_strength'
                        - Risk Managed: 'rm_strategy_return', 'rm_cumulative_return', 'rm_position', 'rm_action'
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Volatility Squeeze strategy with default and user-provided parameters.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Strategy-specific parameters.
        """
        default_params = {
            'bb_period': 20,
            'bb_std': 2.0,
            'kc_period': 20,
            'kc_atr_period': 20,
            'kc_multiplier': 1.5,
            'momentum_period': 12,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.00,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }
        if params:
            default_params.update(params)
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
        Generate trading signals along with risk-managed trade metrics based on volatility squeeze.
        
        This method retrieves historical price data (for a given date range or lookback period),
        computes Bollinger Bands and Keltner Channels, calculates momentum via a vectorized linear regression slope,
        and identifies squeeze conditions. Trading signals—long or short—are generated when a squeeze is released,
        with signal strength normalized by the rolling standard deviation of momentum.
        Finally, risk management adjustments (stop-loss, take-profit, slippage, and transaction cost) are applied.
        
        Supports processing a single ticker (str) or a list of tickers.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Start date for backtesting ('YYYY-MM-DD').
            end_date (str, optional): End date for backtesting ('YYYY-MM-DD').
            initial_position (int): Starting position (0 for flat, 1 for long, -1 for short).
            latest_only (bool): If True, return only the latest signal for each ticker.
        
        Returns:
            pd.DataFrame: DataFrame combining price data, indicators, signals, normalized signal strength,
                          and risk-managed trade metrics.
        """
        # Retrieve historical price data using the base class method.
        if start_date or end_date:
            price_data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
        else:
            price_data = self.get_historical_prices(ticker, lookback=252)

        # Define technical indicator parameters.
        bb_period = int(self.params['bb_period'])
        bb_std_val = self.params['bb_std']
        kc_period = int(self.params['kc_period'])
        kc_atr_period = int(self.params['kc_atr_period'])
        kc_multiplier = self.params['kc_multiplier']
        momentum_period = int(self.params['momentum_period'])

        # Require a minimum number of periods to ensure rolling calculations work.
        min_periods = max(bb_period, kc_period, kc_atr_period, momentum_period) + 10

        def process_group(df: pd.DataFrame) -> pd.DataFrame:
            """
            Process price data for one ticker: compute indicators, signals, and risk-managed trade metrics.
            
            Args:
                df (pd.DataFrame): Price data (with date index) for one ticker.
            
            Returns:
                pd.DataFrame: DataFrame with computed technical indicators, trading signals,
                              normalized signal strength, and risk-managed metrics.
            """
            if not self._validate_data(df, min_records=min_periods):
                self.logger.warning("Insufficient data for ticker to generate signals")
                return pd.DataFrame()

            # Calculate Bollinger Bands.
            bb_middle = df['close'].rolling(window=bb_period).mean()
            bb_std = df['close'].rolling(window=bb_period).std()
            bb_upper = bb_middle + (bb_std * bb_std_val)
            bb_lower = bb_middle - (bb_std * bb_std_val)

            # Calculate Keltner Channels.
            kc_middle = df['close'].rolling(window=kc_period).mean()
            kc_atr = self._calculate_atr(df, period=kc_atr_period)
            kc_upper = kc_middle + (kc_atr * kc_multiplier)
            kc_lower = kc_middle - (kc_atr * kc_multiplier)

            # Compute momentum as the slope over a rolling window (vectorized).
            momentum = self._linear_regression_slope(df['close'], period=momentum_period)

            # Determine squeeze: in a squeeze when Bollinger Bands lie within Keltner Channels.
            squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)
            squeeze_off = (~squeeze_on) & squeeze_on.shift(1)

            # Build DataFrame with price data and computed indicators.
            res = df.copy()
            res['bb_middle'] = bb_middle
            res['bb_upper'] = bb_upper
            res['bb_lower'] = bb_lower
            res['kc_middle'] = kc_middle
            res['kc_upper'] = kc_upper
            res['kc_lower'] = kc_lower
            res['momentum'] = momentum
            res['squeeze_on'] = squeeze_on
            res['squeeze_off'] = squeeze_off

            # Generate trading signals: +1 if squeeze releases and momentum > 0; -1 if momentum < 0.
            res['signal'] = 0
            res.loc[squeeze_off & (momentum > 0), 'signal'] = 1
            res.loc[squeeze_off & (momentum < 0), 'signal'] = -1

            # Normalize signal strength using the rolling standard deviation of momentum.
            res['signal_strength'] = 0.0
            rolling_mom_std = momentum.rolling(window=20).std() + 1e-6
            res.loc[res['signal'] != 0, 'signal_strength'] = np.abs(momentum[res['signal'] != 0]) / rolling_mom_std[res['signal'] != 0]

            if self.params['long_only']:
                res.loc[res['signal'] == -1, 'signal'] = 0

            # Drop rows with NaN values due to rolling calculations.
            res = res.dropna(subset=['bb_middle', 'kc_middle', 'momentum'])

            # Apply risk management adjustments.
            risk_manager = RiskManager(
                stop_loss_pct=self.params['stop_loss_pct'],
                take_profit_pct=self.params['take_profit_pct'],
                trailing_stop_pct=self.params['trailing_stop_pct'],
                slippage_pct=self.params['slippage_pct'],
                transaction_cost_pct=self.params['transaction_cost_pct']
            )
            risk_df = risk_manager.apply(res.copy(), initial_position=initial_position)
            # Rename columns from the RiskManager to match conventions.
            risk_df = risk_df.rename(columns={
                'return': 'rm_strategy_return',
                'cumulative_return': 'rm_cumulative_return',
                'position': 'rm_position',
                'exit_type': 'rm_action'
            })
            # Merge risk-managed columns with the technical indicators.
            res = res.join(risk_df[['rm_strategy_return', 'rm_cumulative_return', 'rm_position', 'rm_action']])
            return res

        # Process either a single ticker or a list of tickers.
        if isinstance(ticker, str):
            result = process_group(price_data)
            if latest_only and not result.empty:
                result = result.iloc[[-1]]
        else:
            # For multiple tickers, price_data is a MultiIndex (ticker, date).
            results = []
            for tick, group in price_data.groupby(level=0):
                # Drop the ticker level for processing.
                grp_res = process_group(group.droplevel(0))
                if not grp_res.empty:
                    grp_res['ticker'] = tick
                    if latest_only:
                        grp_res = grp_res.iloc[[-1]]
                    results.append(grp_res)
            result = pd.concat(results) if results else pd.DataFrame()
            if 'ticker' in result.columns:
                result = result.reset_index().set_index(['ticker', 'date']).sort_index()
        return result

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate the Average True Range (ATR) for the given price data.
        
        The ATR measures market volatility based on the true range of price movement.
        True range is the maximum of:
            - (high - low)
            - |high - previous close|
            - |low - previous close|
        ATR is then the exponentially weighted moving average of the true ranges.
        
        Args:
            price_data (pd.DataFrame): DataFrame containing columns 'high', 'low', and 'close'.
            period (int): Lookback period for computing ATR.
        
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

    def _linear_regression_slope(self, series: pd.Series, period: int = 14) -> pd.Series:
        """
        Calculate the slope of a linear regression over a rolling window using a fully vectorized approach.
        
        For each window of length 'period', the slope is computed as:
            slope = (n*Σ(x*y) - Σ(x)*Σ(y)) / (n*Σ(x²) - [Σ(x)]²)
        where x = [0, 1, ..., period-1] and n = period.
        
        Args:
            series (pd.Series): Time series data (e.g. closing prices).
            period (int): Lookback period for the rolling window.
        
        Returns:
            pd.Series: Rolling linear regression slopes.
        """
        x = np.arange(period)
        sum_x = x.sum()
        sum_x2 = (x**2).sum()
        denominator = period * sum_x2 - sum_x**2

        vals = series.values
        if len(vals) < period:
            return pd.Series(np.nan, index=series.index)

        # Create a 2D rolling window view of the series.
        rolling_windows = sliding_window_view(vals, window_shape=period)
        # Compute dot product of each window with x.
        dot = np.dot(rolling_windows, x)
        # Compute rolling sum for each window.
        sum_y = rolling_windows.sum(axis=1)
        slopes = (period * dot - sum_x * sum_y) / denominator
        # Prepend NaN for indices without a full window.
        slopes_full = np.concatenate([np.full(period - 1, np.nan), slopes])
        return pd.Series(slopes_full, index=series.index)