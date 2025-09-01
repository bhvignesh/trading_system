# trading_system/src/strategies/stochastic_oscillator_strat.py

import logging
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class StochasticStrategy(BaseStrategy):
    """
    Stochastic Oscillator Strategy with Integrated Risk Management.
    
    This strategy calculates the stochastic oscillator indicators (%K and %D) using:
    
        %K = 100 * (C - L_n) / (H_n - L_n)
        %D = Moving average(%K) over d_period
    
    where:
      - C = current close price,
      - L_n = lowest low over the last k_period bars,
      - H_n = highest high over the last k_period bars.
    
    **Signal Generation:**
      - Generates a buy signal (1) when %K crosses above %D and both are below the oversold threshold.
      - Generates a sell signal (-1) when %K crosses below %D and both are above the overbought threshold.
      - In a long-only configuration, sell signals are overridden to generate exit signals (0).
    
    Signal strength is computed as the normalized distance from the threshold:
      - For buy signals, strength = ((oversold - min(%k, %d)) / oversold) clipped between 0 and 1.
      - For sell signals, strength = ((max(%k, %d) - overbought) / (100 - overbought)) clipped between 0 and 1.
    
    **Risk Management:**
      A RiskManager instance is used to:
        - Adjust the entry price by incorporating slippage and transaction costs.
        - Calculate stop-loss / take-profit thresholds / trailing stop.
        - Identify exit events (stop loss, take profit, or signal reversal).
        - Compute the realized trade return and cumulative return.
    
    The strategy supports vectorized processing for backtesting over a set date range and also allows
    generating only the last available signal (forecast mode). It is designed to be similar in structure to
    the Awesome Oscillator strategy to ease testing with various hyperparameters.
    
    Attributes:
        k_period (int): Lookback period for the %K calculation (default: 14).
        d_period (int): Smoothing period for %D calculation (default: 3).
        overbought (int): Overbought threshold (default: 80).
        oversold (int): Oversold threshold (default: 20).
        long_only (bool): If True, only long positions are allowed (default: True).
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the StochasticStrategy with database configuration and strategy parameters.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Hyperparameters such as k_period, d_period, overbought, oversold,
                                     stop_loss_pct, take_profit_pct, slippage_pct, transaction_cost_pct,
                                     and long_only.
        """
        super().__init__(db_config, params)
        self.params.setdefault('long_only', True)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._validate_and_set_params()

    def _validate_and_set_params(self):
        """
        Validate and set hyperparameters for the stochastic oscillator.
        
        Raises:
            ValueError: If k_period or d_period is non-positive or if overbought <= oversold.
        """
        self.k_period = int(self.params.get('k_period', 14))
        self.d_period = int(self.params.get('d_period', 3))
        self.overbought = int(self.params.get('overbought', 80))
        self.oversold = int(self.params.get('oversold', 20))

        if self.k_period <= 0 or self.d_period <= 0:
            raise ValueError("k_period and d_period must be positive integers")
        if self.overbought <= self.oversold:
            raise ValueError("overbought must be greater than oversold")

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve historical price data, compute stochastic oscillator indicators, generate
        trading signals with strengths, and apply risk management.
        
        Supports both a single ticker (str) and a list of tickers (List[str]). In the multi-ticker
        scenario, the computation is performed in a vectorized fashion per ticker.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Start date for backtesting (YYYY-MM-DD inclusive).
            end_date (str, optional): End date for backtesting (YYYY-MM-DD inclusive).
            initial_position (int): The initial trading position (0 for flat, 1 for long, -1 for short).
            latest_only (bool): If True, returns only the most recent signal(s) for forecasting.
        
        Returns:
            pd.DataFrame: Processed DataFrame(s) containing 'close', 'high', 'low', 'signal',
                          'strength', 'return', 'cumulative_return', and 'exit_type' columns.
        """
        try:
            # Ensure enough data points: need at least k_period + d_period - 1 records.
            min_data_points = self.k_period + self.d_period - 1

            # Retrieve historical price data using the base class method.
            prices = self.get_historical_prices(
                ticker,
                from_date=start_date,
                to_date=end_date,
                lookback=min_data_points if latest_only else None
            )
            
            # Process multi-ticker data if ticker is a list.
            if isinstance(ticker, list):
                # prices has a MultiIndex of (ticker, date)
                grouped_results = []
                for tck, group in prices.groupby(level=0):
                    group = group.droplevel(0)  # Work on a DataFrame indexed solely by date.
                    if not self._validate_data(group, min_data_points):
                        self.logger.warning("Insufficient data for ticker: %s", tck)
                        continue
                    df_ind = self._calculate_indicators(group)
                    signals, strengths = self._generate_vectorized_signals(df_ind, initial_position)
                    result = self._apply_risk_management(df_ind, signals, strengths, initial_position, latest_only)
                    result["ticker"] = tck
                    # Restore a multi-index: (ticker, date)
                    result.set_index("ticker", append=True, inplace=True)
                    grouped_results.append(result)
                if not grouped_results:
                    return pd.DataFrame()
                final_result = pd.concat(grouped_results)
                final_result = final_result.reorder_levels(['ticker', 'date'])
                # If forecasting only, return the last row per ticker.
                if latest_only:
                    final_result = final_result.groupby(level=0).tail(1)
                return final_result
            else:
                # Process a single ticker.
                if not self._validate_data(prices, min_data_points):
                    self.logger.warning("Insufficient data for ticker: %s", ticker)
                    return pd.DataFrame()
                df_ind = self._calculate_indicators(prices)
                signals, strengths = self._generate_vectorized_signals(df_ind, initial_position)
                return self._apply_risk_management(df_ind, signals, strengths, initial_position, latest_only)
                
        except Exception as e:
            self.logger.error(f"Error processing {ticker}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _calculate_indicators(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the stochastic oscillator indicators (%K and %D) from historical price data.
        
        The %K line is computed as:
            %K = 100 * (close - rolling_min(low, k_period)) / (rolling_max(high, k_period) - rolling_min(low, k_period))
        and %D is the moving average of %K over d_period.
        
        Args:
            prices (pd.DataFrame): DataFrame with columns 'close', 'high', and 'low', indexed by date.
        
        Returns:
            pd.DataFrame: DataFrame containing 'close', 'high', 'low', '%k', and '%d' columns with
                          NaN values removed.
        """
        # Calculate rolling min and max over k_period.
        low_min = prices['low'].rolling(self.k_period, min_periods=self.k_period).min()
        high_max = prices['high'].rolling(self.k_period, min_periods=self.k_period).max()

        # Replace 0 denominator with a very small number to avoid division errors.
        denominator = (high_max - low_min).replace(0, 1e-9)
        k = 100 * (prices['close'] - low_min) / denominator
        d = k.rolling(self.d_period, min_periods=self.d_period).mean()

        df = pd.DataFrame({
            'close': prices['close'],
            'high': prices['high'],
            'low': prices['low'],
            '%k': k,
            '%d': d
        }).dropna()
        return df

    def _generate_vectorized_signals(
        self, df: pd.DataFrame, initial_position: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Generate trading signals and corresponding signal strengths from the %K and %D indicators.
        
        A buy signal (1) is generated when a crossover occurs where the current %K is above %D and
        the previous %K was at or below %D, provided that both %K and %D are below the oversold threshold.
        Conversely, a sell signal (-1) is generated when a crossover occurs where the current %K is below
        %D and the previous %K was at or above %D, provided that both are above the overbought threshold.
        In long-only mode, sell signals are overridden to flat (0).
        
        Signal strength is calculated as follows:
          - For buy signals: strength = clip((oversold - min(%k, %d)) / oversold, 0, 1)
          - For sell signals: strength = clip((max(%k, %d) - overbought) / (100 - overbought), 0, 1)
        
        Args:
            df (pd.DataFrame): DataFrame with columns '%k' and '%d'; indexed by date.
            initial_position (int): The starting trading position.
        
        Returns:
            Tuple[np.ndarray, np.ndarray]:
                - signals: integer array (1 for buy, -1 for sell, 0 for no signal/change).
                - strengths: float array representing normalized signal strengths.
        """
        k = df['%k'].values
        d = df['%d'].values
        prev_k = np.roll(k, 1)
        prev_d = np.roll(d, 1)
        prev_k[0] = np.nan  # No previous value for the first bar

        with np.errstate(invalid='ignore'):
            buy_cond = (
                (k > d) &
                (prev_k <= prev_d) &
                (k < self.oversold) &
                (d < self.oversold)
            )
            sell_cond = (
                (k < d) &
                (prev_k >= prev_d) &
                (k > self.overbought) &
                (d > self.overbought)
            )

        # Stateful signal generation: only change positions when a crossover condition is met.
        signals = np.zeros(len(df), dtype=int)
        current_signal = initial_position
        for i in range(len(df)):
            if i == 0:
                signals[i] = initial_position
            elif buy_cond[i] and current_signal != 1:
                signals[i] = 1
                current_signal = 1
            elif sell_cond[i] and current_signal != -1:
                signals[i] = -1
                current_signal = -1
            else:
                signals[i] = 0

        # Calculate the signal strength based on the normalized distance from the thresholds.
        strengths = np.zeros(len(df), dtype=float)
        buy_mask = signals == 1
        sell_mask = signals == -1
        
        if buy_mask.any():
            min_level = np.minimum(k[buy_mask], d[buy_mask])
            strengths[buy_mask] = np.clip((self.oversold - min_level) / self.oversold, 0, 1)
        if sell_mask.any():
            max_level = np.maximum(k[sell_mask], d[sell_mask])
            strengths[sell_mask] = np.clip((max_level - self.overbought) / (100 - self.overbought), 0, 1)

        # For long-only strategies, override any sell (-1) signal to 0.
        if self.params.get('long_only', True):
            signals[signals == -1] = 0

        return signals, strengths

    def _apply_risk_management(
        self,
        df: pd.DataFrame,
        signals: np.ndarray,
        strengths: np.ndarray,
        initial_position: int,
        latest_only: bool
    ) -> pd.DataFrame:
        """
        Apply the external RiskManager to adjust signals for execution slippage, transaction costs,
        stop-loss, and take-profit thresholds. Computes realized trade returns and cumulative returns.
        
        Args:
            df (pd.DataFrame): DataFrame containing indicator values and price data.
            signals (np.ndarray): Array of trading signals (1, -1, or 0).
            strengths (np.ndarray): Array of signal strengths (normalized between 0 and 1).
            initial_position (int): The starting trading position.
            latest_only (bool): If True, returns only the last row of the processed DataFrame.
        
        Returns:
            pd.DataFrame: A DataFrame including 'close', 'high', 'low', 'signal', 'strength',
                          'return', 'cumulative_return', and 'exit_type' to support full backtest analysis.
        """
        df = df.copy()
        df['signal'] = signals
        df['strength'] = strengths

        risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05),
            take_profit_pct=self.params.get('take_profit_pct', 0.10),
            trailing_stop_pct=self.params.get('trailing_stop_pct', 0.00),
            slippage_pct=self.params.get('slippage_pct', 0.001),
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001)
        )

        result = risk_manager.apply(df, initial_position=initial_position)
        return result.iloc[[-1]] if latest_only else result

    def _validate_data(self, df: pd.DataFrame, min_records: int) -> bool:
        """
        Validate that the input DataFrame has at least the specified number of records.
        
        Args:
            df (pd.DataFrame): The DataFrame with historical price data.
            min_records (int): The minimum number of required records.
            
        Returns:
            bool: True if data is sufficient; otherwise, logs a warning and returns False.
        """
        if len(df) < min_records:
            self.logger.warning(f"Insufficient data: {len(df)} records available, needed {min_records}")
            return False
        return True