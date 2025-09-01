# trading_system/src/strategies/aroon_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class AroonStrategy(BaseStrategy):
    """
    Aroon Strategy with Integrated Risk Management.

    This strategy computes the Aroon Up and Aroon Down indicators over a specified
    lookback window (default 25 days) as follows:

        Aroon Up   = ((index of highest high in window + 1) / N) * 100,
        Aroon Down = ((index of lowest low in window + 1) / N) * 100,

    where N is the lookback period. Then, the Aroon Oscillator is computed as:

        Aroon Oscillator = Aroon Up - Aroon Down

    A normalized signal strength is then defined by normalizing the absolute oscillator value
    by its rolling standard deviation:

        signal_strength = |Aroon Oscillator| / (rolling_std(Aroon Oscillator) + 1e-6)

    Trading signals are generated as:
      - Long signal (1) when Aroon Up ≥ 70 and Aroon Down ≤ 30.
      - Short signal (-1) when Aroon Up ≤ 30 and Aroon Down ≥ 70 (if long_only is False; otherwise, 0).

    Raw signals are forward filled until a reversal is detected. To account for execution costs,
    stop-loss, and take-profit thresholds, the RiskManager is applied. In backtesting mode, the full
    data with computed price returns, risk-managed realized trade returns, cumulative returns, and
    exit action are retained for downstream metric calculations (e.g. Sharpe ratio and max drawdown).
    For real-time forecasting (latest_only=True), only a minimal set of columns is returned.

    Args:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict, optional): Strategy parameters including:
            - 'lookback'             : Lookback period (default: 25).
            - 'stop_loss_pct'        : Stop loss percentage (default: 0.05).
            - 'take_profit_pct'      : Take profit percentage (default: 0.10).
            - 'slippage_pct'         : Slippage percentage (default: 0.001).
            - 'trailing_stop_pct'    : Trailing stop percentage (default: 0.0).
            - 'transaction_cost_pct' : Transaction cost percentage (default: 0.001).
            - 'long_only'            : If True, allow only long positions (default: True).

    Supports processing of a single ticker (str) or multiple tickers (List[str]). When multiple tickers
    are provided, vectorized (group-by-ticker) operations are used for speed.
    """
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        super().__init__(db_config, params)
        self.lookback = int(self.params.get('lookback', 25))
        self.aroon_up_threshold = int(self.params.get('aroon_up_threshold', 70))
        self.aroon_down_threshold = int(self.params.get('aroon_down_threshold', 30))
        self.long_only = self.params.get('long_only', True)
        self.risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05),
            take_profit_pct=self.params.get('take_profit_pct', 0.10),
            trailing_stop_pct=self.params.get('trailing_stop_pct', 0.0),
            slippage_pct=self.params.get('slippage_pct', 0.001),
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001)
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _calculate_aroon(self, high: pd.Series, low: pd.Series) -> pd.DataFrame:
        """
        Compute Aroon Up, Aroon Down, Aroon Oscillator, and normalized signal strength.
        
        Mathematically:
          - Aroon Up   = ((np.argmax(high over lookback window) + 1) / lookback) * 100
          - Aroon Down = ((np.argmin(low over lookback window) + 1) / lookback) * 100
          - Aroon Oscillator = Aroon Up - Aroon Down
          - signal_strength = |Aroon Oscillator| / (rolling_std(Aroon Oscillator) + 1e-6)
          
        Args:
            high (pd.Series): High prices.
            low (pd.Series): Low prices.
            
        Returns:
            pd.DataFrame: DataFrame with columns 'aroon_up', 'aroon_down', 'aroon_osc', and 'signal_strength'.
        """
        # Compute Aroon Up and Aroon Down using a rolling window calculation.
        # The lambda functions work on the rolling window array.
        high_window = high.rolling(window=self.lookback, min_periods=self.lookback)
        low_window = low.rolling(window=self.lookback, min_periods=self.lookback)
        aroon_up = high_window.apply(lambda x: (np.argmax(x) + 1) / self.lookback * 100, raw=True)
        aroon_down = low_window.apply(lambda x: (np.argmin(x) + 1) / self.lookback * 100, raw=True)

        # Calculate the oscillator and its normalized strength.
        aroon_osc = aroon_up - aroon_down
        # Compute rolling standard deviation over the same window.
        osc_std = aroon_osc.rolling(window=self.lookback, min_periods=self.lookback).std()
        signal_strength = (aroon_osc.abs() / (osc_std + 1e-6)).fillna(0)

        # Apply smoothing if specified
        signal_smoothing = int(self.params.get('signal_smoothing', 1))
        
        if signal_smoothing > 1:
            aroon_up = aroon_up.rolling(window=signal_smoothing).mean()
            aroon_down = aroon_down.rolling(window=signal_smoothing).mean()
            aroon_osc = aroon_osc.rolling(window=signal_smoothing).mean()
            signal_strength = signal_strength.rolling(window=signal_smoothing).mean()

        return pd.DataFrame({
            'aroon_up': aroon_up,
            'aroon_down': aroon_down,
            'aroon_osc': aroon_osc,
            'signal_strength': signal_strength
        })

    def _generate_raw_signals(self, df: pd.DataFrame) -> pd.Series:
        """
        Generate raw trading signals based on computed Aroon indicators.

        For each bar:
          - Generate a long signal (1) if aroon_up >= aroon_up_threshold and aroon_down <= aroon_down_threshold.
          - Generate a short signal (-1) if aroon_up <= aroon_down_threshold and aroon_down >= aroon_up_threshold (only if long_only is False).
          - In the absence of a new signal, the previous nonzero signal is forward filled.

        Args:
            df (pd.DataFrame): DataFrame including at least 'aroon_up' and 'aroon_down'.
            
        Returns:
            pd.Series: Integer signals (1 for long, -1 for short, 0 for flat).
        """
        raw_signals = np.zeros(len(df), dtype=int)
        long_cond = (df['aroon_up'] >= self.aroon_up_threshold) & (df['aroon_down'] <= self.aroon_down_threshold)
        short_cond = (df['aroon_up'] <= self.aroon_down_threshold) & (df['aroon_down'] >= self.aroon_up_threshold)
        raw_signals = np.where(long_cond, 1, raw_signals)
        if not self.long_only:
            raw_signals = np.where(short_cond, -1, raw_signals)
        else:
            raw_signals = np.where(short_cond, 0, raw_signals)
        # Forward fill previous nonzero signals.
        signals = pd.Series(raw_signals, index=df.index).replace(0, np.nan).ffill().fillna(0)
        return signals.astype(int)

    def _process_single_ticker(self, df: pd.DataFrame, initial_position: int, latest_only: bool) -> pd.DataFrame:
        """
        Process a single ticker's price DataFrame by calculating indicators, generating signals,
        and applying risk management (if not in latest_only mode).

        Args:
            df (pd.DataFrame): Price data with at least 'open', 'high', 'low', 'close'.
            initial_position (int): Starting position.
            latest_only (bool): Whether to return only the most recent signal.
            
        Returns:
            pd.DataFrame: Processed DataFrame with computed indicators, signals, risk-managed returns,
                          and additional columns for backtesting.
        """
        # Validate that sufficient records exist.
        min_records = self.lookback if latest_only else 2 * self.lookback
        if not self._validate_data(df, min_records=min_records):
            return pd.DataFrame()

        # Compute the Aroon indicators.
        aroon_df = self._calculate_aroon(df['high'], df['low'])
        df = df.join(aroon_df)

        # Generate raw trading signals and assign to the DataFrame.
        df['signal'] = self._generate_raw_signals(df)

        if not latest_only:
            # Apply the Risk Manager to get risk-managed positions, returns, and exit types.
            df = self.risk_manager.apply(df, initial_position=initial_position)
            # Calculate daily returns and derive strategy returns.
            df['daily_return'] = df['close'].pct_change(fill_method=None).fillna(0)
            df['strategy_return'] = df['position'].shift(1) * df['daily_return']
            # Rename risk management specific columns for downstream consistency.
            df.rename(columns={
                'return': 'rm_strategy_return',
                'cumulative_return': 'rm_cumulative_return',
                'exit_type': 'rm_action'
            }, inplace=True)
        else:
            # When forecasting for the latest signal, restrict columns.
            df = df[['open', 'high', 'low', 'close', 'aroon_up', 'aroon_down', 'aroon_osc', 'signal_strength', 'signal']].iloc[[-1]]
        return df

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve historical data and generate trading signals with integrated risk management.

        For each ticker, the method:
          1. Retrieves historical price data (from a specified date range for backtesting or a minimal
             lookback slice for real-time forecasting).
          2. Computes the Aroon indicators (Aroon Up, Aroon Down, Aroon Oscillator, and signal strength).
          3. Generates raw trading signals:
             - Long (1) when Aroon Up ≥ 70 and Aroon Down ≤ 30.
             - Short (-1) when Aroon Up ≤ 30 and Aroon Down ≥ 70 (if not long_only).
             The signals are forward filled until a reversal.
          4. When in backtest mode (latest_only is False), applies risk management to adjust the entry/exit
             prices by incorporating slippage, transaction costs, stop loss and take profit thresholds.
             Risk-managed columns such as 'rm_strategy_return', 'rm_cumulative_return', and 'rm_action'
             are produced. Daily returns and strategy returns (using prior-day positions) are also computed.
          5. For real-time signals (latest_only is True), returns only the last row with essential columns.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (default: 0).
            latest_only (bool): If True, returns only the final row of each ticker for forecasting.
            
        Returns:
            pd.DataFrame: Consolidated DataFrame containing for each ticker:
                - 'open', 'high', 'low', 'close'  : Price references.
                - 'aroon_up', 'aroon_down', 'aroon_osc', 'signal_strength'
                - 'signal'                        : Raw trading signal.
                - (For backtests) 'daily_return', 'strategy_return',
                  'position', 'rm_strategy_return', 'rm_cumulative_return', 'rm_action'
                When latest_only is True, only a minimal set of columns is returned.
        """
        # Process multiple tickers (vectorized group-by) or a single ticker.
        if isinstance(ticker, list):
            # For multiple tickers, retrieve data using the base class method.
            if latest_only:
                df = self.get_historical_prices(ticker, lookback=self.lookback)
            else:
                df = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
            if df.empty:
                return df

            # Process each ticker's data group separately.
            def process_group(group: pd.DataFrame) -> pd.DataFrame:
                return self._process_single_ticker(group, initial_position, latest_only)

            df_processed = df.groupby(level='ticker', group_keys=False).apply(process_group)

            return df_processed

        else:
            # Single ticker processing.
            if latest_only:
                df = self.get_historical_prices(ticker, lookback=self.lookback)
            else:
                df = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
            return self._process_single_ticker(df, initial_position, latest_only)

    def _validate_data(self, df: pd.DataFrame, min_records: int = 1) -> bool:
        """
        Validate that the provided historical price data has at least the required number of records.
        
        Args:
            df (pd.DataFrame): DataFrame with historical prices.
            min_records (int): Minimum number of records required.
            
        Returns:
            bool: True if the data is sufficient for processing; otherwise, False.
        """
        if df.empty or len(df) < min_records:
            self.logger.warning("Insufficient data: %d records found, %d required", len(df), min_records)
            return False
        return True