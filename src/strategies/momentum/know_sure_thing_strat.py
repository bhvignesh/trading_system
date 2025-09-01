# trading_system/src/strategies/momentum/kst_strat.py

"""
Know Sure Thing (KST) Momentum Strategy with Integrated Risk Management.

This strategy implements the KST indicator for generating trading signals,
position simulation, and profit metric computation, following a structure consistent
with other strategies in the system (e.g., AwesomeOscillatorStrategy).

Mathematical Details:
    The KST indicator is computed as a weighted sum of several smoothed ROCs:
        KST = Σ (w_i * SMA(ROC(close, period_i), sma_period_i))
    where:
        ROC(close, period) = close.pct_change(period) * 100,
        SMA(ROC, period) is the simple moving average of the ROC,
        and w_i is the weight for each ROC component.
    A signal line is the SMA of the KST indicator over a specified period:
        signal_line = SMA(KST, signal_period)

Trading Signals:
    - A buy signal (signal = 1) is triggered when KST crosses upward through its signal line.
    - A sell signal (signal = -1 or 0) is triggered when KST crosses downward through its signal line.
      If operating in "long only" mode, sell signals are replaced with 0 (exit signals).
      The 'signal' column represents the *raw trigger* at the crossover point.

Signal Strength:
    Computed as the difference between the KST and its signal line:
        signal_strength = KST - signal_line.

Risk Management:
    Integrated via the external RiskManager component. After raw signals are generated,
    the RiskManager adjusts entry prices for slippage/costs, calculates stop-loss and
    take-profit levels, identifies exit events (stops, targets, reversals), and computes
    risk-managed positions ('position') and returns ('rm_strategy_return', 'rm_cumulative_return').

The strategy supports backtesting (start/end dates) and forecasting (latest_only),
and handles vectorized processing for multiple tickers via standard MultiIndex grouping.

Outputs:
    A DataFrame containing:
      - Price data: 'open', 'close', 'high', 'low'
      - Computed indicators: 'kst', 'signal_line'
      - Raw signal trigger ('signal') and signal strength ('signal_strength')
      - Basic daily return ('daily_return') and unmanaged strategy return ('strategy_return')
      - Risk-managed position ('position')
      - Risk-managed realized trade return ('rm_strategy_return')
      - Risk-managed cumulative return ('rm_cumulative_return')
      - Risk management action type ('rm_action')
      - 'ticker' column (in multi-ticker mode output before optimizer processing)
      - 'date' column (in multi-ticker mode output before optimizer processing, mirroring index)

Strategy-specific parameters provided in `params` (with defaults):
    - 'roc_periods': List of integers for ROC lookback periods (default: [10, 15, 20, 30]).
    - 'sma_periods': List of integers for smoothing (SMA) periods corresponding to each ROC (default: [10, 10, 10, 15]).
    - 'signal_period': Integer period for computing the KST signal line (default: 9).
    - 'kst_weights': List of weights for each ROC component (default: [1, 2, 3, 4]).
    - 'long_only': Boolean flag; if True, restricts trading to long positions (default: True).
    - Risk Parameters (nested under 'risk_params' or individual keys):
        - 'stop_loss_pct' (default: 0.05)
        - 'take_profit_pct' (default: 0.10)
        - 'trailing_stop_pct' (default: 0.0)
        - 'slippage_pct' (default: 0.001)
        - 'transaction_cost_pct' (default: 0.001)
"""

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class KSTStrategy(BaseStrategy):
    """
    Implements the Know Sure Thing (KST) strategy using multiple Rate-of-Change (ROC)
    components, integrated with risk management and aligned with standard strategy structure.

    Hyperparameters are provided via the `params` dictionary during initialization.
    """

    # __init__ and _validate_parameters methods remain unchanged from the previous version
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the KSTStrategy with database settings and hyperparameters.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Strategy-specific hyperparameters.
                See class docstring for expected parameters and defaults.
        """
        # Define default parameters, including risk parameters at the top level
        default_params = {
            'roc_periods': [10, 15, 20, 30],
            'sma_periods': [10, 10, 10, 15],
            'signal_period': 9,
            'kst_weights': [1, 2, 3, 4],
            'long_only': True,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001
        }
        # Merge provided params with defaults
        if params is None:
            params = {}
        merged_params = default_params.copy()
        merged_params.update(params) # User params override defaults

        # Call BaseStrategy __init__
        super().__init__(db_config, merged_params) # Pass merged params

        # Validate and store strategy-specific parameters
        # Ensure list types if passed as tuples or other iterables
        self.params['roc_periods'] = list(self.params.get('roc_periods', default_params['roc_periods']))
        self.params['sma_periods'] = list(self.params.get('sma_periods', default_params['sma_periods']))
        self.params['kst_weights'] = list(self.params.get('kst_weights', default_params['kst_weights']))
        self.params['signal_period'] = int(self.params.get('signal_period', default_params['signal_period']))
        self.long_only = bool(self.params.get('long_only', default_params['long_only']))

        self._validate_parameters() # Validate strategy parameters

        # Initialize RiskManager using risk parameters from the merged params
        risk_params = {
            'stop_loss_pct': float(self.params.get('stop_loss_pct', default_params['stop_loss_pct'])),
            'take_profit_pct': float(self.params.get('take_profit_pct', default_params['take_profit_pct'])),
            'trailing_stop_pct': float(self.params.get('trailing_stop_pct', default_params['trailing_stop_pct'])),
            'slippage_pct': float(self.params.get('slippage_pct', default_params['slippage_pct'])),
            'transaction_cost_pct': float(self.params.get('transaction_cost_pct', default_params['transaction_cost_pct']))
        }
        self.risk_manager = RiskManager(**risk_params)

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"{self.__class__.__name__} initialized with params: {self.params}")

    def _validate_parameters(self):
        """
        Validate strategy-specific parameters (periods, weights).

        Raises:
            ValueError: If required parameters are missing, invalid type, or inconsistent lengths.
        """
        required_strategy_params = ['roc_periods', 'sma_periods', 'signal_period', 'kst_weights']
        for param in required_strategy_params:
            if param not in self.params:
                # This shouldn't happen due to default merging, but good practice
                raise ValueError(f"Internal Error: Missing strategy parameter: {param}")

        num_components = len(self.params['roc_periods'])
        if not all(len(lst) == num_components for lst in [self.params['sma_periods'], self.params['kst_weights']]):
            raise ValueError("Lengths of 'roc_periods', 'sma_periods', and 'kst_weights' must be equal.")

        if num_components == 0:
             raise ValueError("Parameter lists ('roc_periods', etc.) cannot be empty.")

        all_periods = (
            self.params['roc_periods'] +
            self.params['sma_periods'] +
            [self.params['signal_period']]
        )
        if not all(isinstance(p, int) and p > 0 for p in all_periods):
            raise ValueError("All period parameters ('roc_periods', 'sma_periods', 'signal_period') must be positive integers.")

        if not all(isinstance(w, (int, float)) for w in self.params['kst_weights']):
            raise ValueError("'kst_weights' must contain numeric values (int or float).")

    # _calculate_signals_single method remains unchanged from the previous version
    def _calculate_signals_single(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate KST indicator, signal line, raw signals, and strength for a single ticker.

        Args:
            data (pd.DataFrame): Historical price data for a single ticker, indexed by date,
                                 containing columns: ['open', 'high', 'low', 'close'].

        Returns:
            pd.DataFrame: DataFrame with columns: ['open', 'close', 'high', 'low',
                         'kst', 'signal_line', 'signal', 'signal_strength'].
                         'signal' contains the raw crossover trigger (1, -1, or 0).
        """
        # Ensure data is sorted by date (should be guaranteed by BaseStrategy, but double-check)
        data = data.sort_index()
        df = data[['open', 'high', 'low', 'close']].copy() # Work on a copy with necessary columns

        close = df['close']
        roc_periods = self.params['roc_periods']
        sma_periods = self.params['sma_periods']
        weights = self.params['kst_weights']
        signal_period = self.params['signal_period']

        # Calculate weighted, smoothed ROC components
        kst_components = []
        for roc_len, sma_len, weight in zip(roc_periods, sma_periods, weights):
            # Ensure sufficient non-NA values for pct_change and rolling mean
            if len(close) >= roc_len + sma_len:
                roc = close.pct_change(roc_len) * 100
                smoothed_roc = roc.rolling(window=sma_len, min_periods=max(1, sma_len // 2)).mean() # Allow fewer min_periods
                kst_components.append(smoothed_roc * weight)
            else:
                # If not enough data, append a series of NaNs of the correct length
                kst_components.append(pd.Series(np.nan, index=df.index))

        # Calculate KST indicator by summing components
        if kst_components:
             # Ensure alignment before summing, handle potential all-NaN columns
             kst_df = pd.concat(kst_components, axis=1)
             df['kst'] = kst_df.sum(axis=1, skipna=False) # skipna=False ensures NaN if any component is NaN
        else:
             df['kst'] = np.nan # Should not happen if validation passed

        # Calculate the signal line (SMA of KST)
        if len(df['kst'].dropna()) >= signal_period:
            df['signal_line'] = df['kst'].rolling(window=signal_period, min_periods=max(1, signal_period // 2)).mean()
        else:
            df['signal_line'] = np.nan

        # Calculate signal strength
        df['signal_strength'] = df['kst'] - df['signal_line']

        # Generate raw trading signals based on crossovers
        # Need to handle NaNs carefully in comparison
        kst_series = df['kst']
        signal_line_series = df['signal_line']

        # Shifted values (handle potential NaNs at the start)
        kst_prev = kst_series.shift(1)
        signal_line_prev = signal_line_series.shift(1)

        # Crossover conditions (True only if comparison is valid, i.e., not NaN)
        cross_above = (kst_series > signal_line_series) & (kst_prev <= signal_line_prev) & kst_series.notna() & signal_line_series.notna() & kst_prev.notna() & signal_line_prev.notna()
        cross_below = (kst_series < signal_line_series) & (kst_prev >= signal_line_prev) & kst_series.notna() & signal_line_series.notna() & kst_prev.notna() & signal_line_prev.notna()

        # Initialize signal column to 0
        df['signal'] = 0
        df.loc[cross_above, 'signal'] = 1
        df.loc[cross_below, 'signal'] = -1

        # Apply long_only rule: change sell triggers (-1) to exit triggers (0)
        if self.long_only:
            df.loc[df['signal'] == -1, 'signal'] = 0

        # Select and return the relevant columns
        # Drop rows where essential indicators are NaN (usually at the beginning)
        df.dropna(subset=['kst', 'signal_line'], inplace=True)

        return df[['open', 'close', 'high', 'low', 'kst', 'signal_line', 'signal', 'signal_strength']]


    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate KST trading signals with integrated risk management for one or multiple tickers.

        Retrieves price data, calculates KST indicators and raw signals using
        `_calculate_signals_single`, applies risk management via `RiskManager`,
        computes returns, and returns a comprehensive DataFrame.
        **Note:** For multi-ticker requests, this method adds a 'date' column mirroring the index
        to ensure compatibility with the specific input processing logic of the StrategyOptimizer.

        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Backtest start date (YYYY-MM-DD).
            end_date (str, optional): Backtest end date (YYYY-MM-DD).
            initial_position (int): Starting position (0: flat, 1: long, -1: short).
                                     Note: RiskManager handles initial position state internally.
            latest_only (bool): If True, returns only the most recent row(s) for forecasting.

        Returns:
            pd.DataFrame: DataFrame containing price data, indicators, raw signals,
                          risk-managed positions, returns, and RM actions. See class
                          docstring for detailed column descriptions.
                          Returns an empty DataFrame if data is insufficient or errors occur.

        Raises:
            DataRetrievalError: If data cannot be retrieved from the database.
            Exception: Propagates other exceptions during processing.
        """
        try:
            # Determine minimum required lookback based on strategy periods
            max_roc = max(self.params['roc_periods']) if self.params['roc_periods'] else 0
            max_sma = max(self.params['sma_periods']) if self.params['sma_periods'] else 0
            min_lookback = max_roc + max_sma + self.params['signal_period'] + 2 # Add buffer for shifts/calcs

            # Retrieve historical price data using BaseStrategy method
            data = self.get_historical_prices(
                ticker,
                lookback=min_lookback if latest_only else None,
                from_date=start_date,
                to_date=end_date
            )

            if data.empty:
                self.logger.warning(f"No historical price data returned for {ticker}.")
                return pd.DataFrame()

            # --- Process based on single vs multiple tickers ---
            if isinstance(ticker, (list, tuple)) and len(ticker) > 1:
                # --- Multi-Ticker Processing ---
                if not isinstance(data.index, pd.MultiIndex):
                     self.logger.error("Multi-ticker request did not return a MultiIndex DataFrame. Cannot proceed.")
                     return pd.DataFrame()

                signals_list = []
                for ticker_name, group_data in data.groupby(level=0):
                    self.logger.debug(f"Processing ticker: {ticker_name}")
                    if not self._validate_data(group_data, min_records=min_lookback):
                        self.logger.warning(f"Insufficient data for {ticker_name} (required {min_lookback}). Skipping.")
                        continue

                    group_data_no_ticker_idx = group_data.reset_index(level=0, drop=True)
                    raw_signals_df = self._calculate_signals_single(group_data_no_ticker_idx)

                    if raw_signals_df.empty:
                         self.logger.warning(f"Signal calculation returned empty DataFrame for {ticker_name}. Skipping.")
                         continue

                    rm_processed_df = self.risk_manager.apply(raw_signals_df, initial_position)
                    rm_processed_df['daily_return'] = rm_processed_df['close'].pct_change().fillna(0)
                    rm_processed_df['strategy_return'] = rm_processed_df['daily_return'] * rm_processed_df['position'].shift(1).fillna(0)
                    rm_processed_df.rename(columns={'return': 'rm_strategy_return',
                                                    'cumulative_return': 'rm_cumulative_return',
                                                    'exit_type': 'rm_action'}, inplace=True)
                    rm_processed_df['ticker'] = ticker_name
                    signals_list.append(rm_processed_df)

                if not signals_list:
                    self.logger.warning("No signals generated for any ticker in the list.")
                    return pd.DataFrame()
                signals = pd.concat(signals_list) # Has DatetimeIndex and 'ticker' column

                # *** START MODIFICATION FOR OPTIMIZER COMPATIBILITY ***
                # Add a 'date' column explicitly from the index ONLY for multi-ticker case.
                # This allows the optimizer's `set_index(group.index.name or 'date')` to work
                # by finding this 'date' column when group.index.name is None.
                if not signals.empty and isinstance(signals.index, pd.DatetimeIndex):
                    signals['date'] = signals.index
                    self.logger.debug("Added 'date' column to multi-ticker output for optimizer compatibility.")
                # *** END MODIFICATION ***

                if latest_only:
                    signals = signals.sort_index() # Sort by date index first
                    signals = signals.groupby('ticker').tail(1)

            else:
                # --- Single-Ticker Processing ---
                ticker_name = ticker if isinstance(ticker, str) else ticker[0]
                self.logger.debug(f"Processing single ticker: {ticker_name}")

                if not self._validate_data(data, min_records=min_lookback):
                    self.logger.warning(f"Insufficient data for {ticker_name} (required {min_lookback}).")
                    return pd.DataFrame()

                raw_signals_df = self._calculate_signals_single(data)

                if raw_signals_df.empty:
                     self.logger.warning(f"Signal calculation returned empty DataFrame for {ticker_name}.")
                     return pd.DataFrame()

                signals = self.risk_manager.apply(raw_signals_df, initial_position)
                signals['daily_return'] = signals['close'].pct_change().fillna(0)
                signals['strategy_return'] = signals['daily_return'] * signals['position'].shift(1).fillna(0)
                signals.rename(columns={'return': 'rm_strategy_return',
                                        'cumulative_return': 'rm_cumulative_return',
                                        'exit_type': 'rm_action'}, inplace=True)

                # NOTE: No 'date' column added here, as single-ticker output is handled
                # differently by the optimizer and doesn't hit the problematic code block.

                if latest_only:
                    signals = signals.iloc[[-1]].copy()

            # --- Final Column Check and Ordering ---
            expected_cols = [
                'open', 'close', 'high', 'low', 'kst', 'signal_line', 'signal',
                'signal_strength', 'position', 'rm_strategy_return',
                'rm_cumulative_return', 'rm_action', 'daily_return', 'strategy_return'
            ]
            # Include 'ticker' and potentially 'date' if they exist in the final frame
            if 'ticker' in signals.columns and 'ticker' not in expected_cols:
                expected_cols.append('ticker')
            if 'date' in signals.columns and 'date' not in expected_cols:
                expected_cols.append('date')

            if not signals.empty:
                for col in expected_cols:
                    if col not in signals.columns:
                        signals[col] = np.nan # Add missing columns if any

                # Reorder columns for consistency
                final_cols_ordered = [col for col in expected_cols if col in signals.columns] + \
                                     [col for col in signals.columns if col not in expected_cols] # Add any unexpected extra cols at the end
                signals = signals[final_cols_ordered]

            self.logger.info(f"Successfully generated signals for {ticker}. Shape: {signals.shape}")
            return signals

        except DataRetrievalError as dre:
             self.logger.error(f"Data retrieval failed for {ticker}: {dre}")
             raise
        except ValueError as ve:
             self.logger.error(f"Parameter validation or calculation error for {ticker}: {ve}")
             raise
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred generating signals for {ticker}: {e}")
            raise