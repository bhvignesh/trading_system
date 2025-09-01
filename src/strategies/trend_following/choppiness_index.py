# trading_system/src/strategies/choppiness_index_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class ChoppinessIndexStrategy(BaseStrategy):
    """
    Choppiness Index Strategy with MACD confirmation and integrated Risk Management.

    This strategy identifies trading opportunities based on market choppiness and
    trend confirmation using MACD.

    1.  **Indicator Calculation**:
        *   **True Range (TR)**: Calculated based on high, low, and previous close.
        *   **Choppiness Index (CI)**: Measures market trendiness.
            `CI = 100 * log10( sum(TR, n) / (max(high, n) - min(low, n)) ) / log10(n)`.
            A low CI suggests a trending market; a high CI suggests a choppy/consolidating market.
            Default period `n` (ci_period) is 14.
        *   **MACD**: Standard MACD (Fast EMA - Slow EMA), Signal Line (EMA of MACD),
            and Histogram (MACD - Signal Line).

    2.  **Signal Generation**:
        *   **Bullish Signal**: MACD > Signal Line AND CI < 38.2 (market is trending).
        *   **Bearish Signal**: MACD < Signal Line AND CI < 38.2 (market is trending).
            If `long_only` is True, bearish signals are treated as exit signals (0).
        *   **Signal Strength**: Calculated as
            `(|Histogram| / Close Price) * (1 - (CI / 38.2))`,
            only when CI < 38.2. This metric quantifies the signal's conviction.
        *   Signals are forward-filled to maintain positions until a counter-signal or
            exit condition occurs.

    3.  **Risk Management**:
        *   Utilizes an external `RiskManager` class to apply stop-loss, take-profit,
            trailing stops, slippage, and transaction costs to the raw signals.
        *   Calculates risk-managed positions and returns.

    The strategy supports backtesting over specified date ranges and forecasting
    (returning the latest signal). It can process single or multiple tickers.

    Args:
        db_config (DatabaseConfig): Configuration for database connections.
        params (dict, optional): Strategy-specific parameters. Includes:
            - 'ci_period' (int): Period for Choppiness Index (default: 14).
            - 'macd_fast' (int): Fast EMA period for MACD (default: 12).
            - 'macd_slow' (int): Slow EMA period for MACD (default: 26).
            - 'macd_smooth' (int): Signal line EMA period for MACD (default: 9).
            - 'long_only' (bool): If True, only long positions are taken (default: True).
            - Risk management parameters (e.g., 'stop_loss_pct', 'take_profit_pct', etc.)
              are passed to the `RiskManager`.

    Returns:
        pd.DataFrame: A DataFrame indexed by date (and ticker, if multiple tickers)
        containing price data, indicators, raw signals, risk-managed positions,
        returns, and exit types.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        super().__init__(db_config, params)
        self.long_only = self.params.get('long_only', True)
        # Initialize RiskManager with parameters from the strategy's params
        self.risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05),
            take_profit_pct=self.params.get('take_profit_pct', 0.10),
            trailing_stop_pct=self.params.get('trailing_stop_pct', 0.0),
            slippage_pct=self.params.get('slippage_pct', 0.001),
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001)
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generates trading signals with integrated risk management.

        Handles data retrieval, indicator calculation, signal generation, and
        risk management for one or more tickers, for backtesting or forecasting.

        Args:
            ticker (Union[str, List[str]]): A single ticker symbol or a list of symbols.
            start_date (Optional[str]): Backtest start date (YYYY-MM-DD).
                                        Used for historical analysis.
            end_date (Optional[str]): Backtest end date (YYYY-MM-DD).
                                      Used for historical analysis.
            initial_position (int): The initial trading position (1 for long, -1 for short,
                                    0 for flat). Can be a dict for multi-ticker.
            latest_only (bool): If True, retrieves minimal data and returns only the
                                most recent signal row(s) for forecasting.

        Returns:
            pd.DataFrame: DataFrame with price data, signals, risk-managed positions,
                          realized returns, cumulative returns, and exit types.
                          Columns include 'close', 'high', 'low', 'signal', 'position',
                          'return', 'cumulative_return', 'exit_type'.

        Raises:
            DataRetrievalError: If insufficient data is available for any/all tickers
                                after validation.
        """
        # Determine lookback period based on the maximum of CI period and MACD slow period.
        # This ensures enough data for indicator calculations.
        ci_period_val = self.params.get('ci_period', 14)
        macd_slow_val = self.params.get('macd_slow', 26)
        indicator_calc_lookback = max(ci_period_val, macd_slow_val)
        
        # For data retrieval, fetch a bit more (e.g., 2x) to ensure enough points after NaNs from shifts/rolling windows.
        data_retrieval_lookback = indicator_calc_lookback * 2

        # Retrieve historical price data
        raw_prices: pd.DataFrame
        if isinstance(ticker, list):
            if latest_only:
                # For multi-ticker forecasting, fetch only 'data_retrieval_lookback' recent records per ticker
                raw_prices = self.get_historical_prices(
                    tickers=ticker,
                    lookback=data_retrieval_lookback
                )
            else:
                # For multi-ticker backtesting, fetch data within the date range
                raw_prices = self.get_historical_prices(
                    tickers=ticker,
                    from_date=start_date,
                    to_date=end_date
                )
            
            if raw_prices.empty:
                self.logger.error("No data retrieved for the given list of tickers.")
                raise DataRetrievalError("No data retrieved for the given list of tickers.")

            # Per-ticker validation for multi-ticker data
            valid_prices_list = []
            if 'ticker' in raw_prices.index.names: # Check if MultiIndex with 'ticker'
                for ticker_name, group in raw_prices.groupby(level='ticker'):
                    # _validate_data expects a DataFrame, group is already a DataFrame
                    if self._validate_data(group, min_records=data_retrieval_lookback):
                        valid_prices_list.append(group)
                    else:
                        self.logger.warning(
                            f"Ticker {ticker_name} has insufficient data ({len(group)} records, "
                            f"need {data_retrieval_lookback}). Skipping."
                        )
                if not valid_prices_list:
                    self.logger.error("No tickers remaining after data validation.")
                    raise DataRetrievalError("No tickers remaining after data validation.")
                prices = pd.concat(valid_prices_list)
            else: # Should not happen if get_historical_prices for list returns correctly
                 self.logger.error("Multi-ticker data retrieval did not produce a correctly indexed DataFrame.")
                 raise DataRetrievalError("Multi-ticker data format error after retrieval.")

        else: # Single ticker
            prices = self._get_prices_with_lookback(
                ticker, start_date, end_date, indicator_calc_lookback, latest_only
            )
            # Validate data for the single ticker
            if not self._validate_data(prices, min_records=data_retrieval_lookback):
                self.logger.error(f"Insufficient data for ticker {ticker} for analysis.")
                raise DataRetrievalError(f"Insufficient data for ticker {ticker} for analysis.")

        if prices.empty: # Should be caught by specific validation, but as a safeguard
            self.logger.error("Price data is empty after retrieval and validation.")
            raise DataRetrievalError("Price data is empty after retrieval and validation.")

        # Calculate CI and MACD indicators
        df_with_indicators = self._calculate_indicators(prices)

        if df_with_indicators.empty:
            self.logger.warning("DataFrame is empty after indicator calculation. No signals can be generated.")
            # Return an empty DataFrame with expected columns if no data to process
            return self._format_output(pd.DataFrame(), latest_only=False)


        # Generate raw trading signals
        signals_df = self._generate_vectorized_signals(df_with_indicators)

        # Apply risk management. RiskManager handles multi-ticker DataFrames internally.
        results = self.risk_manager.apply(signals_df, initial_position)

        return self._format_output(results, latest_only)

    def _get_prices_with_lookback(self, ticker: str, start_date: Optional[str],
                                  end_date: Optional[str], indicator_lookback: int,
                                  latest_only: bool) -> pd.DataFrame:
        """
        Helper to retrieve historical price data for a single ticker, handling
        the lookback requirements for forecasting versus backtesting.

        Args:
            ticker (str): Stock ticker symbol.
            start_date (Optional[str]): Backtest start date (YYYY-MM-DD).
            end_date (Optional[str]): Backtest end date (YYYY-MM-DD).
            indicator_lookback (int): Base lookback period needed for indicator calculation.
            latest_only (bool): If True, fetches `indicator_lookback * 2` records for forecasting.

        Returns:
            pd.DataFrame: DataFrame with historical prices, indexed by date.
        """
        if latest_only:
            # For forecasting, fetch 2x indicator_lookback recent records
            return self.get_historical_prices(
                tickers=ticker,
                lookback=indicator_lookback * 2, # data_retrieval_lookback
                from_date=None, # Ensure no date restrictions
                to_date=None
            )
        # For backtesting, fetch data within the specified date range
        return self.get_historical_prices(
            tickers=ticker,
            from_date=start_date,
            to_date=end_date
            # No explicit lookback here; date range defines data scope
        )

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates and appends Choppiness Index (CI) and MACD indicators.

        Handles both single-ticker (DatetimeIndex) and multi-ticker (MultiIndex
        with 'ticker' level) DataFrames. For multi-ticker, calculations are
        performed per group.

        Args:
            df (pd.DataFrame): Price DataFrame with 'high', 'low', 'close'.
                               Index is DatetimeIndex or MultiIndex ['ticker', 'date'].

        Returns:
            pd.DataFrame: DataFrame with 'ci', 'macd', 'signal_line', 'histogram'
                          columns added. Rows with NaN indicators are dropped.
        """
        ci_period = int(self.params.get('ci_period', 14))
        fast = int(self.params.get('macd_fast', 12))
        slow = int(self.params.get('macd_slow', 26))
        smooth = int(self.params.get('macd_smooth', 9))

        # Define indicator calculation logic for a single series/group
        def calculate_group_indicators(group_df: pd.DataFrame) -> pd.DataFrame:
            group_df = group_df.sort_index() # Ensure date sorting within group
            
            # True Range
            prev_close = group_df['close'].shift(1)
            tr1 = group_df['high'] - group_df['low']
            tr2 = abs(group_df['high'] - prev_close)
            tr3 = abs(group_df['low'] - prev_close)
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # Choppiness Index
            sum_tr = tr.rolling(window=ci_period, min_periods=ci_period).sum()
            max_high = group_df['high'].rolling(window=ci_period, min_periods=ci_period).max()
            min_low = group_df['low'].rolling(window=ci_period, min_periods=ci_period).min()
            
            range_hl = max_high - min_low
            # Handle range_hl = 0 to prevent division by zero and ensure NaN propagation
            ci_val = 100 * np.log10(sum_tr / range_hl.replace(0, np.nan)) / np.log10(ci_period)
            group_df['ci'] = ci_val

            # MACD
            exp_fast = group_df['close'].ewm(span=fast, adjust=False).mean()
            exp_slow = group_df['close'].ewm(span=slow, adjust=False).mean()
            group_df['macd'] = exp_fast - exp_slow
            group_df['signal_line'] = group_df['macd'].ewm(span=smooth, adjust=False).mean()
            group_df['histogram'] = group_df['macd'] - group_df['signal_line']
            
            return group_df.dropna(subset=['ci', 'macd', 'signal_line', 'histogram'])

        if 'ticker' in df.index.names: # Multi-ticker DataFrame
            # Apply calculations per ticker group
            # Ensure groups are processed correctly if they are already DataFrames
            processed_df = df.groupby(level='ticker', group_keys=False).apply(calculate_group_indicators)
        else: # Single-ticker DataFrame
            processed_df = calculate_group_indicators(df.copy()) # Use .copy() to avoid SettingWithCopyWarning

        return processed_df


    def _generate_vectorized_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates trading signals based on CI and MACD criteria.

        Bullish: MACD > Signal Line and CI < 38.2.
        Bearish: MACD < Signal Line and CI < 38.2 (or exit if long_only).
        Signal strength is calculated. Signals are forward-filled per ticker.

        Args:
            df (pd.DataFrame): DataFrame with prices and indicators ('ci', 'macd',
                               'signal_line', 'histogram', 'close', 'high', 'low').
                               Index can be DatetimeIndex or MultiIndex ['ticker', 'date'].

        Returns:
            pd.DataFrame: DataFrame with 'signal' and 'strength' columns added.
                          Original price columns ('close', 'high', 'low') are retained.
        """
        # Define bullish and bearish conditions
        bullish_condition = (df['macd'] > df['signal_line']) & (df['ci'] < 38.2)
        bearish_condition = (df['macd'] < df['signal_line']) & (df['ci'] < 38.2)

        # Calculate signal strength
        df['strength'] = 0.0
        # Mask for conditions where strength calculation is valid (CI < 38.2)
        strength_calc_mask = df['ci'] < 38.2
        
        # Calculate strength values safely, handling potential division by zero in 'close'
        # Strength is only non-zero where strength_calc_mask is True
        # Temporarily create series for calculation to handle NaNs from division by zero
        safe_close = df.loc[strength_calc_mask, 'close'].replace(0, np.nan)
        strength_values = (abs(df.loc[strength_calc_mask, 'histogram']) / safe_close) * \
                          (1 - (df.loc[strength_calc_mask, 'ci'] / 38.2))
        
        # Assign calculated strength, filling any NaNs (e.g. from division by zero) with 0
        df.loc[strength_calc_mask, 'strength'] = strength_values.fillna(0.0)


        # Generate raw signals (1 for bullish, -1 for bearish, 0 for neutral/exit)
        df['raw_signal'] = 0
        df.loc[bullish_condition, 'raw_signal'] = 1
        if self.long_only:
            # In long-only mode, bearish conditions trigger an exit (signal 0)
            # but the actual exit logic is handled by raw_signal becoming 0
            # and RiskManager potentially exiting based on this.
            # If a bearish condition is met, it effectively means "close long position",
            # which is represented by raw_signal 0 if already long.
            # If a new bearish signal appears while flat, it's ignored.
            # The crucial part is how ffill handles this.
            # To exit a long: a bearish signal (which implies CI < 38.2) should change raw_signal to 0.
            # What if we are flat and bearish happens? raw_signal remains 0. Correct.
            # What if we are long (signal=1) and bearish happens? raw_signal should become 0 to exit.
            df.loc[bearish_condition & (df['raw_signal'].shift(1).fillna(0) == 1), 'raw_signal'] = 0
            # Note: The original logic was simpler: df.loc[bearish, 'raw_signal'] = 0.
            # This implies bearish signals immediately mean "go flat" or "stay flat".
            # The ffill logic then carries forward the last non-zero signal.
            # Let's stick to the original clearer intent for long_only:
            df.loc[bearish_condition, 'raw_signal'] = 0 # Effectively, "don't go short" or "exit long if this is a reversal"
        else:
            df.loc[bearish_condition, 'raw_signal'] = -1

        # Forward-fill signals to maintain positions until a change.
        # This must be done per-ticker if df is multi-indexed.
        if 'ticker' in df.index.names:
            # Use pd.NA for temporary missing values to work with ffill on potentially Int64 dtypes
            temp_signal = df['raw_signal'].replace(0, pd.NA) 
            temp_signal = temp_signal.groupby(level='ticker').ffill()
            df['signal'] = temp_signal.fillna(0).astype(int)
        else: # Single ticker
            df['signal'] = df['raw_signal'].replace(0, np.nan).ffill().fillna(0).astype(int)
        
        return df[['close', 'high', 'low', 'signal', 'strength']]


    def _format_output(self, df: pd.DataFrame, latest_only: bool) -> pd.DataFrame:
        """
        Formats the output DataFrame to include essential columns and handles
        the `latest_only` flag for forecasting.

        Args:
            df (pd.DataFrame): DataFrame with processed signals and risk metrics.
            latest_only (bool): If True, returns only the last observation per ticker.

        Returns:
            pd.DataFrame: Formatted DataFrame with columns: 'close', 'high', 'low',
                          'signal', 'position', 'return', 'cumulative_return', 'exit_type'.
        """
        # Handle empty DataFrame case (e.g., if no signals generated after indicator calculation)
        if df.empty:
            # Define expected columns for an empty output DataFrame
            empty_cols = ['close', 'high', 'low', 'signal', 'position', 
                          'return', 'cumulative_return', 'exit_type']
            return pd.DataFrame(columns=empty_cols)

        # Ensure all required columns are present, adding NaNs if not (e.g. from RiskManager if no trades)
        required_cols = ['close', 'high', 'low', 'signal', 'position',
                         'return', 'cumulative_return', 'exit_type']
        for col in required_cols:
            if col not in df.columns:
                df[col] = np.nan # Add missing columns with NaNs
        
        # Fill NaNs for specific columns that should be 0 if no activity
        df['position'] = df['position'].fillna(0).astype(int)
        df['return'] = df['return'].fillna(0.0)
        df['cumulative_return'] = df['cumulative_return'].ffill().fillna(0.0) # ffill then 0 for initial NaNs
        df['exit_type'] = df['exit_type'].fillna('none')


        # Select and reorder columns
        df = df[required_cols]

        if latest_only:
            if not df.empty: # Ensure df is not empty before trying to get tail
                if 'ticker' in df.index.names:
                    df = df.groupby(level='ticker', group_keys=False).tail(1)
                else:
                    df = df.iloc[[-1]]
        return df

    def _validate_data(self, df: pd.DataFrame, min_records: int) -> bool:
        """
        Validates that the input DataFrame has sufficient and clean price data.

        Checks for:
        - Non-empty DataFrame.
        - Minimum number of records.
        - No missing values in 'close', 'high', 'low' columns.

        Args:
            df (pd.DataFrame): DataFrame to validate. Should have a DatetimeIndex.
            min_records (int): Minimum number of records required.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        if df.empty:
            self.logger.error("Data validation failed: DataFrame is empty.")
            return False
        if len(df) < min_records:
            self.logger.error(
                f"Data validation failed: Insufficient records. "
                f"Need at least {min_records}, got {len(df)}."
            )
            return False
        
        required_price_cols = ['close', 'high', 'low']
        missing_cols = [col for col in required_price_cols if col not in df.columns]
        if missing_cols:
            self.logger.error(f"Data validation failed: Missing required price columns: {missing_cols}.")
            return False

        if df[required_price_cols].isnull().any().any():
            self.logger.error("Data validation failed: Missing price data (NaNs) in 'close', 'high', or 'low'.")
            return False
        
        return True