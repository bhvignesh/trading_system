# trading_system/src/strategies/momentum/macd_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig  # Assuming this exists
# Assuming BaseStrategy and DatabaseConfig are correctly imported
from src.strategies.base_strat import BaseStrategy, DataRetrievalError

# Assuming RiskManager is correctly imported from its location
try:
    # Adjust import path based on your project structure
    from src.strategies.risk_management import RiskManager
except ImportError:
    # Fallback for local testing if needed
    # from risk_management import RiskManager # Comment out if not needed locally
    raise ImportError("Could not import RiskManager. Ensure it's in the correct path.")


class MACDStrategy(BaseStrategy):
    """
    MACD Trading Strategy with Integrated Risk Management.

    Generates trading signals based on MACD crossovers and applies risk management.

    Mathematical formulation:
        - Fast EMA:      EMA(close, span=fast_period)
        - Slow EMA:      EMA(close, span=slow_period)
        - MACD line:     fast_ema - slow_ema ('macd')
        - Signal line:   EMA(macd_line, span=smooth_period) ('signal_line')
        - Histogram:     macd_line - signal_line ('histogram')

    Trading signals ('signal'):
        - Bullish (+1): MACD crosses above the signal line.
        - Bearish (-1): MACD crosses below the signal line (ignored if 'long_only'=True).

    Signal strength ('signal_strength'):
        - Absolute value of the MACD histogram.

    Risk Management Integration (via RiskManager):
        - Uses 'signal', 'open', 'high', 'low', 'close' as input.
        - Adjusts for costs/slippage, calculates stops/targets/trailing stops.
        - Outputs final 'position', risk-managed realized return ('rm_strategy_return'),
          risk-managed cumulative return ('rm_cumulative_return'), and exit reason ('rm_action').

    Output Format (`generate_signals` method):
        - Returns a dictionary where keys are ticker symbols (str) and values are
          pandas DataFrames.
        - Each DataFrame corresponds to a single ticker and has a DatetimeIndex.
        - Columns include:
            - Price data: 'open', 'high', 'low', 'close', 'volume'
            - MACD indicators: 'macd', 'signal_line', 'histogram'
            - Core signal: 'signal' (the crossover signal: 1, -1, or 0)
            - Signal strength: 'signal_strength'
            - Risk-managed results: 'position', 'rm_strategy_return',
                                    'rm_cumulative_return', 'rm_action'
            - Auxiliary performance metrics: 'daily_return', 'strategy_return'

    Args:
        db_config (DatabaseConfig): Database configuration object.
        params (Optional[Dict]): Strategy hyperparameters (see __init__ defaults).
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the MACD strategy.

        Args:
            db_config (DatabaseConfig): Database configuration.
            params (Optional[Dict]): Strategy hyperparameters. Keys include:
                'slow', 'fast', 'smooth', 'long_only', 'stop_loss_pct',
                'take_profit_pct', 'trailing_stop_pct', 'slippage_pct',
                'transaction_cost_pct'. Defaults are applied if keys are missing.

        Raises:
            ValueError: If slow period <= fast period, or periods are invalid/missing.
            ImportError: If RiskManager cannot be imported.
        """
        default_params = {
            'slow': 26, 'fast': 12, 'smooth': 9,
            'stop_loss_pct': 0.05, 'take_profit_pct': 0.10, 'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001, 'transaction_cost_pct': 0.001, 'long_only': True
        }
        # Resolve parameters: start with defaults, update with provided params
        resolved_params = default_params.copy()
        if params:
            resolved_params.update(params)

        super().__init__(db_config, resolved_params) # Pass final params to base
        self.logger = logging.getLogger(self.__class__.__name__)

        # Validate and store core strategy parameters, ensuring correct type
        try:
            # Use .get() with default=None to check presence before casting
            slow = self.params.get('slow')
            fast = self.params.get('fast')
            smooth = self.params.get('smooth')
            if slow is None or fast is None or smooth is None:
                 raise ValueError("MACD parameters 'slow', 'fast', and 'smooth' must be provided.")
            self.slow_period = int(slow)
            self.fast_period = int(fast)
            self.smooth_period = int(smooth)
        except (ValueError, TypeError) as e:
            # Catch explicit casting errors or initial ValueError
            raise ValueError(f"MACD periods (slow, fast, smooth) must be integer-convertible: {e}")

        if self.slow_period <= self.fast_period:
            raise ValueError(f"Slow period ({self.slow_period}) must be greater than fast period ({self.fast_period})")

        self.long_only = self.params.get('long_only', True) # Default handled by .get()

        # Initialize RiskManager using parameters stored in self.params
        risk_params = {
            'stop_loss_pct': self.params.get('stop_loss_pct'),
            'take_profit_pct': self.params.get('take_profit_pct'),
            'trailing_stop_pct': self.params.get('trailing_stop_pct'),
            'slippage_pct': self.params.get('slippage_pct'),
            'transaction_cost_pct': self.params.get('transaction_cost_pct')
        }
        try:
            # Filter out None values if RM constructor doesn't handle them gracefully
            # risk_params = {k: v for k, v in risk_params.items() if v is not None}
            self.risk_manager = RiskManager(**risk_params)
            self.logger.info(f"MACD Strategy initialized: Periods(F/S/Sm)=({self.fast_period}/{self.slow_period}/{self.smooth_period}), "
                             f"LongOnly={self.long_only}, RiskParams={risk_params}")
        except (ValueError, TypeError) as e:
            self.logger.error(f"Failed to initialize RiskManager with params {risk_params}: {e}")
            raise ValueError(f"Invalid risk management parameters: {e}") # Propagate as config error


    def _calculate_signals_single(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate MACD indicators and raw signals ('signal') for a single ticker.

        Args:
            data (pd.DataFrame): Price data (OHLCV) with DatetimeIndex.

        Returns:
            pd.DataFrame: Enhanced DataFrame with 'macd', 'signal_line', 'histogram',
                          'signal', 'signal_strength', plus original OHLCV.
                          Returns empty DataFrame on failure or insufficient data.
        """
        # Minimum data required for reliable calculation (EMA stabilization + smoothing)
        min_required_data = max(self.slow_period * 2, self.fast_period * 2) + self.smooth_period + 10 # Adjusted heuristic
        if not self._validate_data(data, min_records=min_required_data):
            # Logger warning handled by _validate_data
            return pd.DataFrame()

        # Ensure required columns are present before calculations
        required_input_cols = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_input_cols):
             missing = [col for col in required_input_cols if col not in data.columns]
             self.logger.error(f"Input data missing required columns for MACD calculation: {missing}")
             return pd.DataFrame()

        df = data.copy()

        # Calculate MACD indicators
        try:
            fast_ema = df['close'].ewm(span=self.fast_period, adjust=False).mean()
            slow_ema = df['close'].ewm(span=self.slow_period, adjust=False).mean()
            df['macd'] = fast_ema - slow_ema
            df['signal_line'] = df['macd'].ewm(span=self.smooth_period, adjust=False).mean()
            df['histogram'] = df['macd'] - df['signal_line']
        except Exception as e:
             self.logger.error(f"Error calculating MACD indicators: {e}")
             return pd.DataFrame() # Return empty on calculation error

        # Drop initial NaNs resulting from EMA calculations for cleaner signal logic
        df = df.dropna(subset=['macd', 'signal_line', 'histogram'])
        if df.empty:
             self.logger.warning("DataFrame became empty after dropping NaNs from MACD indicator calculation.")
             return pd.DataFrame()

        # Generate crossover signals
        above = df['macd'] > df['signal_line']
        below = df['macd'] < df['signal_line']
        # Use fillna(False) for the first comparison after shift
        # Adding explicit type casting to potentially mitigate FutureWarning if it persists
        above_shifted = above.shift(1).fillna(False).infer_objects(copy=False).astype(bool)
        below_shifted = below.shift(1).fillna(False).infer_objects(copy=False).astype(bool)
        cross_up = above & (~above_shifted)
        cross_down = below & (~below_shifted)

        # Core signal column named 'signal'
        df['signal'] = 0
        df.loc[cross_up, 'signal'] = 1
        if not self.long_only:
            df.loc[cross_down, 'signal'] = -1

        # Calculate signal strength
        df['signal_strength'] = df['histogram'].abs()

        # Select and return required columns for the next stage (Risk Manager)
        # plus the calculated indicators and strength
        required_output_cols = ['open', 'high', 'low', 'close', 'volume',
                                'macd', 'signal_line', 'histogram',
                                'signal', 'signal_strength']

        # Final check if all expected columns are present
        if not all(col in df.columns for col in required_output_cols):
             missing = [col for col in required_output_cols if col not in df.columns]
             self.logger.error(f"Internal error: Missing columns before returning from _calculate_signals_single: {missing}")
             return pd.DataFrame()

        return df[required_output_cols].copy() # Return a copy


    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate MACD trading signals with integrated risk management.

        Args:
            ticker (str or List[str]): Ticker symbol(s).
            start_date (str, optional): Start date (YYYY-MM-DD).
            end_date (str, optional): End date (YYYY-MM-DD).
            initial_position (int): Initial position per ticker (can be overridden
                                    by RiskManager if passed a dict).
            latest_only (bool): If True, return only the last row per ticker
                                in the dictionary values.

        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping ticker symbols to their
                                     results DataFrames. Each DataFrame has a
                                     DatetimeIndex. Returns empty dict on error.
                                     See class docstring for column details.
        """
        all_signals = [] # List to store pandas dataframes
        try:
            # --- Data Retrieval ---
            # Calculate buffer needed for stable indicators and RM lookback
            lookback_buffer = max(self.slow_period * 3, 60) + self.smooth_period + 30 # Heuristic
            if latest_only:
                 self.logger.debug(f"Latest only mode: Using lookback={lookback_buffer} bars.")
                 data = self.get_historical_prices(ticker, lookback=lookback_buffer)
            else:
                 # Fetch data for the range, BaseStrategy handles buffer if dates are None
                 self.logger.debug(f"Fetching data for {ticker} from {start_date} to {end_date} with lookback buffer.")
                 data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date, lookback=lookback_buffer)

            if data.empty:
                self.logger.warning(f"No historical data retrieved for ticker(s): {ticker}. Parameters: start={start_date}, end={end_date}, latest={latest_only}")
                return {}

            # --- Process Data ---
            is_multi_ticker = isinstance(ticker, list) and len(ticker) > 1
            # Check if data format matches expectation based on request type
            if is_multi_ticker and not isinstance(data.index, pd.MultiIndex):
                self.logger.error(f"Expected MultiIndex data for multiple tickers {ticker}, but received {type(data.index)}.")
                return {}
            if not is_multi_ticker and not isinstance(data.index, pd.DatetimeIndex):
                 # Handle case where single ticker in list might still return MultiIndex from DB layer
                 if isinstance(data.index, pd.MultiIndex) and isinstance(ticker, list) and len(ticker)==1:
                      ticker_name_single = ticker[0]
                      self.logger.warning(f"Received MultiIndex for single ticker request '{ticker_name_single}'. Extracting data.")
                      try:
                           data = data.loc[ticker_name_single] # Extract single ticker's data
                           if not isinstance(data.index, pd.DatetimeIndex): # Check again after extraction
                                raise TypeError("Index not DatetimeIndex after extraction.")
                      except Exception as ex_err:
                           self.logger.error(f"Failed to extract single ticker data from MultiIndex for {ticker_name_single}: {ex_err}")
                           return {}
                 else:
                      self.logger.error(f"Expected DatetimeIndex data for single ticker {ticker}, but received {type(data.index)}.")
                      return {}

            # --- Loop through Tickers (or process single) ---
            if is_multi_ticker:
                ticker_level_name = data.index.names[0] if data.index.names[0] is not None else 0
                if not data.index.is_monotonic_increasing:
                    self.logger.warning("Input MultiIndex data is not sorted. Sorting...")
                    data = data.sort_index()

                for ticker_name, group_data in data.groupby(level=ticker_level_name, sort=False):
                    # Reset index to get DatetimeIndex for processing
                    group_data_dt_idx = group_data.reset_index(level=ticker_level_name, drop=True)
                    self.logger.debug(f"Processing {ticker_name}...")
                    processed_df = self._process_dataframe(group_data_dt_idx, initial_position, ticker_name)
                    if processed_df is not None:
                         processed_df['ticker'] = ticker_name
                         if latest_only:
                             if not processed_df.empty:
                                 all_signals.append(processed_df.iloc[[-1]].copy())
                         else:
                             all_signals.append(processed_df)
            else:
                # Process single ticker (data already has DatetimeIndex)
                ticker_name = ticker if isinstance(ticker, str) else ticker[0]
                self.logger.debug(f"Processing single ticker {ticker_name}...")
                 # Handle initial position correctly for single ticker
                rm_initial_pos = initial_position
                if isinstance(initial_position, dict):
                    rm_initial_pos = initial_position.get(ticker_name, 0)
                elif not isinstance(initial_position, int):
                    self.logger.warning(f"Initial position for single ticker {ticker_name} is not int, using 0.")
                    rm_initial_pos = 0

                processed_df = self._process_dataframe(data, rm_initial_pos, ticker_name)
                if processed_df is not None:
                        processed_df['ticker'] = ticker_name
                        if latest_only:
                            if not processed_df.empty:
                                all_signals.append(processed_df.iloc[[-1]].copy())
                        else:
                            all_signals.append(processed_df)
            signals_df = pd.concat(all_signals)
            # Method 1: Reset index first, then set new MultiIndex (Often cleaner)
            df_reset = signals_df.reset_index() # 'date' becomes a regular column
            # Now set the MultiIndex using the 'ticker' and 'date' columns
            df_multi_index = df_reset.set_index(['ticker', 'date'])
            return df_multi_index # Return the dictionary of results

        except DataRetrievalError as e:
            self.logger.error(f"Data retrieval failed during signal generation: {e}")
            return {}
        except ValueError as e: # Catch validation errors (e.g., from __init__)
             self.logger.error(f"Configuration or parameter error: {e}")
             raise # Re-raise config errors as they indicate setup problems
        except Exception as e:
            self.logger.exception(f"Unexpected error generating signals for {ticker}: {e}") # Log full traceback
            return {} # Return empty dict on unexpected errors


    def _process_dataframe(self, df_input: pd.DataFrame, initial_pos: int, ticker_id: str) -> Optional[pd.DataFrame]:
        """
        Internal helper to process a single ticker's DataFrame (calculate signals, apply RM, finalize).

        Args:
            df_input (pd.DataFrame): DataFrame for one ticker with DatetimeIndex.
            initial_pos (int): Initial position for this ticker.
            ticker_id (str): Identifier for logging.

        Returns:
            Optional[pd.DataFrame]: Processed DataFrame or None if processing fails.
        """
        # 1. Calculate indicators and raw signals ('signal' column)
        signals_indicators = self._calculate_signals_single(df_input)
        if signals_indicators.empty:
            self.logger.warning(f"Signal calculation failed or returned empty for {ticker_id}. Skipping.")
            return None

        # 2. Apply Risk Management
        try:
            # Ensure all required columns are present for RM
            required_rm_cols = ['signal', 'open', 'high', 'low', 'close']
            if not all(col in signals_indicators.columns for col in required_rm_cols):
                missing = [col for col in required_rm_cols if col not in signals_indicators.columns]
                self.logger.error(f"DataFrame for {ticker_id} missing columns required by RiskManager: {missing}. Skipping.")
                return None

            rm_results = self.risk_manager.apply(signals_indicators[required_rm_cols + ['volume']], initial_pos) # Pass volume if needed downstream
            if rm_results.empty: # Check if RM returned empty
                 self.logger.warning(f"RiskManager returned empty DataFrame for {ticker_id}. Skipping.")
                 return None
        except Exception as rm_err:
            self.logger.error(f"RiskManager failed for {ticker_id}: {rm_err}", exc_info=True)
            return None

        # 3. Combine results, rename RM columns, add back indicators/strength
        # Start with RM results, which should have the correct index
        final_df = rm_results.rename(columns={
            'return': 'rm_strategy_return',
            'cumulative_return': 'rm_cumulative_return',
            'exit_type': 'rm_action'
            # 'signal' column name is preserved correctly by RM and is the core signal
        })

        # Join back indicators and strength calculated earlier, aligning on index
        indicator_cols = ['macd', 'signal_line', 'histogram', 'signal_strength']
        final_df = final_df.join(signals_indicators[indicator_cols], how='left')

        # 4. Calculate auxiliary returns based on final RM position
        try:
             # Ensure 'close' and 'position' columns are present after RM and join
             if 'close' not in final_df.columns or 'position' not in final_df.columns:
                  raise KeyError("Missing 'close' or 'position' column after risk management.")

             final_df['daily_return'] = final_df['close'].pct_change().fillna(0)
             final_df['strategy_return'] = final_df['daily_return'] * final_df['position'].shift(1).fillna(0)
        except Exception as aux_calc_err:
             self.logger.error(f"Failed to calculate auxiliary returns for {ticker_id}: {aux_calc_err}")
             # Decide if this is critical - perhaps return df without these? For now, treat as failure.
             return None

        # 5. Ensure all expected columns are present in the final output
        expected_final_cols = [
            'open', 'high', 'low', 'close', 'volume', 'macd', 'signal_line',
            'histogram', 'signal', 'signal_strength', 'position', 'daily_return',
            'strategy_return', 'rm_strategy_return', 'rm_cumulative_return', 'rm_action'
        ]
        missing_final = [col for col in expected_final_cols if col not in final_df.columns]
        if missing_final:
            self.logger.warning(f"Final DataFrame for {ticker_id} is missing expected columns: {missing_final}. Adding as NaN.")
            for col in missing_final:
                 final_df[col] = np.nan # Add missing columns

        return final_df[expected_final_cols].copy() # Return selected columns in standard order