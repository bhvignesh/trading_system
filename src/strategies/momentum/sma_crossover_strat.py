# trading_system/src/strategies/momentum/sma_crossover_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import \
    RiskManager  # Ensure correct import path

# Configure logger for this module
logger = logging.getLogger(__name__)

class SMAStrategy(BaseStrategy):
    """
    Implements an optimized Simple Moving Average (SMA) Crossover trading strategy
    with integrated risk management. Optimizes by calculating indicators vectorially
    before applying risk management per ticker.

    Signal Generation Logic: (Same as before)
    Risk Management: (Same as before)
    Output Columns: (Same as before)
    Compatibility: (Same as before)
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """ Initializes the SMA strategy (Identical to previous version). """
        default_params = {
            'short_window': 20, 'long_window': 50, 'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10, 'trailing_stop_pct': 0.0, 'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001, 'long_only': True
        }
        merged_params = {**default_params, **(params or {})}
        super().__init__(db_config, merged_params)

        try:
            self.short_window = int(self.params.get('short_window'))
            self.long_window = int(self.params.get('long_window'))
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid window parameters: {e}. short={self.params.get('short_window')}, long={self.params.get('long_window')}")
            raise ValueError("Window parameters must be convertible to integers.") from e

        self.long_only = self.params.get('long_only')

        if self.short_window >= self.long_window:
            raise ValueError(f"short_window ({self.short_window}) must be less than long_window ({self.long_window}).")

        risk_params = {
            'stop_loss_pct': self.params.get('stop_loss_pct'),
            'take_profit_pct': self.params.get('take_profit_pct'),
            'trailing_stop_pct': self.params.get('trailing_stop_pct'),
            'slippage_pct': self.params.get('slippage_pct'),
            'transaction_cost_pct': self.params.get('transaction_cost_pct')
        }
        self.risk_manager = RiskManager(**risk_params)
        logger.debug(f"Initialized SMA Strategy (Optimized): short={self.short_window}, long={self.long_window}, long_only={self.long_only}, Risk Params={risk_params}")


    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate SMA crossover signals and apply risk management (Optimized).

        Handles single/multiple tickers and backtesting/latest signal modes.
        Calculates indicators vectorially before applying risk management.
        """
        lookback_required = self.long_window + 1

        try:
            # --- Mode 1: Latest Signal Only (Largely unchanged, already efficient) ---
            if latest_only:
                # (Keeping the previous latest_only logic as it was reasonably efficient)
                logger.debug(f"Generating latest signal only for {ticker} with lookback {lookback_required}")
                data = self.get_historical_prices(ticker, lookback=lookback_required)
                if data.empty: return pd.DataFrame()

                latest_signal_cols = ['signal', 'strength', 'close', 'short_sma', 'long_sma']

                if isinstance(ticker, list):
                    all_signals = self._calculate_smas_and_signals_vectorized(data)
                    if all_signals.empty: return pd.DataFrame()

                    latest_signals = all_signals.groupby(level='ticker').tail(1)
                    # Ensure columns and add ticker column if needed
                    latest_signals = latest_signals.reset_index() # Get ticker and date as columns
                    for col in latest_signal_cols:
                         if col not in latest_signals: latest_signals[col] = np.nan
                    # Return with 'ticker' column and 'date' index seems consistent
                    return latest_signals[['ticker', 'date'] + latest_signal_cols].set_index('date')


                else: # Single ticker
                    if not isinstance(data.index, pd.DatetimeIndex): return pd.DataFrame() # Basic check
                    # For single ticker, _calculate_smas_and_signals_vectorized handles it
                    signals_df = self._calculate_smas_and_signals_vectorized(data)
                    if signals_df.empty: return pd.DataFrame()

                    latest_row = signals_df.iloc[-1:]
                    for col in latest_signal_cols:
                        if col not in latest_row: latest_row[col] = np.nan
                    return latest_row[latest_signal_cols]

            # --- Mode 2: Backtesting (Full Period - Optimized) ---
            else:
                logger.debug(f"Generating full backtest signals (Optimized) for {ticker} from {start_date} to {end_date}")

                # Calculate buffer start date (same as before)
                try:
                    buffer_days = int(lookback_required * 1.5) + 5
                    buffer_start_dt = pd.to_datetime(start_date) - pd.Timedelta(days=buffer_days)
                    fetch_start_date = buffer_start_dt.strftime('%Y-%m-%d')
                except Exception as date_err:
                    logger.error(f"Error calculating buffer start date from {start_date}: {date_err}")
                    return pd.DataFrame()

                # Fetch data including the buffer
                data = self.get_historical_prices(ticker, from_date=fetch_start_date, to_date=end_date)
                if data.empty:
                     logger.warning(f"No data retrieved for backtest: {ticker} [{fetch_start_date}-{end_date}]")
                     return pd.DataFrame()

                # Basic validation on the whole fetched data
                required_cols = ['open', 'high', 'low', 'close']
                if not all(col in data.columns for col in required_cols):
                    logger.error(f"Fetched data missing required columns: {required_cols}. Cannot proceed.")
                    return pd.DataFrame()

                # --- Step 1: Calculate Signals Vectorially on Buffered Data ---
                logger.debug(f"Calculating signals vectorially on data shape: {data.shape}")
                signals_with_indicators = self._calculate_smas_and_signals_vectorized(data)
                if signals_with_indicators.empty:
                    logger.warning("Vectorized signal calculation returned empty DataFrame.")
                    return pd.DataFrame()
                logger.debug(f"Vectorized signals calculated. Shape: {signals_with_indicators.shape}")

                # --- Step 2: Apply Risk Management per Ticker ---
                signals_list = []
                grouping_level = 'ticker' if isinstance(signals_with_indicators.index, pd.MultiIndex) else None

                # Need to handle both MultiIndex (multiple tickers) and DatetimeIndex (single ticker) inputs here
                if grouping_level: # Multiple tickers
                    for t, group_with_multi_index in signals_with_indicators.groupby(level=grouping_level, group_keys=False):
                        # Reset index to get DatetimeIndex for RM and validation
                        group = group_with_multi_index.reset_index(level=grouping_level, drop=True)
                        if not isinstance(group.index, pd.DatetimeIndex):
                             logger.error(f"Ticker {t}: Group index is not DatetimeIndex after reset. Skipping.")
                             continue

                        # Validate required length for this specific ticker's group
                        if len(group) < lookback_required:
                             logger.warning(f"Insufficient data for {t} after buffer fetch: {len(group)} < {lookback_required}")
                             continue
                        if not self._validate_ohlc(group): # Simple OHLC check
                             logger.warning(f"Validation failed for {t} OHLC data. Skipping.")
                             continue

                        # Apply RiskManager
                        logger.debug(f"Applying RM to ticker: {t}, shape: {group.shape}")
                        try:
                            risk_managed = self.risk_manager.apply(group.copy(), initial_position) # Pass copy
                        except Exception as rm_err:
                             logger.error(f"RiskManager failed for ticker {t}: {rm_err}", exc_info=True)
                             continue # Skip this ticker

                        # Add performance columns & rename
                        risk_managed['daily_return'] = risk_managed['close'].pct_change().fillna(0)
                        risk_managed['strategy_return'] = risk_managed['daily_return'] * risk_managed['position'].shift(1).fillna(0)
                        risk_managed.rename(columns={'return': 'rm_strategy_return', 'cumulative_return': 'rm_cumulative_return', 'exit_type': 'rm_action'}, inplace=True)
                        risk_managed['ticker'] = t
                        signals_list.append(risk_managed)

                else: # Single ticker
                     group = signals_with_indicators # Already has DatetimeIndex
                     if not isinstance(group.index, pd.DatetimeIndex):
                          logger.error(f"Single ticker {ticker} has unexpected index type: {type(group.index)}")
                          return pd.DataFrame()
                     if len(group) < lookback_required:
                           logger.warning(f"Insufficient data for single ticker {ticker} after buffer fetch: {len(group)} < {lookback_required}")
                           return pd.DataFrame()
                     if not self._validate_ohlc(group):
                           logger.warning(f"Validation failed for single ticker {ticker} OHLC data.")
                           return pd.DataFrame()

                     logger.debug(f"Applying RM to single ticker: {ticker}, shape: {group.shape}")
                     try:
                          risk_managed = self.risk_manager.apply(group.copy(), initial_position)
                     except Exception as rm_err:
                          logger.error(f"RiskManager failed for single ticker {ticker}: {rm_err}", exc_info=True)
                          return pd.DataFrame() # Fail single ticker if RM fails

                     risk_managed['daily_return'] = risk_managed['close'].pct_change().fillna(0)
                     risk_managed['strategy_return'] = risk_managed['daily_return'] * risk_managed['position'].shift(1).fillna(0)
                     risk_managed.rename(columns={'return': 'rm_strategy_return', 'cumulative_return': 'rm_cumulative_return', 'exit_type': 'rm_action'}, inplace=True)
                     # No 'ticker' column needed for single ticker return, RM output preserves index
                     final_signals_unfiltered = risk_managed


                # Combine if multiple tickers were processed
                if grouping_level and signals_list:
                     final_signals_unfiltered = pd.concat(signals_list)
                     # Set MultiIndex before filtering
                     final_signals_unfiltered = final_signals_unfiltered.set_index(['ticker', final_signals_unfiltered.index.rename('date')])
                elif not grouping_level: # Single ticker already assigned above
                     pass
                else: # No successful signals
                    logger.warning("No signals generated for any ticker after RM application.")
                    return pd.DataFrame()


                # --- Step 3: Filter Final Results to Original Date Range ---
                original_start_dt = pd.to_datetime(start_date)
                original_end_dt = pd.to_datetime(end_date)

                if final_signals_unfiltered.empty:
                    logger.warning("Final signals DataFrame is empty before date filtering.")
                    return pd.DataFrame()

                logger.debug(f"Filtering final results from {original_start_dt} to {original_end_dt}. Input shape: {final_signals_unfiltered.shape}")
                if isinstance(final_signals_unfiltered.index, pd.MultiIndex):
                     date_level_values = final_signals_unfiltered.index.get_level_values('date')
                     mask = (date_level_values >= original_start_dt) & (date_level_values <= original_end_dt)
                     final_signals = final_signals_unfiltered.loc[mask] # Use .loc for potential performance benefit
                elif isinstance(final_signals_unfiltered.index, pd.DatetimeIndex): # Single ticker case
                     final_signals = final_signals_unfiltered.loc[original_start_dt:original_end_dt]
                else:
                     logger.warning("Could not filter final signals by original date range due to unexpected index.")
                     final_signals = final_signals_unfiltered

                if final_signals.empty:
                     logger.warning(f"Final signals DataFrame is empty AFTER filtering for dates {start_date} to {end_date}.")

                logger.debug(f"Returning final signals. Shape: {final_signals.shape}")
                return final_signals

        except DataRetrievalError as dre:
             logger.error(f"Data retrieval error for {ticker}: {dre}")
             return pd.DataFrame()
        except ValueError as ve:
             logger.error(f"Configuration or calculation error for SMA strategy: {ve}", exc_info=True)
             return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error generating signals for {ticker}: {e}", exc_info=True)
            return pd.DataFrame()


    def _calculate_smas_and_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate SMAs and generate raw crossover signals vectorially for single
        or multi-ticker DataFrames.

        Args:
            df (pd.DataFrame): Input DataFrame, indexed by DatetimeIndex (single ticker)
                               or MultiIndex ['ticker', 'date'] (multi-ticker).
                               Must contain 'open', 'high', 'low', 'close'.

        Returns:
            pd.DataFrame: Original DataFrame with added columns:
                          'short_sma', 'long_sma', 'signal', 'strength'.
                          Returns an empty DataFrame on failure.
        """
        if df.empty:
            return pd.DataFrame()

        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
             missing = [col for col in required_cols if col not in df.columns]
             logger.error(f"Input missing required columns for vectorized calculation: {missing}")
             return pd.DataFrame()

        df_calc = df.copy() # Work on a copy

        # Determine grouping for rolling calculations
        grouping_level = 'ticker' if isinstance(df_calc.index, pd.MultiIndex) else None

        try:
            # Calculate SMAs - Use groupby if multi-index, otherwise apply directly
            if grouping_level:
                close_prices = df_calc.groupby(level=grouping_level)['close']
                df_calc['short_sma'] = close_prices.rolling(window=self.short_window, min_periods=self.short_window).mean().reset_index(level=0, drop=True)
                df_calc['long_sma'] = close_prices.rolling(window=self.long_window, min_periods=self.long_window).mean().reset_index(level=0, drop=True)
            else: # Single ticker
                df_calc['short_sma'] = df_calc['close'].rolling(window=self.short_window, min_periods=self.short_window).mean()
                df_calc['long_sma'] = df_calc['close'].rolling(window=self.long_window, min_periods=self.long_window).mean()

            # Calculate previous SMAs (shift respects grouping in MultiIndex)
            df_calc['prev_short'] = df_calc.groupby(level=grouping_level)['short_sma'].shift(1) if grouping_level else df_calc['short_sma'].shift(1)
            df_calc['prev_long'] = df_calc.groupby(level=grouping_level)['long_sma'].shift(1) if grouping_level else df_calc['long_sma'].shift(1)

        except ValueError as e:
             logger.error(f"Error during vectorized rolling/shift calculation: {e}")
             return pd.DataFrame()
        except Exception as e: # Catch other potential errors like KeyError if 'close' is missing
             logger.error(f"Unexpected error during vectorized indicator calculation: {e}", exc_info=True)
             return pd.DataFrame()


        # Identify crossover conditions
        crossed_above = (df_calc['short_sma'] > df_calc['long_sma']) & (df_calc['prev_short'] <= df_calc['prev_long'])
        crossed_below = (df_calc['short_sma'] < df_calc['long_sma']) & (df_calc['prev_short'] >= df_calc['prev_long'])

        # Generate signals
        df_calc['signal'] = 0
        df_calc.loc[crossed_above, 'signal'] = 1
        df_calc.loc[crossed_below, 'signal'] = -1

        # Calculate strength
        df_calc['strength'] = np.where(
             np.abs(df_calc['close']) > 1e-9,
            (df_calc['short_sma'] - df_calc['long_sma']) / df_calc['close'],
             0.0
        )
        df_calc['strength'].fillna(0.0, inplace=True)

        # Apply long-only constraint
        if self.long_only:
            df_calc['signal'] = df_calc['signal'].clip(lower=0)

        # Drop temporary columns before returning
        df_calc = df_calc.drop(columns=['prev_short', 'prev_long'], errors='ignore')

        return df_calc

    def _validate_ohlc(self, df: pd.DataFrame) -> bool:
        """ Basic check for required OHLC columns. """
        required = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required):
             missing = [col for col in required if col not in df.columns]
             logger.debug(f"Validation failed: Missing OHLC columns: {missing}")
             return False
        return True

    # Removed the _validate_data method as validation is now more integrated
    # into the generate_signals flow (checking length and OHLC within the loop/single ticker path)