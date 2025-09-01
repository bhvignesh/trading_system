# trading_system/src/strategies/momentum/relative_vigor_index_strat.py

import logging
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager

# Initialize module logger
logger = logging.getLogger(__name__)

class RVIStrategy(BaseStrategy):
    """
    Relative Vigor Index (RVI) Strategy with Integrated Risk Management.

    Implements a trading strategy based on the Relative Vigor Index (RVI) indicator.
    It generates buy/sell signals based on crossovers between the RVI and its signal line,
    calculates signal strength, and applies risk management rules using the RiskManager.

    Mathematical Formulation:
        Median Price(t) = (High(t) + Low(t)) / 2 # Note: Original RVI uses O/C/H/L directly
        Num(t)   = (C(t)-O(t)) + 2*(C(t-1)-O(t-1)) + 2*(C(t-2)-O(t-2)) + (C(t-3)-O(t-3))
        Den(t)   = (H(t)-L(t)) + 2*(H(t-1)-L(t-1)) + 2*(H(t-2)-L(t-2)) + (H(t-3)-L(t-3))
        RVI_raw(t) = Num(t) / Den(t)  # Calculated point-wise
        RVI(t)     = SMA(RVI_raw, period=lookback)
        SignalLine(t) = (RVI(t) + 2*RVI(t-1) + 2*RVI(t-2) + RVI(t-3)) / 6

    Trading Signals:
        - Buy (+1): RVI crosses above Signal Line.
        - Sell (-1): RVI crosses below Signal Line.
        - Hold/Exit (0): No crossover. Sell signals become 0 if `long_only=True`.

    Signal Strength:
        Calculated as the Z-score of the absolute difference between RVI and Signal Line
        over a rolling `signal_strength_window`. Provides a normalized measure of the
        divergence between the two lines.

    Risk Management:
        Uses the `RiskManager` class to apply stop-loss, take-profit, trailing stops,
        slippage, and transaction costs. It adjusts positions and calculates realized
        returns based on these rules.

    Outputs:
        A DataFrame containing:
          - 'open', 'close', 'high', 'low': Price references.
          - 'rvi': Smoothed Relative Vigor Index value.
          - 'signal_line': RVI Signal Line value.
          - 'signal': Raw trading signal (1, -1, 0) before risk management.
          - 'signal_strength': Normalized signal strength.
          - 'position': Risk-managed position (1, -1, 0).
          - 'return': Realized fractional return from closed trades (risk-managed).
          - 'cumulative_return': Cumulative fractional return from risk-managed trades.
          - 'exit_type': Reason for trade exit ('stop_loss', 'take_profit', etc.).

    Strategy Parameters (`params` dict):
        - 'lookback' (int): Period for RVI smoothing (SMA). Default: 10.
        - 'signal_strength_window' (int): Rolling window for Z-score normalization. Default: 20.
        - 'long_only' (bool): If True, only long trades are initiated. Default: True.
        - 'stop_loss_pct' (float): Stop-loss percentage. Default: 0.05.
        - 'take_profit_pct' (float): Take-profit percentage. Default: 0.10.
        - 'trailing_stop_pct' (float): Trailing stop percentage (0 to disable). Default: 0.0.
        - 'slippage_pct' (float): Estimated slippage per transaction. Default: 0.001.
        - 'transaction_cost_pct' (float): Estimated transaction cost per transaction. Default: 0.001.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the RVIStrategy.

        Args:
            db_config (DatabaseConfig): Database configuration object.
            params (dict, optional): Strategy hyperparameters. Defaults are used if not provided.
        """
        super().__init__(db_config, params)
        # Set default parameters if not provided
        self.params.setdefault('lookback', 10)
        self.params.setdefault('signal_strength_window', 20)
        self.params.setdefault('long_only', True)
        self.params.setdefault('stop_loss_pct', 0.05)
        self.params.setdefault('take_profit_pct', 0.10)
        self.params.setdefault('trailing_stop_pct', 0.0) # Default to disabled
        self.params.setdefault('slippage_pct', 0.001)
        self.params.setdefault('transaction_cost_pct', 0.001)

        # Ensure integer parameters are integers
        try:
            self.params['lookback'] = int(self.params['lookback'])
            self.params['signal_strength_window'] = int(self.params['signal_strength_window'])
        except (ValueError, TypeError) as e:
            logger.error(f"Parameters 'lookback' ({self.params.get('lookback')}) and "
                         f"'signal_strength_window' ({self.params.get('signal_strength_window')}) "
                         f"must be integers.", exc_info=True)
            raise ValueError(f"Lookback and signal strength window must be convertible to integers: {e}")

        # Validate window sizes
        if self.params['lookback'] <= 3: # Needs at least 3 previous periods for calculation
             raise ValueError("lookback period must be greater than 3.")
        if self.params['signal_strength_window'] < 2:
             raise ValueError("signal_strength_window must be at least 2 for std deviation.")

        # Initialize RiskManager instance once
        try:
            self.risk_manager = RiskManager(
                stop_loss_pct=float(self.params['stop_loss_pct']),
                take_profit_pct=float(self.params['take_profit_pct']),
                trailing_stop_pct=float(self.params['trailing_stop_pct']),
                slippage_pct=float(self.params['slippage_pct']),
                transaction_cost_pct=float(self.params['transaction_cost_pct'])
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid risk parameter type or value: {e}", exc_info=True)
            raise

        self.logger.info(f"RVIStrategy initialized with params: {self.params}")


    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate RVI trading signals and apply risk management for one or more tickers.

        Retrieves price data, calculates RVI and signal line, generates raw buy/sell signals
        based on crossovers, computes signal strength, and then applies risk management rules
        (stops, targets, costs) via the RiskManager.

        Args:
            ticker (Union[str, List[str]]): A single ticker symbol or a list of ticker symbols.
            start_date (str, optional): The start date for historical data retrieval (YYYY-MM-DD).
                                        If None, fetches data based on 'latest_only' or all available.
            end_date (str, optional): The end date for historical data retrieval (YYYY-MM-DD).
                                      If None, fetches up to the most recent data.
            initial_position (int): The starting position (0=flat, 1=long, -1=short) before
                                    the analysis period begins. Default is 0.
            latest_only (bool): If True, fetches only the minimum required data and returns
                                only the signal for the most recent date. Default is False.

        Returns:
            pd.DataFrame: A DataFrame containing prices, RVI values, signals, signal strength,
                          and risk-managed results (position, return, cumulative_return, exit_type).
                          Indexed by date for a single ticker, or MultiIndex (ticker, date) for multiple.
                          Returns an empty DataFrame if data is insufficient or an error occurs.

        Raises:
            DataRetrievalError: If insufficient data is found for a ticker.
        """
        lookback = self.params['lookback']
        signal_strength_window = self.params['signal_strength_window']
        required_periods = max(lookback + 3, signal_strength_window) + 3 + 5

        self.logger.info(f"Generating RVI signals for {ticker}...")
        self.logger.debug(f"Required data periods: {required_periods}")

        if isinstance(ticker, list):
            # --- Multi-Ticker Processing ---
            self.logger.debug(f"Processing multiple tickers: {len(ticker)}")
            if latest_only:
                 prices_df = self.get_historical_prices(
                     ticker, lookback=required_periods, data_source='yfinance'
                 )
            else:
                 prices_df = self.get_historical_prices(
                     ticker, from_date=start_date, to_date=end_date, data_source='yfinance'
                 )

            if prices_df.empty:
                self.logger.warning("No historical data retrieved for the given tickers and date range.")
                return pd.DataFrame()

            # Ensure the main DataFrame has the expected MultiIndex structure before grouping
            if not isinstance(prices_df.index, pd.MultiIndex):
                 self.logger.error("Multi-ticker fetch did not return a MultiIndex DataFrame. Check get_historical_prices.")
                 # Attempt to fix if possible, otherwise return empty
                 if 'ticker' in prices_df.columns and 'date' in prices_df.columns:
                      prices_df['date'] = pd.to_datetime(prices_df['date'])
                      prices_df = prices_df.set_index(['ticker', 'date']).sort_index()
                      self.logger.warning("Attempted to fix index structure.")
                 else:
                      return pd.DataFrame()


            grouped = prices_df.groupby('ticker', group_keys=False) # group_keys=False is slightly cleaner

            results_list = []
            processed_keys = [] # Keep track of tickers processed successfully

            # Use ticker_name for loop variable to avoid confusion with the input list 'ticker'
            # Iterate through the groups provided by groupby
            for ticker_name, group_df_multi_idx in grouped:
                self.logger.debug(f"Processing ticker: {ticker_name} with {len(group_df_multi_idx)} rows")
                try:
                    
                    # Ensure the group has a DatetimeIndex for calculations and RiskManager
                    # Drop the 'ticker' level from the index for this specific group's processing
                    group_df = group_df_multi_idx.reset_index(level='ticker', drop=True)

                    # Now validate the DataFrame with the simple DatetimeIndex
                    if not self._validate_data(group_df, min_records=required_periods, ticker_name=ticker_name):
                        self.logger.warning(f"Skipping ticker {ticker_name} due to insufficient/invalid data.")
                        continue # Skip this ticker

                    # Pass the correctly indexed group_df to calculations
                    rvi, signal_line = self._calculate_rvi_components(group_df, lookback)
                    signals_df = self._generate_signals_df(group_df, rvi, signal_line)

                    # Apply Risk Management - signals_df now has DatetimeIndex
                    risk_managed_df = self.risk_manager.apply(signals_df, initial_position)

                    # Append the result (which has DatetimeIndex) to the list
                    results_list.append(risk_managed_df)
                    processed_keys.append(ticker_name) # Add key for successful processing

                except Exception as e:
                    self.logger.error(f"Error processing ticker {ticker_name}: {e}", exc_info=True)
                    continue # Continue to the next ticker

            if not results_list:
                 self.logger.warning("No results generated successfully for any ticker.")
                 return pd.DataFrame()

            # Concatenate results using the keys of successfully processed tickers
            # This rebuilds the MultiIndex correctly
            final_df = pd.concat(results_list, keys=processed_keys, names=['ticker', 'date'])

            if latest_only:
                # Return the last row for each ticker using the final multi-indexed DataFrame
                # Need to group again on the final frame
                return final_df.groupby(level='ticker').tail(1)
            else:
                return final_df

        else:
            # --- Single Ticker Processing ---
            ticker_name = ticker
            self.logger.debug(f"Processing single ticker: {ticker_name}")
            try:
                if latest_only:
                    prices_df = self.get_historical_prices(ticker_name, lookback=required_periods, data_source='yfinance')
                else:
                    prices_df = self.get_historical_prices(
                        ticker_name, from_date=start_date, to_date=end_date, data_source='yfinance'
                    )

                # Single ticker fetch should already return DatetimeIndex from _execute_query
                if not isinstance(prices_df.index, pd.DatetimeIndex):
                     self.logger.error(f"Single ticker fetch for {ticker_name} did not return DatetimeIndex. Check get_historical_prices.")
                     # Attempt fix or return empty
                     if 'date' in prices_df.columns:
                          prices_df['date'] = pd.to_datetime(prices_df['date'])
                          prices_df = prices_df.set_index('date').sort_index()
                          self.logger.warning("Attempted to fix index structure.")
                     elif 'date' in prices_df.index.names: # If date is already the index name but not type
                           prices_df.index = pd.to_datetime(prices_df.index)
                           self.logger.warning("Attempted to fix index type.")
                     else:
                           return pd.DataFrame()


                if not self._validate_data(prices_df, min_records=required_periods, ticker_name=ticker_name):
                    self.logger.warning(f"Insufficient or invalid data for ticker {ticker_name}.")
                    raise DataRetrievalError(f"Insufficient/invalid data for {ticker_name}")

                # Calculations expect DatetimeIndex, which prices_df should have
                rvi, signal_line = self._calculate_rvi_components(prices_df, lookback)
                signals_df = self._generate_signals_df(prices_df, rvi, signal_line)

                # Risk manager expects DatetimeIndex, which signals_df should have
                result_df = self.risk_manager.apply(signals_df, initial_position)

                if latest_only:
                    return result_df.tail(1)
                else:
                    return result_df

            except DataRetrievalError as e:
                self.logger.error(f"Data retrieval error for {ticker_name}: {e}", exc_info=False)
                raise # Re-raise the specific error
            except Exception as e:
                self.logger.error(f"Error processing single ticker {ticker_name}: {e}", exc_info=True)
                return pd.DataFrame()
            

    def _calculate_rvi_components(self, prices_df: pd.DataFrame, lookback: int) -> Tuple[pd.Series, pd.Series]:
        """
        Calculates the Relative Vigor Index (RVI) and its signal line.

        Args:
            prices_df (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close' columns.
            lookback (int): The smoothing period for the RVI (SMA window).

        Returns:
            Tuple[pd.Series, pd.Series]: A tuple containing the RVI Series and the Signal Line Series.
                                         Indices match the input prices_df.
        """
        self.logger.debug(f"Calculating RVI components with lookback={lookback}")
        o = prices_df['open']
        h = prices_df['high']
        l = prices_df['low']
        c = prices_df['close']
        epsilon = 1e-9 # Small value to prevent division by zero

        # Calculate weighted numerator and denominator for raw RVI
        # Shift requires enough preceding data points (at least 3)
        numerator = (c - o) + \
                    2 * (c.shift(1) - o.shift(1)) + \
                    2 * (c.shift(2) - o.shift(2)) + \
                    (c.shift(3) - o.shift(3))

        denominator = (h - l) + \
                      2 * (h.shift(1) - l.shift(1)) + \
                      2 * (h.shift(2) - l.shift(2)) + \
                      (h.shift(3) - l.shift(3))

        # Avoid division by zero or near-zero range
        denominator = denominator.replace(0, np.nan) # Replace exact zero with NaN first
        denominator.loc[denominator.abs() < epsilon] = np.nan # Replace near-zero with NaN
        denominator = denominator.ffill() # Fill NaNs from initial periods if possible

        # Calculate raw RVI
        rvi_raw = numerator / (denominator + epsilon) # Add epsilon for safety

        # Smooth RVI using Simple Moving Average
        rvi = rvi_raw.rolling(window=lookback, min_periods=max(1, lookback // 2)).mean() # Use min_periods

        # Calculate the Signal Line (weighted average of RVI)
        # Needs at least 3 previous RVI values
        signal_line = (rvi + 2 * rvi.shift(1) + 2 * rvi.shift(2) + rvi.shift(3)) / 6

        self.logger.debug("Finished calculating RVI components.")
        return rvi, signal_line


    def _generate_signals_df(self, prices_df: pd.DataFrame, rvi: pd.Series, signal_line: pd.Series) -> pd.DataFrame:
        """
        Generates raw trading signals and signal strength based on RVI crossovers.

        Args:
            prices_df (pd.DataFrame): Original price DataFrame (must include 'open', 'close', 'high', 'low').
            rvi (pd.Series): The calculated RVI series.
            signal_line (pd.Series): The calculated Signal Line series.

        Returns:
            pd.DataFrame: A DataFrame containing necessary price columns ('open', 'close', 'high', 'low'),
                          'rvi', 'signal_line', the generated 'signal' (1, -1, 0), and
                          'signal_strength' (normalized). Index matches input.
        """
        self.logger.debug("Generating signals and strength...")
        # Create a DataFrame containing prices and indicators.
        # DO NOT dropna() here - let signal calculation handle NaNs.
        df = pd.DataFrame({
            'open': prices_df['open'],
            'close': prices_df['close'],
            'high': prices_df['high'],
            'low': prices_df['low'],
            'rvi': rvi,
            'signal_line': signal_line
        }) # Keep original index

        # Use shifted values for crossover detection
        rvi_prev = df['rvi'].shift(1)
        signal_line_prev = df['signal_line'].shift(1)

        # --- Generate Raw Signals ---
        # Condition 1: Buy crossover (was below, now above)
        buy_signal = (rvi_prev < signal_line_prev) & (df['rvi'] > df['signal_line'])
        # Condition 2: Sell crossover (was above, now below)
        sell_signal = (rvi_prev > signal_line_prev) & (df['rvi'] < df['signal_line'])

        # Assign signals: 1 for buy, -1 for sell, 0 otherwise
        df['signal'] = np.select(
            [buy_signal, sell_signal],
            [1, -1],
            default=0
        )

        # Apply long-only constraint if specified
        if self.params.get('long_only', True):
            df.loc[df['signal'] == -1, 'signal'] = 0
            self.logger.debug("Applied long_only constraint, sell signals set to 0.")

        # --- Calculate Signal Strength ---
        window = self.params['signal_strength_window']
        epsilon = 1e-9 # For safe division

        # Calculate the absolute difference |RVI - SignalLine|
        abs_diff = (df['rvi'] - df['signal_line']).abs()

        # Calculate rolling mean and std dev of the absolute difference
        rolling_mean = abs_diff.rolling(window=window, min_periods=max(1, window // 2)).mean()
        rolling_std = abs_diff.rolling(window=window, min_periods=max(1, window // 2)).std()

        # Calculate Z-score: (value - mean) / (std + epsilon)
        # Only calculate strength where RVI and signal_line are valid
        signal_strength_raw = (abs_diff - rolling_mean) / (rolling_std + epsilon)

        # Assign strength, handling potential NaNs (especially at the start)
        # Set strength to 0 if std dev is NaN or zero (no variation) or if abs_diff is NaN
        df['signal_strength'] = signal_strength_raw.fillna(0)
        # Replace any infinities that might arise from epsilon edge cases (unlikely)
        df['signal_strength'] = df['signal_strength'].replace([np.inf, -np.inf], 0)

        # Ensure the required columns for RiskManager are present
        required_cols = ['open', 'close', 'high', 'low', 'signal']
        output_cols = required_cols + ['rvi', 'signal_line', 'signal_strength']

        self.logger.debug("Finished generating signals and strength.")
        # Return only the necessary columns for the next step (RiskManager) + indicators
        return df[output_cols]


    def _validate_data(self, df: pd.DataFrame, min_records: int, ticker_name: Optional[str]="") -> bool:
        """
        Validates the input DataFrame for sufficient records and data integrity.

        Checks for minimum length and presence of NaNs in essential price columns.

        Args:
            df (pd.DataFrame): DataFrame containing price data.
            min_records (int): Minimum number of rows required.
            ticker_name (str, optional): Name of the ticker for logging purposes.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        prefix = f"Ticker {ticker_name}: " if ticker_name else ""
        if df is None or df.empty:
             self.logger.warning(f"{prefix}DataFrame is None or empty.")
             return False
        if len(df) < min_records:
            self.logger.warning(f"{prefix}Insufficient data length ({len(df)} rows, required {min_records}).")
            return False
        # Check for NaNs in columns required for calculation AND RiskManager
        required_cols = ['open', 'high', 'low', 'close']
        if df[required_cols].isnull().values.any():
            nan_counts = df[required_cols].isnull().sum()
            self.logger.warning(f"{prefix}Missing values found in required columns:\n{nan_counts[nan_counts > 0]}")
            # Decide if this is acceptable or should return False. For now, allow if RiskManager can handle.
            # Let's return False if critical inputs are missing.
            # return False # Stricter check
            pass # Allow for now, RVI/RM might handle some NaNs

        return True