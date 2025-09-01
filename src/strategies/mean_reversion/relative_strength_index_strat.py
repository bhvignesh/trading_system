# trading_system/src/strategies/rsi_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class RSIStrategy(BaseStrategy):
    """
    Relative Strength Index (RSI) Trading Strategy with Integrated Risk Management.

    Strategy Logic:
    ---------------
    The Relative Strength Index (RSI) is a momentum oscillator that measures the speed and change
    of price movements. It is calculated as:
      RSI = 100 - (100 / (1 + RS))
    where RS (Relative Strength) is the ratio of an N-period Exponential Moving Average (EMA)
    of gains to an N-period EMA of losses. Wilder's smoothing method (alpha = 1/period) is used.

    - A buy signal (signal = 1) is generated when the RSI crosses above an 'oversold' threshold
      (e.g., 30) from below.
    - A sell signal (signal = -1) is generated when the RSI crosses below an 'overbought'
      threshold (e.g., 70) from above.
    - If 'long_only' mode is enabled, sell signals are converted to neutral signals (signal = 0),
      effectively only allowing long entries and exits. Correspondingly, signal strength for
      such neutralized sell signals is also set to 0.

    Signal strength is calculated based on how far the RSI moves beyond the threshold at the
    time of the signal, normalized by the range between overbought and oversold thresholds.

    Risk Management:
    ----------------
    Generated raw signals are processed by the `RiskManager` class, which applies:
    - Stop-loss orders.
    - Take-profit orders.
    - Optional trailing stop-loss orders.
    - Adjustments for estimated slippage and transaction costs on entries and exits.
    The `RiskManager` determines the final position, calculates trade returns,
    cumulative portfolio returns, and identifies the type of exit.

    Data Handling:
    --------------
    - For backtesting, historical price data is fetched from `start_date` (with an
      additional lookback period for initial indicator calculation) to `end_date`.
    - For live forecasting (`latest_only=True`), a minimal amount of recent data is fetched.
    - The strategy supports processing for both single and multiple tickers efficiently
      using vectorized operations and pandas `groupby` where necessary.

    Output:
    -------
    The `generate_signals` method returns a pandas DataFrame with the following columns,
    indexed by date (or by (ticker, date) for multiple tickers):
    - 'open', 'high', 'low', 'close': Original price data.
    - 'rsi': Calculated RSI values.
    - 'signal': Raw trading signal (1 for buy, -1 for sell, 0 for hold/neutral), which is
                then passed to the RiskManager.
    - 'signal_strength': Normalized strength of the raw signal.
    - Columns from RiskManager: 'position' (actual position after RM), 'return' (realized trade return),
      'cumulative_return', 'exit_type'.
    """
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initializes the RSIStrategy.

        Args:
            db_config (DatabaseConfig): Configuration for database access.
            params (dict, optional): Strategy-specific parameters. Expected keys include:
                - 'rsi_period' (int): Lookback period for RSI (default: 14).
                - 'overbought' (float): RSI level above which is considered overbought (default: 70.0).
                - 'oversold' (float): RSI level below which is considered oversold (default: 30.0).
                - 'long_only' (bool): If True, only long positions are taken (default: True).
                - Risk management parameters (e.g., 'stop_loss_pct', 'take_profit_pct', etc.)
                  as expected by the `RiskManager` class.
        """
        super().__init__(db_config, params or {})
        self.logger = logging.getLogger(self.__class__.__name__)
        # Ensure default for long_only is set if not provided in params
        self.params.setdefault('long_only', True)

    def _validate_parameters(self) -> dict:
        """
        Validates strategy-specific parameters for RSI and applies defaults.

        Returns:
            dict: A dictionary containing validated RSI parameters:
                  'rsi_period', 'overbought', 'oversold'.

        Raises:
            ValueError: If parameters are invalid (e.g., rsi_period < 1, or
                        invalid threshold ordering 0 < oversold < overbought < 100).
        """
        # Ensure types are correct and apply defaults
        params = {
            'rsi_period': int(self.params.get('rsi_period', 14)),
            'overbought': float(self.params.get('overbought', 70.0)),
            'oversold': float(self.params.get('oversold', 30.0))
        }

        if params['rsi_period'] < 1:
            msg = f"Invalid rsi_period: {params['rsi_period']}. Must be a positive integer (>= 1)."
            self.logger.error(msg)
            raise ValueError(msg)
        
        if not (0 < params['oversold'] < params['overbought'] < 100):
            msg = (
                f"Invalid RSI thresholds: oversold={params['oversold']}, overbought={params['overbought']}. "
                "Must satisfy 0 < oversold < overbought < 100."
            )
            self.logger.error(msg)
            raise ValueError(msg)
        
        return params

    def _get_price_data(self, 
                        ticker: Union[str, List[str]], 
                        rsi_period: int, 
                        start_date_str: Optional[str], 
                        end_date_str: Optional[str],
                        latest_only: bool) -> pd.DataFrame:
        """
        Retrieves and prepares historical price data for RSI calculation.

        For backtesting (`start_date_str` is provided), it fetches data from an 
        `adjusted_start_date` (which is `start_date_str` minus a warmup buffer period 
        for the indicator) up to `end_date_str`. The returned DataFrame is then 
        filtered to begin from the original `start_date_str`.
        For `latest_only` mode (live forecasting), it fetches a minimal lookback period 
        sufficient for the most recent RSI calculation.

        Args:
            ticker (Union[str, List[str]]): Ticker symbol or a list of ticker symbols.
            rsi_period (int): The period used for RSI calculation, influencing the warmup buffer.
            start_date_str (Optional[str]): The strategy's conceptual start date ('YYYY-MM-DD').
                                         Data prior to this date may be fetched for indicator warmup.
            end_date_str (Optional[str]): The strategy's conceptual end date ('YYYY-MM-DD').
            latest_only (bool): If True, fetches only recent data needed for the latest signal.

        Returns:
            pd.DataFrame: A DataFrame containing historical price data ('open', 'high',
                          'low', 'close', 'volume'), indexed by date (for a single ticker)
                          or by a ('ticker', 'date') MultiIndex (for multiple tickers).
                          The data is sorted by index. Returns an empty DataFrame if no data.
        """
        if latest_only:
            # Fetch minimal data for the last RSI value + signal (requires 1 previous RSI for crossover)
            # RSI period + 1 for current RSI, +1 for previous RSI value for crossover check.
            lookback_needed = rsi_period + 2 
            df = self.get_historical_prices(ticker, lookback=lookback_needed)
            return df.sort_index() # Ensure data is chronologically sorted

        if start_date_str:
            original_start_dt = pd.to_datetime(start_date_str)
            # Calculate an adjusted start date to fetch enough data for RSI warmup.
            # A common heuristic is 2-3 times the indicator period.
            buffer_days = rsi_period * 3 
            adjusted_start_dt = original_start_dt - pd.DateOffset(days=buffer_days)
            adjusted_start_str_for_query = adjusted_start_dt.strftime('%Y-%m-%d')
            
            self.logger.debug(
                f"Original start date: {start_date_str}. Fetching data from adjusted start: {adjusted_start_str_for_query} for RSI warmup."
            )
            df_full_range = self.get_historical_prices(
                ticker, 
                from_date=adjusted_start_str_for_query, 
                to_date=end_date_str # end_date_str can be None
            )

            if df_full_range.empty:
                self.logger.warning(f"No data retrieved between {adjusted_start_str_for_query} and {end_date_str} for {ticker}.")
                return df_full_range

            # Filter the DataFrame to begin from the original_start_dt.
            # Data fetched before original_start_dt is used implicitly for indicator warmup.
            if isinstance(df_full_range.index, pd.MultiIndex):
                # Filter based on the date level (assuming it's level 1)
                df = df_full_range[df_full_range.index.get_level_values(1) >= original_start_dt]
            else: # Single DatetimeIndex
                df = df_full_range[df_full_range.index >= original_start_dt]
            return df.sort_index() 
        else:
            # No start_date provided, fetch a general large history (e.g., for full backtest)
            # Using rsi_period + (252 trading days * 2 years) as a substantial lookback.
            df = self.get_historical_prices(ticker, lookback=rsi_period + (252 * 2))
            return df.sort_index()


    def _calculate_rsi(self, close_prices: pd.Series, period: int) -> pd.Series:
        """
        Calculates the Relative Strength Index (RSI) using Wilder's smoothing method.

        Args:
            close_prices (pd.Series): Series of closing prices.
            period (int): Lookback period for RSI calculation. Must be a positive integer.

        Returns:
            pd.Series: Series containing the calculated RSI values. NaNs will be present
                       at the beginning until enough data (`period`) is available.
        """
        delta = close_prices.diff()
        
        # Calculate gains (positive price changes) and losses (absolute of negative price changes)
        gain = delta.where(delta > 0, 0.0).fillna(0) 
        loss = (-delta).where(-delta > 0, 0.0).fillna(0)

        # Calculate Wilder's Exponential Moving Average for gains and losses
        # alpha = 1 / period for Wilder's smoothing.
        # min_periods=period ensures EMA starts only after enough data.
        avg_gain = gain.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()

        # Calculate Relative Strength (RS)
        rs = avg_gain / avg_loss
        
        # Calculate RSI
        # RSI = 100 - (100 / (1 + RS))
        # If avg_loss is 0:
        #   - If avg_gain > 0, RS = inf, RSI = 100.
        #   - If avg_gain = 0, RS = NaN (0/0), RSI = NaN.
        rsi = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi


    def _generate_rsi_signals(self, 
                              rsi: pd.Series, 
                              oversold_thr: float, 
                              overbought_thr: float,
                              is_long_only: bool) -> tuple[pd.Series, pd.Series]:
        """
        Generates trading signals and their strength based on RSI values and thresholds.

        Args:
            rsi (pd.Series): Series of calculated RSI values.
            oversold_thr (float): The RSI level below which is considered oversold.
            overbought_thr (float): The RSI level above which is considered overbought.
            is_long_only (bool): If True, sell signals are suppressed (signal becomes 0, strength 0).

        Returns:
            Tuple[pd.Series, pd.Series]:
                - signals: Series of trading signals (1 for buy, -1 for sell, 0 for hold/neutral).
                - signal_strength: Series of normalized signal strengths (0.0 to 1.0).
        """
        # Ensure RSI series has at least two values for rsi.shift(1) to work
        if len(rsi) < 2:
            self.logger.debug("RSI series too short for signal generation. Returning neutral signals.")
            empty_signals = pd.Series(0, index=rsi.index, dtype=int)
            empty_strength = pd.Series(0.0, index=rsi.index, dtype=float)
            return empty_signals, empty_strength

        rsi_shifted = rsi.shift(1) # Previous RSI value

        # Define signal conditions
        buy_condition = (rsi_shifted <= oversold_thr) & (rsi > oversold_thr)
        sell_condition = (rsi_shifted >= overbought_thr) & (rsi < overbought_thr)

        # Calculate signal strength normalization factor
        range_width = overbought_thr - oversold_thr
        if abs(range_width) < 1e-6: # Avoid division by zero if thresholds are virtually identical
            self.logger.warning(
                f"RSI overbought ({overbought_thr}) and oversold ({oversold_thr}) thresholds are "
                "too close or equal. Signal strength calculation might be affected (set to 0 or 1)."
            )
            # Default to a non-zero width to prevent NaNs/infs in strength; strength will be 0 or based on RSI value
            range_width = max(1.0, abs(overbought_thr)) # A large denominator will make strength small

        # Calculate potential strengths
        buy_strength_raw = ((rsi - oversold_thr) / range_width).clip(lower=0.0, upper=1.0)
        sell_strength_raw = ((overbought_thr - rsi) / range_width).clip(lower=0.0, upper=1.0)

        # Initialize signals and strengths with neutral values
        final_signals = pd.Series(0, index=rsi.index, dtype=int)
        final_signal_strength = pd.Series(0.0, index=rsi.index, dtype=float)

        # Apply buy signals
        final_signals.loc[buy_condition] = 1
        final_signal_strength.loc[buy_condition] = buy_strength_raw[buy_condition]

        # Apply sell signals (conditionally based on long_only)
        if not is_long_only:
            final_signals.loc[sell_condition] = -1
            final_signal_strength.loc[sell_condition] = sell_strength_raw[sell_condition]
        # If is_long_only is True, sell signals are implicitly ignored (signal and strength remain 0).
            
        # Fill NaNs that might occur at the beginning due to rsi.shift(1)
        final_signals = final_signals.fillna(0)
        final_signal_strength = final_signal_strength.fillna(0.0)
        
        return final_signals, final_signal_strength

    def _apply_risk_management(self, 
                               df_with_signals: pd.DataFrame, 
                               risk_manager_params: dict,
                               initial_position: int, 
                               latest_only: bool,
                               # Pass original start/end for final filtering after RM
                               filter_start_date_str: Optional[str], 
                               filter_end_date_str: Optional[str]    
                              ) -> pd.DataFrame:
        """
        Applies risk management rules using the RiskManager class to the generated signals.
        Filters the results to the specified date range if not in `latest_only` mode.

        Args:
            df_with_signals (pd.DataFrame): DataFrame containing price data and raw signals.
                                           Must include 'open', 'high', 'low', 'close', 'signal'.
                                           Index should be DatetimeIndex or ('ticker', 'date') MultiIndex.
            risk_manager_params (dict): Parameters for initializing the RiskManager.
            initial_position (int): The initial trading position for the RiskManager.
            latest_only (bool): If True, returns only the latest record per ticker.
            filter_start_date_str (Optional[str]): The conceptual start date of the strategy, used for
                                         filtering the final results if not `latest_only`.
            filter_end_date_str (Optional[str]): The conceptual end date of the strategy, used for
                                       filtering the final results if not `latest_only`.
        Returns:
            pd.DataFrame: DataFrame augmented with risk-managed columns ('position',
                          'return', 'cumulative_return', 'exit_type').
        """
        risk_manager = RiskManager(**risk_manager_params)
        
        # Ensure DataFrame is sorted before passing to RiskManager.
        # This should generally be true from _get_price_data and subsequent operations.
        df_sorted_for_rm = df_with_signals.sort_index()

        # RiskManager.apply handles single vs. multi-ticker DataFrames appropriately.
        # It expects specific columns: 'open', 'high', 'low', 'close', 'signal'.
        processed_df = risk_manager.apply(df_sorted_for_rm, initial_position)

        # Filter results based on the original conceptual date range, unless in latest_only mode.
        # This ensures the output aligns with the user's requested backtest period.
        if not latest_only and filter_start_date_str:
            final_start_dt = pd.to_datetime(filter_start_date_str)
            final_end_dt = pd.to_datetime(filter_end_date_str) if filter_end_date_str else None

            if isinstance(processed_df.index, pd.MultiIndex):
                # Assuming date is the second level (level 1) of the MultiIndex
                date_level_values = processed_df.index.get_level_values(1)
                mask = date_level_values >= final_start_dt
                if final_end_dt:
                    mask &= (date_level_values <= final_end_dt)
                processed_df = processed_df[mask]
            else: # Single DatetimeIndex
                mask = processed_df.index >= final_start_dt
                if final_end_dt:
                    mask &= (processed_df.index <= final_end_dt)
                processed_df = processed_df[mask]
        
        # If latest_only is True, return only the last available record per ticker.
        if latest_only:
            if isinstance(processed_df.index, pd.MultiIndex):
                # Group by ticker (level 0) and take the last entry for each ticker
                processed_df = processed_df.groupby(level=0, group_keys=False).tail(1)
            else: # Single ticker DataFrame
                processed_df = processed_df.tail(1)
                
        return processed_df

    def generate_signals(self, 
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generates RSI-based trading signals with integrated risk management.

        This method orchestrates the entire process:
        1. Validates RSI-specific parameters.
        2. Retrieves historical price data, including a warmup period for indicators.
        3. Calculates RSI and generates raw buy/sell signals for each ticker.
        4. Applies risk management rules (stop-loss, take-profit, costs) via RiskManager.
        5. Filters the final output to the requested date range or latest signal.

        Args:
            ticker (Union[str, List[str]]): A single ticker symbol or a list of ticker symbols.
            start_date (Optional[str]): The start date for the backtesting period ('YYYY-MM-DD').
                                        If None, a long history is fetched.
            end_date (Optional[str]): The end date for the backtesting period ('YYYY-MM-DD').
                                      If None, data up to the most recent available is used.
            initial_position (int): The initial trading position (0 for flat, 1 for long, -1 for short).
                                    This is passed to the RiskManager.
            latest_only (bool): If True, only the latest signal data point for each ticker is returned,
                                suitable for live trading or forecasting.

        Returns:
            pd.DataFrame: A DataFrame containing price data, RSI values, raw signals, signal strength,
                          and comprehensive risk-managed trade performance metrics (position, returns, etc.).
                          The DataFrame is indexed by date (for a single ticker) or by a 
                          ('ticker', 'date') MultiIndex (for multiple tickers).
        """
        # Validate RSI-specific parameters from self.params
        rsi_specific_params = self._validate_parameters()
        rsi_period = rsi_specific_params['rsi_period']
        overbought_thr = rsi_specific_params['overbought']
        oversold_thr = rsi_specific_params['oversold']
        
        # Determine if strategy operates in long_only mode from general self.params
        is_long_only = self.params.get('long_only', True) # Default to True if not specified

        # Retrieve historical price data. _get_price_data handles warmup lookback & initial date filtering.
        price_df = self._get_price_data(ticker, rsi_period, start_date, end_date, latest_only)
        
        if price_df.empty:
            self.logger.warning(f"No historical price data retrieved for ticker(s): {ticker}. "
                                "Cannot generate signals. Returning empty DataFrame.")
            # Return an empty DataFrame; downstream consumers should check for .empty
            return pd.DataFrame()

        # Helper function to calculate indicators and signals for a DataFrame group (single ticker's data)
        def _process_ticker_group(group_df: pd.DataFrame) -> pd.DataFrame:
            # Ensure data within the group is sorted by date (important for time-series calculations)
            # This should already be true if _get_price_data and BaseStrategy.get_historical_prices sort correctly.
            group_df = group_df.sort_index() 
            
            if 'close' not in group_df.columns:
                ticker_name = group_df.name if hasattr(group_df, 'name') else 'Unknown Ticker'
                self.logger.error(
                    f"Price data for ticker '{ticker_name}' is missing the 'close' column. "
                    "Cannot calculate RSI or signals."
                )
                # Add placeholder columns to maintain DataFrame structure for .apply()
                group_df['rsi'] = np.nan
                group_df['signal'] = 0
                group_df['signal_strength'] = 0.0
                return group_df

            # Calculate RSI
            group_df['rsi'] = self._calculate_rsi(group_df['close'], rsi_period)
            
            # Generate raw signals and their strength based on RSI
            signals, strength = self._generate_rsi_signals(
                group_df['rsi'], oversold_thr, overbought_thr, is_long_only
            )
            group_df['signal'] = signals
            group_df['signal_strength'] = strength
            return group_df

        # Apply indicator and raw signal calculations
        if isinstance(price_df.index, pd.MultiIndex): # Multi-ticker DataFrame
            # Group by ticker (level 0 of MultiIndex) and apply processing
            # group_keys=False prevents adding an extra level to the index from group names
            signals_df = price_df.groupby(level=0, group_keys=False).apply(_process_ticker_group)
        else: # Single-ticker DataFrame
            signals_df = _process_ticker_group(price_df)

        # Prepare parameters for RiskManager, extracting from self.params
        # RiskManager will use its own defaults if specific params are not found here.
        risk_manager_param_keys = [
            'stop_loss_pct', 'take_profit_pct', 'trailing_stop_pct',
            'slippage_pct', 'transaction_cost_pct'
        ]
        risk_manager_params = {
            key: self.params[key] for key in risk_manager_param_keys if key in self.params
        }
        
        # Apply risk management.
        # _apply_risk_management handles final date filtering (for start_date/end_date)
        # and latest_only logic *after* RiskManager processing.
        final_df = self._apply_risk_management(
            signals_df, 
            risk_manager_params, 
            initial_position, 
            latest_only,
            start_date, # Pass original start_date for final filtering
            end_date    # Pass original end_date for final filtering
        )
        
        return final_df