# trading_system/src/strategies/tsi_strategy.py

import logging
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.database.config import \
    DatabaseConfig  # Added for type hinting __init__
# Assuming these imports are correct relative to the project structure
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class TSIStrategy(BaseStrategy):
    """
    True Strength Index (TSI) Strategy with Integrated Risk Management.

    Calculates the TSI indicator and generates trading signals based on crossovers
    between the TSI line and its signal line. Applies risk management rules
    (stop-loss, take-profit, trailing stop) using the RiskManager component.

    Indicator Logic:
        1. Calculate price change: delta = close.diff()
        2. Double Smooth delta: pc_ema1 = EMA(delta, long_period)
                                pc_ema2 = EMA(pc_ema1, short_period)
        3. Double Smooth absolute delta: abs_pc_ema1 = EMA(abs(delta), long_period)
                                         abs_pc_ema2 = EMA(abs_pc_ema1, short_period)
        4. TSI = 100 * (pc_ema2 / (abs_pc_ema2 + epsilon))
        5. Signal Line = EMA(TSI, signal_period)
        6. Raw Signal: +1 if TSI crosses above Signal Line, -1 if below.
           (Adjusted to 0 for sell signals if long_only=True).

    Risk Management:
        - Applied sequentially after raw signal generation using RiskManager.
        - Computes risk-adjusted positions, returns, and exit reasons.

    Output DataFrame Columns:
        - open, high, low, close: Original price data.
        - tsi: True Strength Index value.
        - signal_line: EMA of TSI.
        - strength: Difference between TSI and signal line.
        - signal: Raw trading signal (1, -1, 0) based on crossover.
        - position: Risk-managed position (-1, 0, 1).
        - return: Realized return from RiskManager on trade closure.
        - cumulative_return: Cumulative return from RiskManager.
        - exit_type: Reason for exit from RiskManager.
        - ticker: Included if multiple tickers are processed.

    Required Parameters in `params` dict:
        - long_period (int): Period for the first EMA smoothing (default: 25).
        - short_period (int): Period for the second EMA smoothing (default: 13).
        - signal_period (int): Period for the TSI signal line EMA (default: 12).
        - long_only (bool): If True, suppress short signals (default: True).
        - stop_loss_pct (float): Stop loss percentage (default: 0.05).
        - take_profit_pct (float): Take profit percentage (default: 0.10).
        - trailing_stop_pct (float): Trailing stop percentage (0 to disable, default: 0.0).
        - slippage_pct (float): Slippage cost per transaction (default: 0.001).
        - transaction_cost_pct (float): Transaction cost per transaction (default: 0.001).
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the TSIStrategy.

        Args:
            db_config (DatabaseConfig): Database configuration object.
            params (dict, optional): Dictionary of strategy and risk parameters.
        """
        default_params = {
            'long_period': 25,
            'short_period': 13,
            'signal_period': 12,
            'long_only': True,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001
        }
        # Ensure params is a dict, handle potential None input
        params = params if params is not None else {}
        # Merge provided params with defaults, prioritizing provided ones
        merged_params = {**default_params, **params}

        super().__init__(db_config, merged_params) # Pass merged params to BaseStrategy

        # --- Strategy Parameters ---
        self.long_period = int(self.params['long_period'])
        self.short_period = int(self.params['short_period'])
        self.signal_period = int(self.params['signal_period'])
        self.long_only = bool(self.params['long_only'])

        # --- Risk Parameters (passed to RiskManager) ---
        risk_params = {
            'stop_loss_pct': float(self.params['stop_loss_pct']),
            'take_profit_pct': float(self.params['take_profit_pct']),
            'trailing_stop_pct': float(self.params['trailing_stop_pct']),
            'slippage_pct': float(self.params['slippage_pct']),
            'transaction_cost_pct': float(self.params['transaction_cost_pct'])
        }

        self._validate_parameters() # Validate periods and risk params

        # --- Components ---
        self.risk_manager = RiskManager(**risk_params)
        self.logger = logging.getLogger(self.__class__.__name__)

        # Calculate required lookback buffer (sum of periods for full calculation + safety margin)
        # Similar approach to AwesomeOscillator lookback calculation
        self.lookback_buffer = 2 * (self.long_period + self.short_period + self.signal_period)
        # Minimum records required *after* lookback for calculation stability
        self.min_calculation_records = self.long_period + self.short_period + self.signal_period

    def _validate_parameters(self):
        """Validate strategy and risk parameters."""
        if not (self.long_period > 0 and self.short_period > 0 and self.signal_period > 0):
            raise ValueError("All TSI period parameters (long, short, signal) must be positive integers.")
        # RiskManager already validates its parameters, but adding basic checks here too
        if any(self.params[p] < 0 for p in ['stop_loss_pct', 'take_profit_pct', 'trailing_stop_pct', 'slippage_pct', 'transaction_cost_pct']):
             raise ValueError("Risk management percentages (stops, costs) cannot be negative.")


    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: Union[int, Dict[str, int]] = 0, # Allow dict for multi-ticker
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generates trading signals and applies risk management.

        Fetches data, calculates TSI indicators and raw signals, applies the
        RiskManager, and returns a DataFrame compatible with the optimization framework.

        Args:
            ticker (Union[str, List[str]]): Ticker symbol or list of symbols.
            start_date (str, optional): Start date ('YYYY-MM-DD').
            end_date (str, optional): End date ('YYYY-MM-DD').
            initial_position (Union[int, Dict[str, int]]): Starting position(s).
                                                          Int for single/all, Dict for per-ticker.
            latest_only (bool): If True, return only the last row per ticker.

        Returns:
            pd.DataFrame: DataFrame with prices, indicators, signals, and
                          risk management outputs. Indexed by DatetimeIndex (single)
                          or MultiIndex ['ticker', 'date'] (multiple).

        Raises:
            DataRetrievalError: If data fetching fails or data is insufficient.
            ValueError: If parameters are invalid.
        """
        self.logger.info(f"Generating TSI signals for {ticker} from {start_date} to {end_date}")

        try:
            # 1. Fetch Data with Lookback Buffer
            # Use get_historical_prices from BaseStrategy
            prices_df = self.get_historical_prices(
                tickers=ticker,
                from_date=start_date,
                to_date=end_date,
                lookback=self.lookback_buffer # Use calculated lookback
            )

            if prices_df.empty:
                self.logger.warning(f"No price data found for {ticker} in the given range/lookback.")
                return pd.DataFrame()

            # 2. Process Single or Multiple Tickers
            results_list = []
            is_multi_ticker = isinstance(ticker, list) and len(ticker) > 1

            if is_multi_ticker:
                # Ensure input was MultiIndex or handle DataFrame with 'ticker' column if needed
                if not isinstance(prices_df.index, pd.MultiIndex):
                     # This case might occur if get_historical_prices logic changes, add safety check
                     if 'ticker' not in prices_df.columns:
                         raise ValueError("Multi-ticker price data lacks MultiIndex and 'ticker' column.")
                     # If 'ticker' column exists, set MultiIndex (ticker, date)
                     prices_df['date'] = pd.to_datetime(prices_df['date']) # Ensure date is datetime
                     prices_df = prices_df.set_index(['ticker', 'date']).sort_index()
                     if not isinstance(prices_df.index, pd.MultiIndex): # Check conversion
                          raise ValueError("Failed to set MultiIndex for multi-ticker data.")


                initial_positions_dict = initial_position if isinstance(initial_position, dict) else {ticker: initial_position for ticker in ticker}

                for ticker_name, group_df in prices_df.groupby(level=0):
                    self.logger.debug(f"Processing ticker: {ticker_name}")
                    # Drop the ticker level for processing, index becomes DatetimeIndex
                    single_ticker_df = group_df.droplevel(0)

                    # Validate data length for this specific ticker
                    if not self._validate_data(single_ticker_df, min_records=self.min_calculation_records):
                        self.logger.warning(f"Insufficient data for {ticker_name} after lookback ({len(single_ticker_df)} < {self.min_calculation_records}). Skipping.")
                        continue

                    # Calculate signals for this ticker
                    signals_calculated = self._calculate_signals_single(single_ticker_df)

                    # Apply Risk Management
                    ticker_initial_pos = initial_positions_dict.get(ticker_name, 0) # Get specific initial pos
                    rm_applied = self.risk_manager.apply(signals_calculated, ticker_initial_pos)

                    # Add ticker identifier back for concatenation
                    rm_applied['ticker'] = ticker_name
                    results_list.append(rm_applied)

                if not results_list:
                    self.logger.warning("No tickers had sufficient data or produced results.")
                    return pd.DataFrame()

                # Combine results and set MultiIndex
                final_df = pd.concat(results_list)
                final_df['date'] = final_df.index # Index is date, move to column
                final_df['date'] = pd.to_datetime(final_df['date'])
                final_df = final_df.set_index(['ticker', 'date']).sort_index()

            else: # Single Ticker
                ticker_name = ticker if isinstance(ticker, str) else ticker[0]
                self.logger.debug(f"Processing single ticker: {ticker_name}")

                # Validate data length
                if not self._validate_data(prices_df, min_records=self.min_calculation_records):
                    self.logger.warning(f"Insufficient data for {ticker_name} after lookback ({len(prices_df)} < {self.min_calculation_records}).")
                    return pd.DataFrame() # Return empty if not enough data

                # Calculate signals
                signals_calculated = self._calculate_signals_single(prices_df)

                # Apply Risk Management
                single_initial_pos = initial_position if isinstance(initial_position, int) else initial_position.get(ticker_name, 0)
                final_df = self.risk_manager.apply(signals_calculated, single_initial_pos)


            # 3. Final Output Handling
            if latest_only:
                if is_multi_ticker:
                     # Check if index is MultiIndex before grouping
                    if isinstance(final_df.index, pd.MultiIndex):
                        return final_df.groupby(level=0).tail(1)
                    else:
                         # Should not happen if multi-ticker logic is correct, but fallback
                         return final_df.iloc[[-1]]
                else:
                    return final_df.iloc[[-1]]
            else:
                # Ensure correct column order (adjust as needed for compatibility)
                # Keep RM outputs as they are named: 'position', 'return', 'cumulative_return', 'exit_type'
                # Required by framework: 'close', 'signal', 'position'
                # Useful info: 'open', 'high', 'low', TSI indicators, RM return/cum_return/exit_type
                output_cols = [
                    'open', 'high', 'low', 'close', # Prices
                    'tsi', 'signal_line', 'strength', # Indicators
                    'signal', # Raw signal
                    'position', 'return', 'cumulative_return', 'exit_type' # RM Output
                ]
                # Ensure all expected columns exist, add NaN if missing (shouldn't happen ideally)
                for col in output_cols:
                     if col not in final_df.columns:
                          final_df[col] = np.nan
                return final_df[output_cols]

        except DataRetrievalError as e:
            self.logger.error(f"Data retrieval error for {ticker}: {e}")
            raise # Re-raise specific error
        except ValueError as e:
            self.logger.error(f"Parameter validation error: {e}")
            raise # Re-raise specific error
        except Exception as e:
            self.logger.error(f"Unexpected error generating signals for {ticker}: {e}", exc_info=True)
            # Return empty DataFrame on unexpected errors
            return pd.DataFrame()


    def _calculate_signals_single(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Compute TSI indicators and raw signal for a single ticker's price data.

        Args:
            prices (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close' columns,
                                   indexed by DatetimeIndex for a single ticker.

        Returns:
            pd.DataFrame: DataFrame containing OHLC, TSI indicators ('tsi', 'signal_line',
                          'strength'), and the raw 'signal' (1, -1, 0). Ready for RiskManager.
        """
        if not isinstance(prices.index, pd.DatetimeIndex):
             # This check is important as RM expects DatetimeIndex for single ticker
             raise TypeError(f"_calculate_signals_single expects DataFrame with DatetimeIndex, got {type(prices.index)}")

        df = prices.copy() # Work on a copy

        # Calculate TSI and Signal Line
        df['tsi'], df['signal_line'] = self._calculate_tsi(df['close'])

        # Calculate Strength
        df['strength'] = df['tsi'] - df['signal_line']

        # Generate Raw Crossover Signals
        df['signal'] = 0 # Default to no signal
        tsi_cross_above = (df['tsi'] > df['signal_line']) & (df['tsi'].shift(1) <= df['signal_line'].shift(1))
        tsi_cross_below = (df['tsi'] < df['signal_line']) & (df['tsi'].shift(1) >= df['signal_line'].shift(1))

        df.loc[tsi_cross_above, 'signal'] = 1
        df.loc[tsi_cross_below, 'signal'] = -1

        # Apply Long-Only Rule
        if self.long_only:
            df.loc[df['signal'] == -1, 'signal'] = 0 # Override sell signals to hold/exit

        # Select and return columns required by RiskManager + indicators
        # OHLC + signal are minimum for RM. Indicators are for context.
        required_cols = ['open', 'high', 'low', 'close', 'signal',
                         'tsi', 'signal_line', 'strength']

        # Drop rows with NaNs produced during indicator calculation warmup
        df.dropna(subset=['tsi', 'signal_line'], inplace=True) # Drop rows where TSI/signal couldn't be calculated

        return df[required_cols]

    def _calculate_tsi(self, close: pd.Series) -> Tuple[pd.Series, pd.Series]:
        """
        Calculates the True Strength Index (TSI) and its signal line.

        Args:
            close (pd.Series): Series of closing prices.

        Returns:
            Tuple[pd.Series, pd.Series]: (tsi, signal_line)
        """
        delta = close.diff()
        # First smoothing (long period)
        ema1 = delta.ewm(span=self.long_period, adjust=False).mean()
        abs_ema1 = delta.abs().ewm(span=self.long_period, adjust=False).mean()
        # Second smoothing (short period)
        ema2 = ema1.ewm(span=self.short_period, adjust=False).mean()
        abs_ema2 = abs_ema1.ewm(span=self.short_period, adjust=False).mean()

        # TSI Calculation (add epsilon for numerical stability)
        epsilon = 1e-10
        tsi = 100 * (ema2 / (abs_ema2 + epsilon))

        # Signal Line Calculation
        signal_line = tsi.ewm(span=self.signal_period, adjust=False).mean()

        return tsi, signal_line

    def __repr__(self) -> str:
        """Return a string representation of the strategy instance."""
        param_str = ", ".join(f"{k}={v}" for k, v in self.params.items())
        return f"{self.__class__.__name__}({param_str})"