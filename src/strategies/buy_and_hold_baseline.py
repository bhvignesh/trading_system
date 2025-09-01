# trading_system/src/strategies/buy_and_hold_strategy.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import \
    BaseStrategy  # Assuming BaseStrategy is in this path
# from src.database.config import DatabaseConfig # Only if you were to run this standalone for testing
from src.strategies.risk_management import \
    RiskManager  # Assuming RiskManager is in this path


class BuyAndHoldStrategy(BaseStrategy):
    """
    A simple Buy and Hold benchmark strategy.

    This strategy generates a buy signal (1) on the first available data point for
    each ticker and maintains this signal (i.e., signal remains 1). It is intended 
    to serve as a benchmark against which other trading strategies can be compared.

    The signals are then processed by the RiskManager, which will handle the
    actual position taking, cost application, and risk controls (like stop-loss
    or take-profit if configured within RiskManager's parameters).

    The strategy supports:
        - Backtesting with a specific date range.
        - End-of-day "forecasting" (effectively reporting current hold state).
        - Vectorized processing for a list of tickers.

    Output:
        A DataFrame similar to other strategies, containing for each date (and ticker):
          - Price data ('open', 'high', 'low', 'close', 'volume').
          - 'signal': Trading signal (always 1 after initial buy).
          - 'signal_strength': Strength of the signal (1.0 on first buy day, 0.0 after).
          - 'position': Trading position after risk management.
          - 'return': Realized trade return on exit events (if any, via RiskManager).
          - 'cumulative_return': Cumulative strategy return.
          - 'exit_type': Reason for exit (if any, via RiskManager).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the BuyAndHoldStrategy.

        Args:
            db_config: A database configuration object.
            params (dict, optional): Strategy-specific parameters.
                                     Mainly used for RiskManager configuration
                                     and 'data_lookback' from BaseStrategy.
        """
        super().__init__(db_config, params)
        self.logger.info("BuyAndHoldStrategy initialized with parameters: %s", self.params)

        # Initialize RiskManager with parameters from strategy params or defaults
        self.risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05), # Example default
            take_profit_pct=self.params.get('take_profit_pct', 0.10), # Example default
            trailing_stop_pct=self.params.get('trailing_stop_pct', 0.0), # Example default
            slippage_pct=self.params.get('slippage_pct', 0.001), # Example default
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001) # Example default
        )

    def _generate_signals_for_group(self, group_df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function to generate buy-and-hold signals for a single ticker's DataFrame.
        The input DataFrame `group_df` is expected to be sorted by date and have a DatetimeIndex.
        """
        # Ensure working with a copy if group_df is a slice from a larger DataFrame
        df = group_df.copy()

        if df.empty:
            # Ensure essential columns for RiskManager if somehow missing, though unlikely
            # if data comes from get_historical_prices.
            # Price columns should be present. Signal columns are added here.
            df['signal'] = 0
            df['signal_strength'] = 0.0
            return df

        # Signal: Buy on the first day and maintain the signal as 1.
        # RiskManager will interpret this as "enter on first 1, then hold".
        df['signal'] = 1 

        # Signal Strength: 1.0 on the first day the signal is active, 0.0 otherwise.
        # Since 'signal' is 1 for all rows, this means strength 1.0 on the very first row.
        df['signal_strength'] = 0.0
        if not df.empty: # Redundant check as earlier df.empty guard, but safe
            df.iloc[0, df.columns.get_loc('signal_strength')] = 1.0 
        
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
        Generate buy-and-hold signals and apply risk management.

        Args:
            ticker (str or List[str]): Single ticker symbol or list of ticker strings.
            start_date (str, optional): Backtest start in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end in 'YYYY-MM-DD' format.
            initial_position (int): Starting position (default: 0).
            latest_only (bool): If True, returns only the final row (per ticker).

        Returns:
            pd.DataFrame: DataFrame with price data, signals, and risk-managed outputs.
                          Returns an empty DataFrame if no data is retrieved.
        """
        default_data_lookback = 252 # Default lookback if not specified in params
        data_lookback_param = self.params.get("data_lookback", default_data_lookback)
        try:
            data_lookback = int(data_lookback_param)
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid data_lookback parameter '{data_lookback_param}', "
                f"using default {default_data_lookback}."
            )
            data_lookback = default_data_lookback

        # Determine lookback for data retrieval based on mode
        if latest_only:
            # For Buy & Hold, to get the "current" state for one day, minimal data is needed.
            # RiskManager uses current day's close for entry if not already in position.
            effective_lookback_for_retrieval = 1 
        else:
            effective_lookback_for_retrieval = data_lookback
        
        # Define expected columns for consistent empty DataFrame returns
        empty_df_cols = sorted(list(set(
            ['open', 'high', 'low', 'close', 'volume', # from get_historical_prices
             'signal', 'signal_strength',             # from this strategy
             'position', 'return', 'cumulative_return', 'exit_type'] # from RiskManager
        )))

        # Retrieve historical price data using BaseStrategy method
        # The 'lookback' argument to get_historical_prices is used if from_date/to_date are None.
        df_prices = self.get_historical_prices(
            tickers=ticker,
            lookback=effective_lookback_for_retrieval if not (start_date or end_date) else None,
            from_date=start_date,
            to_date=end_date
        )

        if df_prices.empty:
            self.logger.warning(
                f"No price data retrieved for ticker(s): {ticker} with the given parameters. "
                f"Returning empty DataFrame."
            )
            return pd.DataFrame(columns=empty_df_cols)

        if isinstance(ticker, list):
            # --- Multi-ticker processing ---
            # get_historical_prices for multiple tickers returns a DataFrame with MultiIndex ['ticker', 'date']
            if not (isinstance(df_prices.index, pd.MultiIndex) and \
                    'ticker' in df_prices.index.names and \
                    'date' in df_prices.index.names):
                self.logger.error(
                    "Multi-ticker price data from get_historical_prices does not have the "
                    "expected MultiIndex ['ticker', 'date']. Attempting to reconstruct."
                )
                # Attempt to reconstruct if 'ticker' and 'date' are columns
                if 'ticker' in df_prices.columns and 'date' in df_prices.columns:
                    try:
                        df_prices = df_prices.set_index(['ticker', 'date']).sort_index()
                    except Exception as e:
                        self.logger.error(f"Failed to reconstruct MultiIndex for multi-ticker data: {e}")
                        return pd.DataFrame(columns=empty_df_cols)
                else: # Cannot reconstruct
                    self.logger.error("Cannot reconstruct MultiIndex, 'ticker' or 'date' columns missing.")
                    return pd.DataFrame(columns=empty_df_cols)
            
            # Apply signal generation to each ticker group.
            # Each group passed to _generate_signals_for_group will have 'date' as its index.
            df_signals = df_prices.groupby(level='ticker', group_keys=False).apply(
                self._generate_signals_for_group
            )
            
            if df_signals.empty: # Should only happen if all groups were empty, caught by df_prices.empty earlier
                self.logger.warning("Signal generation resulted in an empty DataFrame for multi-ticker processing.")
                return pd.DataFrame(columns=empty_df_cols)

            # Apply RiskManager. It should handle MultiIndex input correctly.
            df_risk_managed = self.risk_manager.apply(df_signals, initial_position)
            
            # Ensure sorting, RiskManager should preserve index structure
            df_risk_managed = df_risk_managed.sort_index()

            if latest_only:
                if not df_risk_managed.empty:
                    # Group by the 'ticker' level of the MultiIndex
                    df_risk_managed = df_risk_managed.groupby(level='ticker', group_keys=False).tail(1)
            
            return df_risk_managed

        else:
            # --- Single ticker processing ---
            # df_prices should be a DataFrame with DatetimeIndex
            if not isinstance(df_prices.index, pd.DatetimeIndex):
                 self.logger.error(
                     f"Price data for single ticker {ticker} is not DatetimeIndexed as expected."
                 )
                 # If 'date' column exists, try to set it as index
                 if 'date' in df_prices.columns:
                     try:
                         df_prices = df_prices.set_index('date').sort_index()
                     except Exception as e:
                         self.logger.error(f"Failed to set 'date' as index for single ticker {ticker}: {e}")
                         return pd.DataFrame(columns=empty_df_cols)
                 else: # Cannot fix
                    return pd.DataFrame(columns=empty_df_cols)

            df_signals = self._generate_signals_for_group(df_prices)

            if df_signals.empty: # Should only happen if df_prices was empty
                self.logger.warning(f"Signal generation resulted in an empty DataFrame for single ticker {ticker}.")
                return pd.DataFrame(columns=empty_df_cols)

            df_risk_managed = self.risk_manager.apply(df_signals, initial_position)

            if latest_only:
                return df_risk_managed.tail(1) if not df_risk_managed.empty else df_risk_managed
            
            return df_risk_managed