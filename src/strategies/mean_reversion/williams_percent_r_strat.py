# trading_system/src/strategies/williams_percent_r.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class WilliamsRStrategy(BaseStrategy):
    """
    Williams %R Trading Strategy with Integrated Risk Management.

    The Williams %R indicator is computed as:
        WR = -100 * ((Highest High - Close) / (Highest High - Lowest Low))
    where Highest High and Lowest Low are computed over a lookback period (default 14 days).

    Trading signals are generated as follows:
        - A buy signal (signal = 1) is triggered when the previous WR value is above the
          oversold threshold (default -80) and the current WR value crosses below or equals -80.
        - A sell signal (signal = -1) is triggered when the previous WR value is below the
          overbought threshold (default -20) and the current WR value crosses above or equals -20.
        - In a long-only strategy, sell signals are neutralized (i.e. set to 0).
    
    Signal strength is computed as the absolute difference between the WR value and the 
    relevant threshold for active signals:
        - For a buy:    |WR - (oversold)|
        - For a sell:   |WR - (overbought)|
        - Otherwise:    0.0

    After signal generation, the RiskManager is applied. This component:
        - Adjusts the entry price for slippage and transaction cost.
        - Computes stop-loss and take-profit thresholds.
        - Determines exit events (for stop loss, take profit, or a signal reversal).
        - Computes the realized return and cumulative return.
    
    The strategy supports:
        - Backtesting with a specific date range.
        - End-of-day forecasting (using minimal lookback data).
        - Vectorized processing of a single ticker or a list of tickers.
    
    Output:
        A DataFrame containing for each date (and ticker if applicable):
          - 'open', 'high', 'low', 'close': Price data.
          - 'wr': Williams %R indicator.
          - 'signal': Trading signal (1 for buy, -1 for sell, 0 for hold).
          - 'signal_strength': Strength of the signal.
          - 'position': Trading position after risk management.
          - 'return': Realized trade return on exit events.
          - 'cumulative_return': Cumulative strategy return.
          - 'exit_type': Reason for exit (stop_loss, take_profit, signal_exit).

    Args:
        db_config: Database configuration details.
        params (dict, optional): Strategy-specific parameters including:
            - 'wr_period': Lookback period for WR calculation (default: 14).
            - 'oversold_threshold': Oversold level (default: -80).
            - 'overbought_threshold': Overbought level (default: -20).
            - 'data_lookback': Number of records to retrieve if no date range is given (default: 252).
            - 'long_only': Flag for long-only trading (default: True).
            - 'stop_loss_pct': Stop loss percentage (default: 0.05).
            - 'trailing_stop_pct': Trailing Stop percentage (default: 0.0).
            - 'take_profit_pct': Take profit percentage (default: 0.10).
            - 'slippage_pct': Slippage percentage (default: 0.001).
            - 'transaction_cost_pct': Transaction cost percentage (default: 0.001).

    Methods:
        generate_signals: Generates the Williams %R signals, applies risk management, and returns
                          a DataFrame for backtesting or EOD signal forecast.
    """
    
    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Williams %R Strategy.

        Args:
            db_config: A database configuration object.
            params (dict, optional): Strategy-specific parameters.
        """
        super().__init__(db_config, params)
        self.logger.info("WilliamsRStrategy initialized with parameters: %s", self.params)
        self.long_only = bool(self.params.get('long_only', True))
        # Epsilon for numerical stability in WR calculation (denominator + epsilon)
        self.epsilon = 1e-9 
    
    def _calculate_wr_signals(
        self,
        data: pd.DataFrame, # Expects a copy of the original data for modification
        wr_period: int,
        oversold: int,
        overbought: int
    ) -> pd.DataFrame:
        """
        Helper function to calculate Williams %R and signals for a single DataFrame.
        This function expects `data` to be for a single ticker and sorted by date.
        It returns a DataFrame that might be shorter than input if leading WR values are NaN,
        or an empty DataFrame if no valid WR values can be computed.
        """
        # Calculate Williams %R
        high_roll = data['high'].rolling(window=wr_period, min_periods=wr_period).max()
        low_roll = data['low'].rolling(window=wr_period, min_periods=wr_period).min()
        denominator = high_roll - low_roll
        
        data['wr'] = -100 * ((high_roll - data['close']) / (denominator + self.epsilon))
        data['wr'] = data['wr'].replace([np.inf, -np.inf], np.nan) # Handle potential infinities
        
        # Drop rows where WR could not be calculated (typically leading NaNs from rolling operation
        # or if input prices themselves were NaN causing WR to be NaN).
        # A .copy() is implicitly made by dropna if 'data' might be a view.
        # Explicit .copy() here on `processed_data` ensures it's a new object.
        processed_data = data.dropna(subset=['wr']).copy() 

        if processed_data.empty:
            # If all WR values were NaN or the input DataFrame was too short/problematic
            # to yield any valid WR after dropna.
            return processed_data # Return the empty DataFrame

        # Generate signals using .shift() for previous WR value
        processed_data['prev_wr'] = processed_data['wr'].shift(1)
        
        # Signal conditions (NaNs in prev_wr will result in False, correctly yielding no signal)
        buy_cond = (processed_data['prev_wr'] > oversold) & (processed_data['wr'] <= oversold)
        sell_cond = (processed_data['prev_wr'] < overbought) & (processed_data['wr'] >= overbought)
        
        processed_data['signal'] = 0 # Default to hold
        processed_data.loc[buy_cond, 'signal'] = 1
        processed_data.loc[sell_cond, 'signal'] = -1

        if self.long_only:
            processed_data['signal'] = processed_data['signal'].clip(lower=0)

        # Calculate signal strength (only for active signals)
        processed_data['signal_strength'] = 0.0
        
        buy_signal_mask = processed_data['signal'] == 1
        if buy_signal_mask.any():
            processed_data.loc[buy_signal_mask, 'signal_strength'] = \
                np.abs(processed_data.loc[buy_signal_mask, 'wr'] - oversold)
        
        sell_signal_mask = processed_data['signal'] == -1
        if sell_signal_mask.any():
            processed_data.loc[sell_signal_mask, 'signal_strength'] = \
                np.abs(processed_data.loc[sell_signal_mask, 'wr'] - overbought)
        
        processed_data = processed_data.drop(columns=['prev_wr']) # Clean up temporary column
        
        return processed_data

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate trading signals using the Williams %R indicator, then apply risk management rules.

        Args:
            ticker (str or List[str]): Single ticker symbol or list of ticker strings.
            start_date (str, optional): Backtest start in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end in 'YYYY-MM-DD' format.
            initial_position (int): Starting position (default: 0).
            latest_only (bool): If True, returns only the final row (per ticker in multi-ticker mode).

        Returns:
            pd.DataFrame: A DataFrame containing price data, indicator values, raw signals, 
            risk-managed positions, realized returns, cumulative returns, and exit types.
            Returns an empty DataFrame if no data is retrieved or if signals cannot be generated.
        """
        wr_period = int(self.params.get("wr_period", 14))
        oversold = int(self.params.get("oversold_threshold", -80))
        overbought = int(self.params.get("overbought_threshold", -20))
        
        default_data_lookback = 252 
        data_lookback_param = self.params.get("data_lookback", default_data_lookback)
        try:
            data_lookback = int(data_lookback_param)
        except (ValueError, TypeError): # Added TypeError for None case
            self.logger.warning(f"Invalid data_lookback '{data_lookback_param}', using default {default_data_lookback}.")
            data_lookback = default_data_lookback

        if oversold >= overbought:
            self.logger.error("Configuration error: oversold_threshold must be less than overbought_threshold.")
            raise ValueError("oversold_threshold must be less than overbought_threshold")

        risk_manager = RiskManager(
            stop_loss_pct=self.params.get('stop_loss_pct', 0.05),
            take_profit_pct=self.params.get('take_profit_pct', 0.10),
            trailing_stop_pct=self.params.get('trailing_stop_pct', 0.0),
            slippage_pct=self.params.get('slippage_pct', 0.001),
            transaction_cost_pct=self.params.get('transaction_cost_pct', 0.001)
        )
        
        # Define expected columns for empty DataFrame returns for consistency
        # These are columns that this strategy *might* produce before RiskManager adds its own.
        # RiskManager itself adds 'position', 'return', 'cumulative_return', 'exit_type'.
        # So, the DataFrame passed to RiskManager should have at least 'open','high','low','close','signal'.
        # And this strategy calculates 'wr' and 'signal_strength'.
        empty_df_cols = ['open', 'high', 'low', 'close', 'volume', # from get_historical_prices
                         'wr', 'signal', 'signal_strength', # from this strategy
                         'position', 'return', 'cumulative_return', 'exit_type'] # from RiskManager
        # Deduplicate (though unlikely to have duplicates here)
        empty_df_cols = sorted(list(set(empty_df_cols)))


        if isinstance(ticker, list):
            # Lookback: wr_period for one WR value, +1 for one prev_wr, so wr_period+1 rows minimum.
            required_lookback_for_calc = wr_period + 1 
            # Use data_lookback for full history if no dates, or required_lookback_for_calc if latest_only
            effective_lookback = required_lookback_for_calc if latest_only else data_lookback

            df_all = self.get_historical_prices(
                tickers=ticker, # Renamed for clarity
                lookback=effective_lookback if not (start_date or end_date) else None,
                from_date=start_date, 
                to_date=end_date
            )

            if df_all.empty:
                self.logger.warning(f"No data retrieved for tickers: {ticker} with specified date range/lookback.")
                return pd.DataFrame(columns=empty_df_cols)

            if isinstance(df_all.index, pd.MultiIndex):
                df_all = df_all.reset_index(level=0) # Moves 'ticker' to a column
            
            def process_ticker_group(group: pd.DataFrame) -> pd.DataFrame:
                group_sorted = group.sort_index()
                group_ticker_name = getattr(group, 'name', 'UnknownGroup') # group.name is the ticker
                
                # Check if group has enough data for even one WR value.
                if len(group_sorted) < wr_period:
                    self.logger.warning(
                        f"Ticker {group_ticker_name} has insufficient data ({len(group_sorted)} rows, "
                        f"need {wr_period}) for WR calculation. Skipping signal generation."
                    )
                    # Return DataFrame with original data and NaN/0 for strategy columns
                    # to ensure consistent structure for RiskManager.
                    # These columns will be passed to _calculate_wr_signals which handles empty results,
                    # but this is a pre-emptive return of the original group structure.
                    group_sorted['wr'] = np.nan
                    group_sorted['signal'] = 0
                    group_sorted['signal_strength'] = 0.0
                    return group_sorted
                
                # Pass a copy to avoid modifying the original group slice directly in _calculate_wr_signals
                return self._calculate_wr_signals(group_sorted.copy(), wr_period, oversold, overbought)

            df_processed = df_all.groupby('ticker', group_keys=False).apply(process_ticker_group)
            
            if df_processed.empty:
                self.logger.warning("No signals generated for any ticker after processing (df_processed is empty).")
                return pd.DataFrame(columns=empty_df_cols)
            
            # Filter out groups that might have become all NaNs and only consist of original columns
            # This can happen if `process_ticker_group` returned a group that was too short and had NaNs added.
            # RiskManager needs 'signal'. If 'signal' is not in df_processed, it means something went wrong.
            # However, `_calculate_wr_signals` and `process_ticker_group` ensure these columns are added.
            # df_processed should have 'signal' from non-empty groups.
            
            df_risk = risk_manager.apply(df_processed, initial_position)
            
            if 'ticker' in df_risk.columns and isinstance(df_risk.index, pd.DatetimeIndex):
                df_risk = df_risk.set_index('ticker', append=True)
                df_risk = df_risk.reorder_levels(['ticker', 'date'])
                df_risk = df_risk.sort_index()
            
            if latest_only:
                if not df_risk.empty:
                    # Groupby level='ticker' if MultiIndex, or column 'ticker'
                    groupby_key = level='ticker' if isinstance(df_risk.index, pd.MultiIndex) and 'ticker' in df_risk.index.names else 'ticker'
                    if groupby_key == 'ticker' and 'ticker' not in df_risk.columns and not (isinstance(df_risk.index, pd.MultiIndex) and 'ticker' in df_risk.index.names) :
                         self.logger.warning("Cannot apply latest_only: 'ticker' identifier not found in df_risk index or columns.")
                    else:
                         df_risk = df_risk.groupby(groupby_key, group_keys=False).tail(1)
                # else df_risk is empty, nothing to do.

            return df_risk
        
        else: # Single ticker processing
            required_lookback_for_calc = wr_period + 1
            effective_lookback = required_lookback_for_calc if latest_only else data_lookback
            
            df = self.get_historical_prices(
                ticker=ticker, # Renamed for clarity
                lookback=effective_lookback if not (start_date or end_date) else None,
                from_date=start_date, 
                to_date=end_date
            )

            if not self._validate_data(df, min_records=wr_period):
                error_msg = (f"Insufficient data for {ticker} (min {wr_period} records needed for WR calc). "
                             f"Retrieved {len(df)} records.")
                self.logger.error(error_msg)
                raise DataRetrievalError(error_msg) # As per original behavior
            
            df = df.sort_index()
            df_processed = self._calculate_wr_signals(df.copy(), wr_period, oversold, overbought) # Pass a copy
            
            if df_processed.empty:
                self.logger.warning(f"No valid Williams %R signals generated for {ticker} (df_processed is empty).")
                return pd.DataFrame(columns=empty_df_cols)

            result_df = risk_manager.apply(df_processed, initial_position)

            if latest_only:
                return result_df.iloc[-1:] if not result_df.empty else result_df
            
            return result_df