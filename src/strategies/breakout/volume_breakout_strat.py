# trading_system/src/strategies/breakout/volume_breakout.py

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class VolumeBreakoutStrategy(BaseStrategy):
    """
    Volume Breakout Strategy with Integrated Risk Management.
    
    This strategy identifies price breakouts that are confirmed by significant volume surges.
    It computes rolling resistance and support levels, and verifies the breakout using volume and
    price change thresholds. Optionally, it employs an ATR filter to account for volatility.
    
    Mathematical Formulation:
      - Resistanceₜ = max(highₜ₋ₙ₊₁, ..., highₜ)
      - Supportₜ = min(lowₜ₋ₙ₊₁, ..., lowₜ)
      - Average Volumeₜ = mean(volumeₜ₋ₘ₊₁, ..., volumeₜ)
      - Price Change = pct_change(closeₜ)
      
      **Buy Signal (signal = 1) Conditions:**  
      1. A number of consecutive bars (≥ consecutive_bars) exist where  
         closeₜ > Resistanceₜ₋₁.
      2. volumeₜ > volume_threshold × Average Volumeₜ.
      3. |Price Change| > price_threshold.
      4. (Optional) |Price Change| > atr_threshold × (ATR / closeₜ) if ATR filtering is enabled.
      
      **Sell Signal (signal = –1) Conditions:**  
      1. A number of consecutive bars (≥ consecutive_bars) exist where  
         closeₜ < Supportₜ₋₁.
      2. volumeₜ > volume_threshold × Average Volumeₜ.
      3. |Price Change| > price_threshold.
      4. (Optional) |Price Change| > atr_threshold × (ATR / closeₜ) if ATR filtering is enabled.
      
      The signal strength is defined as the normalized absolute price change:
          signal_strength = |Price Change| / (price_volatility + 1e-6),
      where price_volatility is (ATR / closeₜ) when ATR filtering is enabled, otherwise |Price Change|.
      
      Risk Management:
      After generating raw signals, this strategy integrates with the external RiskManager,
      which adjusts the entry prices for slippage and transaction costs and applies stop-loss and
      take-profit rules. For long trades:
          Stop Loss = entry_price × (1 – stop_loss_pct)
          Take Profit = entry_price × (1 + take_profit_pct)
      For short trades, these levels are inverted.
    
    Args:
        db_config (DatabaseConfig): Database configuration settings.
        params (Optional[Dict]): Strategy parameters.
    
    Returns:
        pd.DataFrame: A DataFrame containing price data and breakout indicators along with:
          - 'open', 'high', 'low', 'close', 'volume'
          - 'resistance', 'support', 'avg_volume', 'price_change', 'atr', 'price_volatility'
          - 'above_resistance_count', 'below_support_count'
          - 'signal' and 'signal_strength'
          - Risk-managed outputs: 'position', 'rm_strategy_return', 'rm_cumulative_return', 'rm_action'
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Volume Breakout strategy with parameters.
        
        Parameters in `params` (with defaults):
            - lookback_period (int): Period for resistance/support calculation (default: 20).
            - volume_threshold (float): Multiplier to confirm volume breakout (default: 1.5).
            - price_threshold (float): Minimum price move as a fraction (default: 0.02).
            - volume_avg_period (int): Moving average period for volume (default: 20).
            - consecutive_bars (int): Required consecutive bars to validate breakout (default: 1).
            - use_atr_filter (bool): Enable ATR-based volatility filtering (default: False).
            - atr_period (int): Lookback period for ATR calculation (default: 14).
            - atr_threshold (float): ATR multiplier threshold for signal filtering (default: 1.0).
            - stop_loss_pct (float): Stop loss percentage (default: 0.05).
            - take_profit_pct (float): Take profit percentage (default: 0.10).
            - trailing_stop_pct (float): Trailing stop percentage (default: 0.0).
            - slippage_pct (float): Slippage percentage (default: 0.001).
            - transaction_cost_pct (float): Transaction cost percentage (default: 0.001).
            - long_only (bool): If True, only long trades are allowed (default: True).
        """
        default_params = {
            'lookback_period': 20,
            'volume_threshold': 1.5,
            'price_threshold': 0.02,
            'volume_avg_period': 20,
            'consecutive_bars': 1,
            'use_atr_filter': False,
            'atr_period': 14,
            'atr_threshold': 1.0,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }
        # Ensure all default params are present if params is partially provided
        current_params = default_params.copy()
        if params:
            current_params.update(params)
        
        # Ensure numeric params that are used as integers are indeed integers
        # Hyperopt's hp.quniform returns float, so cast is needed before use if strict int is required by pandas.
        # However, most pandas functions handle float window sizes by flooring.
        # Explicit casting in __init__ or where used is safer.
        # For this strategy, casting is handled within generate_signals where parameters are used.
        
        super().__init__(db_config, current_params)
    
    def generate_signals(self,
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals using the Volume Breakout strategy with risk management.
        
        Retrieves historical price data, computes resistance/support levels, average volume, 
        and ATR (if enabled). It then calculates consecutive breakout counts in a vectorized manner,
        generates buy/sell signals based on volume and price move thresholds, computes normalized 
        signal strength, and integrates risk management adjustments.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtesting start date (YYYY-MM-DD).
            end_date (str, optional): Backtesting end date (YYYY-MM-DD).
            initial_position (int): Starting trading position (0, 1, or -1).
            latest_only (bool): If True, returns only the most recent signal row per ticker.
        
        Returns:
            pd.DataFrame: DataFrame containing price data, breakout indicators, raw signals,
                          normalized signal strength, and risk-managed trade metrics.
        """
        
        # Determine extra lookback buffer for indicator calculation.
        # Ensure params used for lookback are integers.
        param_lookback_period = int(self.params['lookback_period'])
        param_volume_avg_period = int(self.params['volume_avg_period'])
        param_atr_period = int(self.params['atr_period']) if self.params['use_atr_filter'] else 0

        extra_buffer = 50 # Increased buffer for safety with various lookbacks
        lookback_length = int(max(
            param_lookback_period, 
            param_volume_avg_period,
            param_atr_period
        ) + extra_buffer)
        
        price_data = self.get_historical_prices(ticker, lookback=lookback_length, from_date=start_date, to_date=end_date)
        if price_data.empty:
            self.logger.warning(f"Insufficient data for ticker(s) {ticker} to generate Volume Breakout signals.")
            return pd.DataFrame()
        
        result = price_data.copy() # Start with a copy to add columns

        if isinstance(price_data.index, pd.MultiIndex):
            # Multi-ticker processing
            price_data = price_data.sort_index() # Ensure sorted for groupby operations
            result = result.sort_index()

            result['resistance'] = price_data.groupby(level=0)['high'].transform(
                lambda x: x.rolling(window=param_lookback_period).max()
            )
            result['support'] = price_data.groupby(level=0)['low'].transform(
                lambda x: x.rolling(window=param_lookback_period).min()
            )
            result['avg_volume'] = price_data.groupby(level=0)['volume'].transform(
                lambda x: x.rolling(window=param_volume_avg_period).mean()
            )
            result['price_change'] = price_data.groupby(level=0)['close'].transform(
                lambda x: x.pct_change(fill_method=None) # Explicit fill_method
            )

            if self.params['use_atr_filter']:
                atr_parts = []
                for _, group_df in price_data.groupby(level=0): # Iterate per ticker
                    if not group_df.empty:
                        atr_group = self._calculate_atr(group_df, period=param_atr_period)
                        atr_parts.append(atr_group)
                
                if atr_parts:
                    atr = pd.concat(atr_parts).sort_index()
                     # Ensure index names match for robust alignment, though concat usually handles this well.
                    if isinstance(atr.index, pd.MultiIndex) and atr.index.names != price_data.index.names:
                        atr.index.names = price_data.index.names
                else: # Fallback if no ATR could be calculated (e.g., all groups too short)
                    atr = pd.Series(np.nan, index=price_data.index)
                
                result['atr'] = atr
                # Ensure price_data['close'] is aligned with atr for division
                # If atr has NaNs (e.g. from insufficient data for a ticker), price_volatility will also have NaNs
                price_volatility = atr.div(price_data['close']) # Using .div for clarity on Series/Series op
            else:
                result['atr'] = pd.Series(0.0, index=price_data.index)
                price_volatility = pd.Series(0.0, index=price_data.index)
            
            result['price_volatility'] = price_volatility
            
            above_mask = result['close'] > result.groupby(level=0)['resistance'].shift(1)
            below_mask = result['close'] < result.groupby(level=0)['support'].shift(1)
            
            def count_consecutive_true(series_bool: pd.Series) -> pd.Series:
                """Counts consecutive True values in a boolean Series."""
                return series_bool.groupby((~series_bool).cumsum()).cumcount() + 1
            
            # Apply count_consecutive per group using transform
            result['above_resistance_count'] = above_mask.groupby(level=0).transform(count_consecutive_true)
            result['above_resistance_count'] = result['above_resistance_count'].where(above_mask, 0)
            
            result['below_support_count'] = below_mask.groupby(level=0).transform(count_consecutive_true)
            result['below_support_count'] = result['below_support_count'].where(below_mask, 0)

        else:
            # Single-ticker processing
            price_data = price_data.sort_index()
            result = result.sort_index()

            result['resistance'] = price_data['high'].rolling(window=param_lookback_period).max()
            result['support'] = price_data['low'].rolling(window=param_lookback_period).min()
            result['avg_volume'] = price_data['volume'].rolling(window=param_volume_avg_period).mean()
            result['price_change'] = price_data['close'].pct_change(fill_method=None) # Explicit fill_method

            if self.params['use_atr_filter']:
                result['atr'] = self._calculate_atr(price_data, period=param_atr_period)
                result['price_volatility'] = result['atr'] / result['close']
            else:
                result['atr'] = pd.Series(0.0, index=price_data.index)
                result['price_volatility'] = pd.Series(0.0, index=price_data.index)
            
            above_mask = result['close'] > result['resistance'].shift(1)
            below_mask = result['close'] < result['support'].shift(1)

            def count_consecutive_true_single(series_bool: pd.Series) -> pd.Series:
                """ Helper for single ticker consecutive counts. """
                return series_bool.groupby((~series_bool).cumsum()).cumcount() + 1

            result['above_resistance_count'] = count_consecutive_true_single(above_mask)
            result['above_resistance_count'] = result['above_resistance_count'].where(above_mask, 0)
            
            result['below_support_count'] = count_consecutive_true_single(below_mask)
            result['below_support_count'] = result['below_support_count'].where(below_mask, 0)

        # Generate raw trading signals
        result['signal'] = 0
        param_consecutive_bars = int(self.params['consecutive_bars'])

        buy_conditions = (
            (result['above_resistance_count'] >= param_consecutive_bars) &
            (result['volume'] > result['avg_volume'] * self.params['volume_threshold']) &
            (result['price_change'].abs() > self.params['price_threshold'])
        )
        if self.params['use_atr_filter']:
            buy_conditions &= (result['price_change'].abs() > result['price_volatility'] * self.params['atr_threshold'])
        
        sell_conditions = (
            (result['below_support_count'] >= param_consecutive_bars) &
            (result['volume'] > result['avg_volume'] * self.params['volume_threshold']) &
            (result['price_change'].abs() > self.params['price_threshold'])
        )
        if self.params['use_atr_filter']:
            sell_conditions &= (result['price_change'].abs() > result['price_volatility'] * self.params['atr_threshold'])
        
        result.loc[buy_conditions, 'signal'] = 1
        if not self.params['long_only']: # Apply short sell signals only if not long_only
            result.loc[sell_conditions, 'signal'] = -1
        # If long_only is True, sell_conditions effectively become exit signals if RiskManager interprets signal 0 as exit.
        # Or, they are ignored if RiskManager only acts on 1 and -1 explicitly.
        # The current logic implies sell signals are simply not generated if long_only.
        # If an explicit exit/flatten signal (0) is desired for long_only from sell_conditions,
        # it would need `result.loc[sell_conditions, 'signal'] = 0` if a long position is active.
        # However, standard practice is that RM handles exits. Raw signal 0 means "no new signal".

        # Compute normalized signal strength
        # Ensure price_volatility is not zero if use_atr_filter is true to avoid division by zero if price_change is non-zero
        # The 1e-6 epsilon handles this.
        if self.params['use_atr_filter']:
            # price_volatility can be 0 if ATR is 0 or close is very large, or if use_atr_filter was false (then it's 0.0)
            # The condition `if self.params['use_atr_filter']` ensures price_volatility is from ATR here.
            # If ATR is genuinely zero (e.g. flat price for `atr_period`), then price_volatility is zero.
            denominator = result['price_volatility'] + 1e-9 # Use a smaller epsilon
            result['signal_strength'] = result['price_change'].abs().div(denominator)
        else:
            result['signal_strength'] = result['price_change'].abs()
        
        # Fill NaNs in signal_strength that might arise from division by zero or NaN inputs
        result['signal_strength'].fillna(0.0, inplace=True)

        # Drop rows with NaN values in essential calculation columns BEFORE RiskManager
        # These NaNs typically occur at the beginning of the series due to rolling calculations.
        essential_cols_for_signal_gen = ['resistance', 'support', 'avg_volume', 'price_change']
        if self.params['use_atr_filter']:
            essential_cols_for_signal_gen.extend(['atr', 'price_volatility'])
        
        # Add 'close' to ensure RiskManager receives valid close prices
        essential_cols_for_signal_gen.append('close')
        
        result.dropna(subset=essential_cols_for_signal_gen, inplace=True)

        if result.empty:
            self.logger.warning(f"DataFrame became empty after dropping NaNs for ticker(s) {ticker}. No signals to process.")
            return pd.DataFrame()

        if latest_only:
            if isinstance(result.index, pd.MultiIndex):
                result = result.groupby(level=0).tail(1)
            else:
                result = result.tail(1)
        
        risk_manager = RiskManager(
            stop_loss_pct=self.params['stop_loss_pct'],
            take_profit_pct=self.params['take_profit_pct'],
            trailing_stop_pct=self.params['trailing_stop_pct'],
            slippage_pct=self.params['slippage_pct'],
            transaction_cost_pct=self.params['transaction_cost_pct']
        )
        
        # RiskManager needs 'open', 'high', 'low', 'close', 'signal'.
        # Ensure 'open' is present. If not in source, 'close' can be a proxy for RM entry calc.
        # BaseStrategy.get_historical_prices provides 'open'.
        rm_input_df = result[['open', 'high', 'low', 'close', 'signal']].copy()
        # Add any other columns from 'result' that might be useful for RM or pass-through,
        # like 'signal_strength', 'atr', etc.
        # RiskManager currently only strictly needs OHLC and signal.
        # For now, pass only required + 'signal_strength' and 'atr' as they are strategy outputs.
        if 'signal_strength' in result.columns:
             rm_input_df['signal_strength'] = result['signal_strength']
        if 'atr' in result.columns: # Pass ATR for potential use or logging
             rm_input_df['atr'] = result['atr']
        
        # Apply Risk Manager
        processed_result = risk_manager.apply(rm_input_df, initial_position=initial_position)
        
        # Merge RM outputs back to the 'result' DataFrame if desired, or return 'processed_result'
        # For consistency with AwesomeOscillator, we want to return a single DataFrame
        # with all strategy indicators AND RM outputs.
        
        # Columns from processed_result: 'position', 'return', 'cumulative_return', 'exit_type', plus original input to RM
        # Rename RM columns
        processed_result = processed_result.rename(columns={
            'return': 'rm_strategy_return', 
            'cumulative_return': 'rm_cumulative_return', 
            'exit_type': 'rm_action'
        })

        # Combine 'result' (which has strategy indicators) with 'processed_result' (RM outputs)
        # Need to be careful about index alignment and dropped NaN rows.
        # 'result' was already filtered by dropna. 'processed_result' index should match this.
        final_df = result.copy() # Start with the strategy indicators dataframe
        
        # Add RM columns to final_df, ensuring alignment
        cols_from_rm = ['position', 'rm_strategy_return', 'rm_cumulative_return', 'rm_action']
        for col in cols_from_rm:
            if col in processed_result.columns:
                final_df[col] = processed_result[col]
            else: # Should not happen if RM runs correctly
                final_df[col] = np.nan 
                self.logger.warning(f"Column {col} missing from RiskManager output.")

        return final_df
    
    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Calculate the Average True Range (ATR) for volatility measurement.
        
        ATR is calculated using Wilder's smoothing method:
            True Range (TR)ₜ = max( highₜ – lowₜ, |highₜ – closeₜ₋₁|, |lowₜ – closeₜ₋₁| )
            ATRₜ = EMA(TR, alpha = 1/period)
        
        Args:
            price_data (pd.DataFrame): DataFrame with 'high', 'low', and 'close' prices,
                                       indexed by date for a single ticker.
            period (int): Lookback period for ATR calculation (default: 14).
        
        Returns:
            pd.Series: Series of ATR values, indexed like price_data.
        """
        if price_data.empty or len(price_data) < period:
            # Return a series of NaNs with the same index if data is insufficient
            return pd.Series(np.nan, index=price_data.index)

        high = price_data['high']
        low = price_data['low']
        # Ensure prev_close has same index as high, low for concat
        prev_close = price_data['close'].shift(1).reindex_like(high) 

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # Fill NaN in true_range that might occur if prev_close is NaN for the first row
        # This first TR value would be `high - low` effectively if prev_close is NaN
        # EWM handles initial NaNs correctly by starting calculation after min_periods.
        
        atr = true_range.ewm(alpha=1.0/period, min_periods=period, adjust=False).mean()
        # Using adjust=False for Wilder's EMA, which is common for ATR.
        # min_periods=period ensures ATR starts after 'period' TR values.
        
        return atr