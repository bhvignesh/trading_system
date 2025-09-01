# trading_system/src/strategies/cci_oscillator_strat.py

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class CCIStrategy(BaseStrategy):
    """
    Commodity Channel Index (CCI) Strategy with Integrated Risk Management.
    
    Mathematical formulation:
        Typical Price (TP) = (High + Low + Close) / 3
        SMA = Rolling Simple Moving Average of TP over a specified period (cci_period)
        MAD = Rolling Mean Absolute Deviation of TP over the same period
        CCI = (TP - SMA) / (0.015 * MAD)
        
    Trading signal logic:
        - A buy signal (signal = 1) is triggered when the previous CCI is below the lower threshold 
          (cci_lower_band) and the current CCI crosses upward through it.
        - A sell signal (signal = -1) is triggered when the previous CCI is above the upper threshold 
          (cci_upper_band) and the current CCI crosses downward through it.
        - In long-only mode the sell signal is replaced by an exit signal (0).
        - Signal strength is defined as the difference between the current CCI and the threshold that was 
          breached during the crossover.
    
    Risk management is applied via the RiskManager component, which:
        - Adjusts entry prices for slippage and transaction costs.
        - Sets stop loss and take profit thresholds.
        - Monitors exit conditions and computes realized trade returns and cumulative returns.
    
    This strategy supports both backtesting (using a specific date range) and latest signal forecasting 
    (using a smaller recent lookback). It also supports processing for one or multiple tickers simultaneously.
    
    Attributes:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict): Strategy-specific parameters.
        risk_manager (RiskManager): Risk management handler instance.
    
    Strategy-specific parameters (with defaults):
        - 'cci_period': int, period for CCI calculation (default: 20).
        - 'cci_upper_band': float, upper threshold for sell signal (default: 150).
        - 'cci_lower_band': float, lower threshold for buy signal (default: -150).
        - 'lookback_period': int, number of days to fetch for backtesting (default: 252).
        - 'data_source': str, data source identifier (default: 'yfinance').
        - 'stop_loss_pct': float, stop loss percentage (default: 0.05).
        - 'take_profit_pct': float, take profit percentage (default: 0.10).
        - 'trailing_stop_pct': float, trailing stop percentage (default: 0.0).
        - 'slippage_pct': float, estimated slippage as a percentage of the price (default: 0.001).
        - 'transaction_cost_pct': float, estimated transaction cost as a percentage of the price (default: 0.001).
        - 'long_only': bool, if True, only long positions are allowed (default: True).
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the CCIStrategy with provided database configuration and parameters.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Dictionary of strategy parameters.
            
        Raises:
            ValueError: If any strategy parameter is invalid.
        """
        default_params = {
            'cci_period': 20,
            'cci_upper_band': 150,
            'cci_lower_band': -150,
            'lookback_period': 252,
            'data_source': 'yfinance',
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True
        }
        params = default_params | (params or {})
        self._validate_params(params)
        
        super().__init__(db_config, params=params)
        self.risk_manager = RiskManager(
            stop_loss_pct=params['stop_loss_pct'],
            take_profit_pct=params['take_profit_pct'],
            trailing_stop_pct=params['trailing_stop_pct'],
            slippage_pct=params['slippage_pct'],
            transaction_cost_pct=params['transaction_cost_pct']
        )
    
    def _validate_params(self, params: Dict) -> None:
        """
        Validate strategy parameters ensuring logical ranges.
        
        Args:
            params (dict): Dictionary of strategy parameters.
            
        Raises:
            ValueError: If any parameter does not meet acceptable criteria.
        """
        if params['cci_upper_band'] <= params['cci_lower_band']:
            raise ValueError("cci_upper_band must be greater than cci_lower_band")
        if int(params['cci_period']) < 2:
            raise ValueError("cci_period must be at least 2")
        if not (0 < params['stop_loss_pct'] < 1):
            raise ValueError("stop_loss_pct must be between 0 and 1")
        if not (0 < params['take_profit_pct'] < 1):
            raise ValueError("take_profit_pct must be between 0 and 1")
    
    def generate_signals(self, ticker: Union[str, List[str]], start_date: Optional[str] = None,
                         end_date: Optional[str] = None, initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate CCI-based trading signals with risk management adjustments.
        
        This method performs the following:
          1. Retrieves historical price data based on the specified date range for backtesting or uses a 
             reduced recent lookback (3 × cci_period) for latest signal forecasting.
          2. Calculates the CCI indicator using:
                 TP = (High + Low + Close) / 3,
                 SMA = Rolling mean of TP over 'cci_period',
                 MAD = Rolling mean absolute deviation of TP,
                 CCI = (TP - SMA) / (0.015 * MAD).
          3. Generates raw trading signals:
                 - Buy (1) when the previous CCI is below 'cci_lower_band' and the current CCI crosses above.
                 - Sell (-1) when the previous CCI is above 'cci_upper_band' and the current CCI crosses below.
                 - For long-only mode, sell signals are replaced with 0 (exit).
          4. Determines signal strength as the difference between the current CCI and the threshold breached.
          5. Applies risk management via the RiskManager component to adjust for slippage, transaction 
             costs, stop loss, take profit, and to compute realized and cumulative returns.
          6. Supports both full backtesting and latest signal forecasting, as well as processing a single ticker 
             or a list of tickers.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (0 for none, 1 for long, -1 for short).
            latest_only (bool): If True, returns only the most recent signal row per ticker.
            
        Returns:
            pd.DataFrame: DataFrame containing price data, CCI, raw and risk-managed signals,
                          realized returns, cumulative returns, and exit types for risk management.
        """
        try:
            if self.params['long_only'] and initial_position not in {0, 1}:
                raise ValueError("Long-only strategy requires initial_position ∈ {0, 1}")
            
            hist_data = self._get_data(ticker, start_date, end_date, latest_only)
            if hist_data.empty:
                return pd.DataFrame()
            
            # Process for a single ticker
            if isinstance(ticker, str):
                signals = self._calculate_cci_signals(hist_data)
                risk_managed = self.risk_manager.apply(signals, initial_position)
                return risk_managed[-1:] if latest_only else risk_managed
            # Process for multiple tickers
            else:
                frames = []
                for t, group in hist_data.groupby(level=0):
                    group = group.droplevel(0)
                    signals = self._calculate_cci_signals(group)
                    risk_managed = self.risk_manager.apply(signals, initial_position)
                    risk_managed['ticker'] = t
                    if latest_only:
                        risk_managed = risk_managed.iloc[[-1]]
                    frames.append(risk_managed)
                result_df = pd.concat(frames)
                result_df = result_df.reset_index().set_index(['ticker', 'date']).sort_index()
                return result_df
        
        except Exception as e:
            self.logger.error(f"Signal generation failed: {e}")
            return pd.DataFrame()
    
    def _get_data(self, ticker: Union[str, List[str]], start_date: Optional[str],
                  end_date: Optional[str], latest_only: bool) -> pd.DataFrame:
        """
        Retrieve historical price data for the given ticker(s).
        
        For backtesting, fetches data between start_date and end_date using the configured lookback_period.
        For the latest signal forecast, retrieves a reduced window (3 × cci_period) to ensure signal stability.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Data retrieval start date (YYYY-MM-DD).
            end_date (str, optional): Data retrieval end date (YYYY-MM-DD).
            latest_only (bool): Flag to indicate whether only the most recent data window is needed.
            
        Returns:
            pd.DataFrame: Historical price data with a DateTime index for a single ticker or a MultiIndex 
                          (ticker, date) for multiple tickers.
        """
        if latest_only:
            return self.get_historical_prices(
                ticker,
                lookback=3 * int(self.params['cci_period']),
                data_source=self.params['data_source']
            )
        else:
            return self.get_historical_prices(
                ticker,
                lookback=int(self.params['lookback_period']),
                data_source=self.params['data_source'],
                from_date=start_date,
                to_date=end_date
            )
    
    def _validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate that the historical data contains enough records for a stable CCI computation.
        
        Args:
            data (pd.DataFrame): Historical price data.
            
        Returns:
            bool: True if at least (cci_period + 1) records are available; otherwise, False.
        """
        min_records = int(self.params['cci_period']) + 1
        if len(data) < min_records:
            self.logger.warning(f"Insufficient data: {len(data)} records, required {min_records}")
            return False
        return True
    
    def _calculate_cci_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the Commodity Channel Index (CCI) and generate raw trading signals.
        
        The computation proceeds as follows:
          1. Calculate the Typical Price (TP) = (High + Low + Close) / 3.
          2. Compute the rolling Simple Moving Average (SMA) of TP over 'cci_period'.
          3. Compute the rolling Mean Absolute Deviation (MAD) of TP over 'cci_period'.
          4. Calculate CCI = (TP - SMA) / (0.015 * MAD), with MAD values of zero replaced by 1e-6.
          5. Generate signals:
                 - Buy (1) if previous CCI < cci_lower_band and current CCI > cci_lower_band.
                 - Sell (-1) if previous CCI > cci_upper_band and current CCI < cci_upper_band.
                 - For long-only mode, sell signals are replaced with 0.
          6. Calculate signal strength as the difference between the current CCI and the respective threshold.
        
        Args:
            data (pd.DataFrame): Historical price data containing 'high', 'low', and 'close' columns.
            
        Returns:
            pd.DataFrame: DataFrame that includes 'close', 'high', 'low', 'cci', raw 'signal', and 
                          'signal_strength'.
        """
        # Calculate Typical Price (TP)
        tp = (data['high'] + data['low'] + data['close']) / 3
        
        # Compute rolling Simple Moving Average (SMA) of TP
        sma = tp.rolling(window=int(self.params['cci_period'])).mean()
        
        # Compute rolling Mean Absolute Deviation (MAD) of TP
        mad = tp.rolling(window=int(self.params['cci_period'])).apply(
            lambda x: np.abs(x - x.mean()).mean(),
            raw=True
        )
        
        # Calculate CCI (avoid division by zero by replacing 0 with 1e-6)
        cci = (tp - sma) / (0.015 * mad.replace(0, 1e-6))
        
        # Prepare DataFrame with necessary columns
        signals = pd.DataFrame({
            'close': data['close'],
            'high': data['high'],
            'low': data['low'],
            'cci': cci,
            'signal': 0
        }, index=data.index)
        
        # Generate trading signals based on CCI crossovers
        prev_cci = signals['cci'].shift()
        buy = (prev_cci < int(self.params['cci_lower_band'])) & (signals['cci'] > int(self.params['cci_lower_band']))
        sell = (prev_cci > int(self.params['cci_upper_band'])) & (signals['cci'] < int(self.params['cci_upper_band']))
        signals['signal'] = np.select([buy, sell], [1, -1], default=0)
        
        # In long-only mode, replace sell signals with 0 (exit)
        if self.params['long_only']:
            signals.loc[sell, 'signal'] = 0
        
        # Compute signal strength as the difference between the current CCI and the threshold crossed
        signals['signal_strength'] = signals['cci'] - np.where(
            buy, int(self.params['cci_lower_band']),
            np.where(sell, int(self.params['cci_upper_band']), np.nan)
        )
        
        return signals