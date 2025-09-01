# trading_system/src/strategies/disparity_index_strat.py

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class DisparityIndexStrategy(BaseStrategy):
    """
    Disparity Index Strategy with Integrated Risk Management.

    Mathematical Explanation:
    ---------------------------
    The strategy calculates the Disparity Index (DI) as:
        DI = ((Close - MA) / MA) * 100,
    where MA is the moving average of the close price over a specified lookback period 
    (default is 14 days). Trading signals are generated based on the transition of DI:
      - A buy signal (signal = 1) is generated when DI becomes positive after a series 
        of consecutive negative DI values (default consecutive period is 4 days).
      - A sell signal (signal = -1) is generated when DI becomes negative after a series 
        of consecutive positive DI values (default consecutive period is 4 days).
    The signal strength reflects the magnitude of the move:
      - For a buy signal, signal_strength = DI.
      - For a sell signal, signal_strength = -DI.
    
    Risk Management:
    ----------------
    The strategy integrates risk management via the RiskManager component. Upon a signal,
    the RiskManager adjusts the entry price for slippage and transaction costs, sets stop-loss
    and take-profit levels, and triggers exits (via stop_loss, take_profit, or signal reversal).
    The realized return is computed as:
      - For long trades: (exit_price / entry_price) - 1.
      - For short trades: (entry_price / exit_price) - 1.
    Cumulative returns are aggregated over time using the trade multipliers.
    
    Inputs and Outputs:
    --------------------
    The strategy supports both backtesting and forecasting:
      - Backtesting: Provide start_date and end_date to return the full history of trading signals.
      - Forecasting: Set latest_only=True to return only the latest signal(s) for end-of-day decision making.

    The final output DataFrame contains:
      'open', 'high', 'low', 'close'    : Price references.
      'di'                              : Computed Disparity Index.
      'signal'                          : Trading signal (1 = buy, -1 = sell, 0 = hold/exit).
      'signal_strength'                 : Magnitude of the signal.
      'position'                        : Final position after risk management.
      'return'                          : Realized trade return.
      'cumulative_return'               : Aggregated return over trades.
      'exit_type'                       : Indicator of the risk management exit event.

    Parameters in `params` include:
      - di_lookback (int): Lookback period for the moving average (default: 14).
      - consecutive_period (int): Number of consecutive days required to trigger a signal reversal (default: 4).
      - stop_loss_pct (float): Stop loss percentage (default: 0.05).
      - take_profit_pct (float): Take profit percentage (default: 0.10).
      - slippage_pct (float): Slippage percentage (default: 0.001).
      - trailing_stop_pct (float): Trailing stop percentage (default: 0.0).
      - transaction_cost_pct (float): Transaction cost percentage (default: 0.00).
      - long_only (bool): Flag to allow only long positions (default: True).

    The strategy is designed to process a single ticker (str) or multiple tickers (List[str])
    using efficient vectorized computations.
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Disparity Index Strategy with risk management parameters.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (Dict, optional): Strategy-specific parameters.
        """
        params = params or {}
        super().__init__(db_config, params)
        
        # Strategy-specific parameters
        self.di_lookback = int(params.get('di_lookback', 14))
        self.consecutive_period = int(params.get('consecutive_period', 4))
        
        # Risk management parameters
        self.stop_loss_pct = params.get('stop_loss_pct', 0.05)
        self.take_profit_pct = params.get('take_profit_pct', 0.10)
        self.trailing_stop_pct = params.get('trailing_stop_pct', 0.0)
        self.slippage_pct = params.get('slippage_pct', 0.001)
        self.transaction_cost_pct = params.get('transaction_cost_pct', 0.00)
        self.long_only = params.get('long_only', True)
        
        # Initialize RiskManager with provided risk parameters
        self.risk_manager = RiskManager(
            stop_loss_pct=self.stop_loss_pct,
            take_profit_pct=self.take_profit_pct,
            trailing_stop_pct=self.trailing_stop_pct,
            slippage_pct=self.slippage_pct,
            transaction_cost_pct=self.transaction_cost_pct
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def generate_signals(self, 
                         ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals with integrated risk management for backtesting or forecasting.
        
        Retrieves historical price data, computes the disparity index and trading signals,
        then applies risk management rules to adjust trade entries and exits.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of symbols.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Initial trading position (0 = flat, 1 = long, -1 = short).
            latest_only (bool): If True, returns only the latest signal(s) for forecasting.
        
        Returns:
            pd.DataFrame: DataFrame containing price data, computed indicators, and risk-managed
                          signals (position, returns, cumulative returns, exit types) suitable for
                          downstream analysis.
        """
        # Determine the required number of historical records
        required_lookback = self.di_lookback + self.consecutive_period + 1
        
        # Retrieve historical price data for the ticker(s)
        prices_df = self._get_price_data(ticker, start_date, end_date, required_lookback, latest_only)
        
        # Calculate the disparity index, trading signals, and signal strength based on close prices
        di, signals, signal_strength = self._calculate_indicators(prices_df['close'])
        
        # Create a unified DataFrame with price data and computed indicators
        signals_df = self._create_signals_df(prices_df, di, signals, signal_strength)
        
        # Apply risk management to compute final trading positions, returns, and exit types
        return self._apply_risk_management(signals_df, initial_position, latest_only)

    def _get_price_data(self, 
                        ticker: Union[str, List[str]], 
                        start_date: Optional[str], 
                        end_date: Optional[str],
                        required_lookback: int,
                        latest_only: bool) -> pd.DataFrame:
        """
        Retrieve historical price data from the database.
        
        Depending on whether forecasting (latest_only) or backtesting is intended,
        fetches a minimum number of recent records or data within a date range.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol(s).
            start_date (str): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str): Backtest end date in 'YYYY-MM-DD' format.
            required_lookback (int): Minimal number of historical records required.
            latest_only (bool): If True, retrieve only the latest records.
        
        Returns:
            pd.DataFrame: DataFrame with columns ['open', 'high', 'low', 'close'] and a datetime index.
            
        Raises:
            DataRetrievalError: If insufficient data is available to meet the requirements.
        """
        if latest_only:
            prices_df = self.get_historical_prices(ticker, lookback=required_lookback)
        else:
            prices_df = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date)
        
        if not self._validate_data(prices_df, min_records=required_lookback):
            raise DataRetrievalError(f"Insufficient data for {ticker}")
            
        return prices_df

    def _calculate_indicators(self, close_prices: pd.Series) -> tuple:
        """
        Compute the Disparity Index and generate trading signals along with signal strength.
        
        For a single ticker, the calculations are performed directly on the closing price series.
        For multiple tickers (using a MultiIndex), the computations are applied group-wise on each ticker.
        
        The Disparity Index (DI) is computed as:
            DI = ((Close - MA) / MA) * 100,
        where MA is the moving average determined by 'di_lookback'.
        
        Trading signals are generated in the following manner:
            - Buy signal (1): Triggered when DI turns positive after being negative for
              'consecutive_period' days.
            - Sell signal (-1): Triggered when DI turns negative after being positive for
              'consecutive_period' days.
            - In long-only mode, sell signals are converted to 0.
        
        Signal strength reflects the magnitude of the move:
            - For buy signals, equals DI.
            - For sell signals, equals -DI.
        
        Args:
            close_prices (pd.Series): Series of closing prices.
            
        Returns:
            tuple: Three pd.Series representing the disparity index (di), trading signals,
                   and signal strength (all aligned to close_prices’ index).
        """
        def calc_indicators(series: pd.Series) -> pd.DataFrame:
            ma = series.rolling(self.di_lookback, min_periods=self.di_lookback).mean()
            di_local = ((series - ma) / ma) * 100
            shifted_di = di_local.shift(1)
            negative_mask = (shifted_di < 0).rolling(self.consecutive_period).sum() == self.consecutive_period
            positive_mask = (shifted_di > 0).rolling(self.consecutive_period).sum() == self.consecutive_period
            # Generate signals: 1 for a buy (when DI turns positive following consecutive negatives),
            # -1 for a sell (when DI turns negative following consecutive positives), 0 otherwise.
            signals_local = np.where(negative_mask & (di_local > 0), 1,
                                     np.where(positive_mask & (di_local < 0), -1, 0))
            # Signal strength: equals DI for buy and -DI for sell.
            signal_strength_local = np.where(signals_local == 1, di_local, 
                                             np.where(signals_local == -1, -di_local, 0.0))
            if self.long_only:
                signals_local[signals_local == -1] = 0
            return pd.DataFrame({'di': di_local,
                                 'signal': signals_local,
                                 'signal_strength': signal_strength_local}, index=series.index)
        
        # If multi-ticker data is provided (MultiIndex), apply group-wise computations.
        if isinstance(close_prices.index, pd.MultiIndex):
            indicators = close_prices.groupby(level=0, group_keys=False).apply(lambda x: calc_indicators(x))
            di = indicators['di']
            signals = indicators['signal']
            signal_strength = indicators['signal_strength']
        else:
            df_ind = calc_indicators(close_prices)
            di = df_ind['di']
            signals = df_ind['signal']
            signal_strength = df_ind['signal_strength']
            
        return di, signals, signal_strength

    def _create_signals_df(self, 
                           prices_df: pd.DataFrame,
                           di: pd.Series, 
                           signals: pd.Series,
                           signal_strength: pd.Series) -> pd.DataFrame:
        """
        Create a consolidated DataFrame merging price data with computed indicators.
        
        Combines the price DataFrame (with columns 'open', 'high', 'low', 'close') with
        the disparity index, trading signals, and signal strength. Only rows with valid DI values
        are retained.
        
        Args:
            prices_df (pd.DataFrame): DataFrame containing price data.
            di (pd.Series): Series with computed disparity index.
            signals (pd.Series): Series with generated trading signals.
            signal_strength (pd.Series): Series with signal strength values.
        
        Returns:
            pd.DataFrame: Consolidated DataFrame with columns for prices and all computed indicators.
        """
        combined_df = pd.concat([
            prices_df[['open', 'high', 'low', 'close']],
            di.rename('di'),
            pd.Series(signals, name='signal'),
            pd.Series(signal_strength, name='signal_strength')
        ], axis=1)
        return combined_df.dropna(subset=['di'])

    def _apply_risk_management(self, 
                               signals_df: pd.DataFrame,
                               initial_position: int,
                               latest_only: bool) -> pd.DataFrame:
        """
        Apply risk management to adjust trading signals into final positions, returns, and exit events.
        
        This method calls the RiskManager's 'apply' function to adjust entries/exits using
        predetermined stop loss, take profit, slippage, and transaction cost parameters. For multi-ticker
        data, the risk management process is performed independently per ticker.
        
        Args:
            signals_df (pd.DataFrame): DataFrame with price data and raw trading signals.
            initial_position (int): The starting trading position (0 for flat, 1 for long, -1 for short).
            latest_only (bool): If True, returns only the latest record per ticker for forecasting.
            
        Returns:
            pd.DataFrame: DataFrame with price data, indicators, and risk-managed fields including:
                'position', 'return', 'cumulative_return', and 'exit_type'.
        """
        if isinstance(signals_df.index, pd.MultiIndex):
            managed_df = signals_df.groupby(level=0, group_keys=False).apply(
                lambda group: self.risk_manager.apply(group, initial_position=initial_position)
            )
            if latest_only:
                managed_df = managed_df.groupby(level=0, group_keys=False).tail(1)
        else:
            managed_df = self.risk_manager.apply(signals_df, initial_position=initial_position)
            if latest_only:
                managed_df = managed_df.tail(1)
        
        return managed_df