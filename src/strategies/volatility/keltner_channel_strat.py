# trading_system/src/strategies/volatility/keltner_channel.py

import logging
from datetime import timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class KeltnerChannelStrategy(BaseStrategy):
    """
    Keltner Channel Strategy with Integrated Risk Management Component.
    
    This strategy generates trading signals based on the Keltner Channel indicator.
    It computes an exponential moving average (EMA) of the closing price as the channel middle,
    calculates the Average True Range (ATR) using an EMA of the true range, and defines the upper
    and lower channel boundaries as:
        kc_upper = kc_middle + multiplier * ATR
        kc_lower = kc_middle - multiplier * ATR
        
    True Range (TR) is defined as:
        TR = max(high - low, |high - previous close|, |low - previous close|)
        
    Trading signals are generated based on the following rules:
        • Buy Signal (1): Triggered when the close is below kc_lower and shows upward momentum (close > previous close).
        • Sell Signal (-1): Triggered when the close is above kc_upper and shows downward momentum (close < previous close).
          For long-only strategies, sell signals are replaced with an exit (0).
          
    Signal strength is quantified as:
        • For a buy signal: (kc_lower - close) / ATR.
        • For a sell signal: (close - kc_upper) / ATR.
        
    The raw signals are further risk-managed via the RiskManager, which applies stop-loss, 
    take-profit, slippage, and transaction cost adjustments. The final output is a comprehensive
    DataFrame that includes price data, technical indicators, raw signals, risk-managed positions,
    trade returns, and cumulative returns. This output can be used for full backtesting as well as
    for obtaining the latest end-of-day trading signal.
    
    This strategy supports both single and multi-ticker analyses through vectorized operations.
    
    Args:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict, optional): Strategy-specific hyperparameters. Supported parameters (with defaults):
            - kc_span (int): Period for EMA channel middle (default: 20).
            - atr_span (int): Period for ATR calculation (default: 10).
            - multiplier (float): Multiplier for scaling ATR to define channel boundaries (default: 2.0).
            - long_only (bool): If True, only long positions are allowed (default: True).
            - stop_loss (float): Stop loss percentage (default: 0.05).
            - take_profit (float): Take profit percentage (default: 0.10).
            - trailing_stop_pct (float): Trailing stop percentage (default: 0.0).
            - slippage (float): Slippage percentage (default: 0.001).
            - transaction_cost (float): Transaction cost percentage (default: 0.001).
    """
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Keltner Channel Strategy with database configuration and parameters.
        """
        super().__init__(db_config, params)
        self._validate_params()
        self.risk_manager = self._init_risk_manager()

    def _validate_params(self):
        """
        Validate and set default strategy parameters.
        
        Raises:
            ValueError: If any parameter (except risk percentages) is non-positive.
        """
        self.params.setdefault('kc_span', 20)
        self.params.setdefault('atr_span', 10)
        self.params.setdefault('multiplier', 2.0)
        self.params.setdefault('stop_loss', 0.05)
        self.params.setdefault('take_profit', 0.10)
        self.params.setdefault('slippage', 0.001)
        self.params.setdefault('trailing_stop_pct', 0.0)
        self.params.setdefault('transaction_cost', 0.001)
        self.params.setdefault('long_only', True)

        self.params['kc_span'] = int(self.params['kc_span'])
        self.params['atr_span'] = int(self.params['atr_span'])

        # Ensure non-risk parameters are positive
        for k, v in self.params.items():
            if k not in ['stop_loss', 'take_profit', 'trailing_stop_pct'] and v <= 0:
                raise ValueError(f"Parameter {k} must be positive")
        if self.params['stop_loss'] < 0 or self.params['take_profit'] < 0:
            raise ValueError("Risk parameters cannot be negative")

    def _init_risk_manager(self) -> RiskManager:
        """
        Initialize the RiskManager with strategy risk parameters.
        
        Returns:
            RiskManager: Instance for managing trade risk.
        """
        return RiskManager(
            stop_loss_pct=self.params['stop_loss'],
            take_profit_pct=self.params['take_profit'],
            trailing_stop_pct=self.params['trailing_stop_pct'],
            slippage_pct=self.params['slippage'],
            transaction_cost_pct=self.params['transaction_cost']
        )

    def generate_signals(self, ticker: Union[str, List[str]],
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals with risk management adjustments.
        
        The method retrieves historical price data for one or multiple tickers, computes the Keltner Channel
        components (EMA of close, ATR, upper and lower channels), and generates raw trading signals. It then
        applies risk management rules to adjust trade entries for slippage, transaction costs, stop loss, 
        and take profit.
        
        Depending on the parameters, it returns a DataFrame suitable for backtesting over a specified date range,
        including price data, technical indicators, raw signals, and risk-managed trading metrics. It also
        supports retrieving only the latest signal for end-of-day trading decisions.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (0 for no position, 1 for long, -1 for short).
            latest_only (bool): If True, returns only the most recent record per ticker.
            
        Returns:
            pd.DataFrame: DataFrame containing price data, Keltner Channel indicators, raw and risk-managed signals,
                          positions, realized returns, cumulative returns, and risk management exit indicators.
                          
        Raises:
            DataRetrievalError: If data retrieval or signal generation fails.
        """
        self.logger.info(f"Processing ticker: {ticker} with parameters: {self.params}")

        # Determine extra lookback period for stable indicator calculation.
        max_lookback = max(self.params['kc_span'], self.params['atr_span'])
        required_bars = max_lookback * 2  # Buffer period

        try:
            data = self._fetch_data(ticker, start_date, end_date, required_bars, latest_only)
            if data.empty:
                return pd.DataFrame()

            # Compute technical indicators and raw signals in a vectorized manner.
            signals = self._calculate_components(data)

            # Apply risk management adjustments.
            risk_managed = self.risk_manager.apply(signals, initial_position)

            # Consolidate price data, technical components, and risk-managed trade metrics.
            full_df = self._format_output(data, signals, risk_managed)

            # Final filtering: apply date filters and, if requested, return only the most recent signal.
            return self._filter_results(full_df, start_date, end_date, latest_only)

        except Exception as e:
            self.logger.error(f"Signal generation failed: {str(e)}")
            raise DataRetrievalError(f"Failed processing ticker: {ticker}") from e

    def _fetch_data(self, ticker: Union[str, List[str]],
                    start_date: Optional[str],
                    end_date: Optional[str],
                    lookback: int,
                    latest_only: bool) -> pd.DataFrame:
        """
        Retrieve historical price data with an extended lookback period for stable indicator calculations.
        
        If latest_only is True, retrieves only a fixed number of the most recent records. Otherwise, the start date
        is extended backwards by the lookback period.
        
        Args:
            ticker (str or List[str]): Single or multiple ticker symbols.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            lookback (int): Number of extra bars to use for stable indicator computation.
            latest_only (bool): If True, fetch only the most recent records.
            
        Returns:
            pd.DataFrame: DataFrame containing historical price data.
        """
        if latest_only:
            return self.get_historical_prices(ticker, lookback=lookback)

        if start_date and end_date:
            adjusted_start = (pd.to_datetime(start_date) - timedelta(days=lookback)).strftime('%Y-%m-%d')
            return self.get_historical_prices(ticker, from_date=adjusted_start, to_date=end_date)
        
        return self.get_historical_prices(ticker, lookback=lookback)

    def _calculate_components(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Compute Keltner Channel components and raw signals using vectorized operations.
        
        For each ticker (if multiple tickers are provided) the following are calculated:
          - True Range (TR): max(high - low, |high - previous close|, |low - previous close|)
          - Average True Range (ATR): Exponential moving average (EMA) of TR over the specified period.
          - Channel middle (kc_middle): EMA of the close prices over the specified period.
          - Upper and lower channel boundaries:
                kc_upper = kc_middle + multiplier * ATR
                kc_lower = kc_middle - multiplier * ATR
          - Trading signals:
                • Buy (1): When close < kc_lower and there is upward momentum (close > previous close).
                • Sell (-1): When close > kc_upper and there is downward momentum (close < previous close).
          - Signal strength:
                • For a buy signal: (kc_lower - close) / ATR.
                • For a sell signal: (close - kc_upper) / ATR.
                
        Args:
            prices (pd.DataFrame): DataFrame containing 'close', 'high', and 'low' columns. For multiple tickers,
                                   the index is expected to be a MultiIndex with levels ['ticker', 'date'].
                                   
        Returns:
            pd.DataFrame: DataFrame with technical indicators (kc_middle, kc_upper, kc_lower),
                          raw trading signals ('signal') and corresponding normalized 'signal_strength'.
        """
        df = prices.copy()
        
        # Process multi-ticker data if index is a MultiIndex containing 'ticker'
        if isinstance(df.index, pd.MultiIndex) and 'ticker' in df.index.names:
            # Compute previous close per ticker.
            df['prev_close'] = df.groupby(level='ticker')['close'].shift(1)
            # Calculate True Range (TR)
            tr1 = df['high'] - df['low']
            tr2 = (df['high'] - df['prev_close']).abs()
            tr3 = (df['low'] - df['prev_close']).abs()
            df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            # Compute ATR and channel middle using group-wise exponential moving averages.
            df['atr'] = df.groupby(level='ticker')['tr'].transform(lambda x: x.ewm(span=self.params['atr_span'], adjust=False).mean())
            df['kc_middle'] = df.groupby(level='ticker')['close'].transform(lambda x: x.ewm(span=self.params['kc_span'], adjust=False).mean())
            
            # Determine the channel boundaries.
            df['kc_upper'] = df['kc_middle'] + self.params['multiplier'] * df['atr']
            df['kc_lower'] = df['kc_middle'] - self.params['multiplier'] * df['atr']
            
            # Calculate buy and sell conditions using the ticker-specific previous close.
            prev_close = df.groupby(level='ticker')['close'].shift(1)
            buy_cond = (df['close'] < df['kc_lower']) & (df['close'] > prev_close)
            sell_cond = (df['close'] > df['kc_upper']) & (df['close'] < prev_close)
            
            df['signal'] = np.select([buy_cond, sell_cond], [1, -1], 0)
            long_strength = (df['kc_lower'] - df['close']) / df['atr']
            short_strength = (df['close'] - df['kc_upper']) / df['atr']
            df['signal_strength'] = np.where(df['close'] < df['kc_lower'], long_strength,
                                             np.where(df['close'] > df['kc_upper'], short_strength, 0))
                                             
            # Remove temporary columns.
            df.drop(columns=['prev_close', 'tr'], inplace=True)
            
            # For long-only strategies, override sell signals.
            if self.params.get('long_only', True):
                df.loc[sell_cond, 'signal'] = 0
                
            return df
        else:
            # Single ticker processing.
            df['prev_close'] = df['close'].shift(1)
            tr1 = df['high'] - df['low']
            tr2 = (df['high'] - df['prev_close']).abs()
            tr3 = (df['low'] - df['prev_close']).abs()
            df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            df['atr'] = df['tr'].ewm(span=self.params['atr_span'], adjust=False).mean()
            df['kc_middle'] = df['close'].ewm(span=self.params['kc_span'], adjust=False).mean()
            df['kc_upper'] = df['kc_middle'] + self.params['multiplier'] * df['atr']
            df['kc_lower'] = df['kc_middle'] - self.params['multiplier'] * df['atr']
            
            prev_close = df['close'].shift(1)
            buy_cond = (df['close'] < df['kc_lower']) & (df['close'] > prev_close)
            sell_cond = (df['close'] > df['kc_upper']) & (df['close'] < prev_close)
            
            df['signal'] = np.select([buy_cond, sell_cond], [1, -1], 0)
            long_strength = (df['kc_lower'] - df['close']) / df['atr']
            short_strength = (df['close'] - df['kc_upper']) / df['atr']
            df['signal_strength'] = np.where(df['close'] < df['kc_lower'], long_strength,
                                             np.where(df['close'] > df['kc_upper'], short_strength, 0))
                                              
            df.drop(columns=['prev_close', 'tr'], inplace=True)
            if self.params.get('long_only', True):
                df.loc[sell_cond, 'signal'] = 0
                
            return df

    def _format_output(self, prices: pd.DataFrame, signals: pd.DataFrame,
                       risk_managed: pd.DataFrame) -> pd.DataFrame:
        """
        Merge and format the outputs from price data, technical indicator computations, and risk management.
        
        The consolidated DataFrame includes:
          - Price references (open, high, low, close).
          - Keltner Channel indicators (kc_middle, kc_upper, kc_lower) and raw signals with normalized signal strength.
          - Risk-managed trade metrics (position, return, cumulative_return, exit_type).
        
        Any missing data is forward-filled and then dropped to produce a continuous output suitable
        for backtesting and further performance analysis.
        
        Args:
            prices (pd.DataFrame): Original historical price data.
            signals (pd.DataFrame): DataFrame with computed technical indicators and raw signals.
            risk_managed (pd.DataFrame): DataFrame with risk-managed trading metrics.
            
        Returns:
            pd.DataFrame: Final consolidated DataFrame.
        """
        combined = pd.concat([
            prices[['open', 'high', 'low', 'close']],
            signals[['kc_middle', 'kc_upper', 'kc_lower', 'signal', 'signal_strength']],
            risk_managed[['position', 'return', 'cumulative_return', 'exit_type']]
        ], axis=1)
        return combined.ffill().dropna()

    def _filter_results(self, df: pd.DataFrame, start_date: Optional[str],
                        end_date: Optional[str], latest_only: bool) -> pd.DataFrame:
        """
        Apply date filtering and (optionally) return only the latest record per ticker.
        
        For multi-ticker data (with a MultiIndex that includes 'date'), the filtering is applied
        on the 'date' level.
        
        Args:
            df (pd.DataFrame): Merged DataFrame of backtesting results.
            start_date (str, optional): Start date (YYYY-MM-DD) for filtering.
            end_date (str, optional): End date (YYYY-MM-DD) for filtering.
            latest_only (bool): If True, return only the final record per ticker.
            
        Returns:
            pd.DataFrame: Filtered DataFrame sorted chronologically.
        """
        if start_date and end_date:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            if isinstance(df.index, pd.MultiIndex) and 'date' in df.index.names:
                mask = (df.index.get_level_values('date') >= start) & (df.index.get_level_values('date') <= end)
            else:
                mask = (df.index >= start) & (df.index <= end)
            df = df.loc[mask]
            
        if latest_only:
            if isinstance(df.index, pd.MultiIndex) and 'ticker' in df.index.names:
                return df.groupby(level='ticker', group_keys=False).tail(1)
            else:
                return df.tail(1)
            
        return df.sort_index()

    def __repr__(self) -> str:
        """
        Return a string representation of the KeltnerChannelStrategy instance with key parameters.
        
        Returns:
            str: Summary string of the strategy configuration.
        """
        return (f"KeltnerChannelStrategy(kc_span={self.params['kc_span']}, "
                f"atr_span={self.params['atr_span']}, multiplier={self.params['multiplier']})")