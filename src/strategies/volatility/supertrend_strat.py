# trading_system/src/strategies/volatility/supertrend_strat.py

import logging
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError
from src.strategies.risk_management import RiskManager


class SupertrendStrategy(BaseStrategy):
    """
    Supertrend Strategy with Integrated Risk Management.
    
    This strategy computes the Supertrend indicator to generate trading signals.
    The Supertrend is built upon the Average True Range (ATR) by calculating
    basic upper and lower bands and then recursively adjusting these to form
    final bands. Based on the final bands, a Supertrend level is determined along
    with a trend direction indicator (-1 for bearish and +1 for bullish). Trading
    signals are generated on the crossovers of the price with the Supertrend. The
    signal strength is computed as the normalized distance between the close and
    Supertrend level. In “long only” mode negative signals are set to 0.
    
    Risk Management is applied via the RiskManager, which adjusts trades for
    slippage, transaction cost, stop loss, take profit, and trailing stop percent.
    These adjustments lead to realized trade returns and cumulative returns,
    making it easy to compute performance metrics downstream (e.g., Sharpe ratio,
    max drawdown).
    
    Strategy Inputs and Hyperparameters:
      - ticker (str or List[str]): Stock ticker symbol(s).
      - start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
      - end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
      - initial_position (int): Starting trading position (0, 1, or -1).
      - latest_only (bool): If True, returns only the final signal row per ticker.
      - params (dict, optional): Dictionary with strategy-specific parameters.
          • 'lookback'            : ATR lookback period (default: 10)
          • 'multiplier'          : ATR multiplier for bands (default: 3.0)
          • 'long_only'           : If True, only long signals are allowed (default: True)
          • 'stop_loss_pct'       : Stop loss percentage (default: 0.05)
          • 'take_profit_pct'     : Take profit percentage (default: 0.10)
          • 'trailing_stop_pct'   : Trailing stop percent (default: 0.0, i.e. disabled)
          • 'slippage_pct'        : Slippage percentage (default: 0.001)
          • 'transaction_cost_pct': Transaction cost percentage (default: 0.001)
    
    Outputs:
      A DataFrame containing:
        - OHLC prices: 'open', 'high', 'low', 'close'.
        - Computed indicator: 'supertrend' and trend direction ('trend').
        - Raw trading signals and normalized signal strength ('signal', 'signal_strength').
        - Risk-managed outputs: 'position', 'return', 'cumulative_return', and 'exit_type'.
    
    The strategy supports both full backtesting (using a clear start_date and end_date)
    and latest signal forecasting (by setting latest_only=True).
    """
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        default_params = {'lookback': 10, 'multiplier': 3.0, 'long_only': True}
        if params:
            default_params.update(params)
        super().__init__(db_config, default_params)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.long_only = bool(self.params.get('long_only', True))
        # Initialize RiskManager with risk-related parameters from params (if provided)
        rm_params = {
            "stop_loss_pct": self.params.get("stop_loss_pct", 0.05),
            "take_profit_pct": self.params.get("take_profit_pct", 0.10),
            "trailing_stop_pct": self.params.get("trailing_stop_pct", 0.0),
            "slippage_pct": self.params.get("slippage_pct", 0.001),
            "transaction_cost_pct": self.params.get("transaction_cost_pct", 0.001)
        }
        self.risk_manager = RiskManager(**rm_params)

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate Supertrend trading signals with risk management adjustments.
        
        Retrieves historical OHLC data from the database, computes all components of
        the Supertrend indicator (including ATR, basic bands, final bands, Supertrend
        level, and trend direction), and generates raw signals based on crossovers.
        Then, risk management rules are applied via the RiskManager to adjust entry
        prices (accounting for slippage and transaction costs) and to compute stop-loss,
        take-profit thresholds, as well as realized and cumulative returns.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of ticker symbols.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Initial trading position (default 0).
            latest_only (bool): If True, returns only the final signal row per ticker.
        
        Returns:
            pd.DataFrame: DataFrame containing OHLC prices, computed Supertrend indicator,
                          raw signals with normalized strength, risk-managed positions,
                          trade returns, cumulative returns, and exit event annotations.
        """
        # Retrieve historical OHLC data with a sufficient lookback period for indicator stability.
        data = self._get_price_data(ticker, start_date, end_date, latest_only)
        if data.empty:
            self.logger.warning("No historical data found.")
            return pd.DataFrame()

        # Process the data per ticker.
        if isinstance(ticker, list):
            groups = []
            # The data is assumed to be a multi-index DataFrame (ticker, date)
            for t, group in data.groupby(level=0):
                group = group.reset_index(level=0, drop=True).sort_index()
                group = self._calculate_supertrend(group)
                group = self._generate_trading_signals(group)
                group = self._apply_risk_management(group, initial_position)
                group['ticker'] = t
                groups.append(group)
            signals = pd.concat(groups).sort_index()
            signals = signals.reset_index().set_index(['ticker', 'date']).sort_index()
            if latest_only:
                signals = signals.groupby('ticker', group_keys=False).tail(1)
        else:
            data = data.sort_index()
            data = self._calculate_supertrend(data)
            signals = self._generate_trading_signals(data)
            signals = self._apply_risk_management(signals, initial_position)
            if latest_only:
                signals = signals.tail(1)
        return signals

    def _get_price_data(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str],
        end_date: Optional[str],
        latest_only: bool
    ) -> pd.DataFrame:
        """
        Retrieve historical OHLC price data from the database.
        
        If latest_only is True, it ensures the retrieval of at least
        max(lookback*3, 252) records for a stable indicator calculation.
        Otherwise, all available data within the date range is returned.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol(s).
            start_date (str or None): Start date in 'YYYY-MM-DD' format.
            end_date (str or None): End date in 'YYYY-MM-DD' format.
            latest_only (bool): If True, limits the data to a minimal lookback.
        
        Returns:
            pd.DataFrame: DataFrame containing historical OHLC data (and 'ticker' column for multi-ticker).
        """
        lookback = int(self.params['lookback'])
        min_records = max(lookback * 3, 252)  # Minimum records for indicator stability.
        try:
            return self.get_historical_prices(
                tickers=ticker,
                from_date=start_date,
                to_date=end_date,
                lookback=min_records if latest_only else None
            )
        except DataRetrievalError as e:
            self.logger.error("Data retrieval failed: %s", e)
            return pd.DataFrame()

    def _calculate_supertrend(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the Supertrend indicator components including ATR, basic bands,
        final bands, and finally determine the Supertrend and trend direction.
        
        Args:
            data (pd.DataFrame): DataFrame with columns 'high', 'low', and 'close'.
        
        Returns:
            pd.DataFrame: Input DataFrame enriched with columns:
                          'tr', 'atr', 'basic_upper', 'basic_lower',
                          'final_upper', 'final_lower', 'supertrend', and 'trend'.
        """
        lookback = int(self.params['lookback'])
        multiplier = self.params['multiplier']

        # Calculate True Range (TR) and then the Average True Range (ATR)
        hl = data['high'] - data['low']
        hc = (data['high'] - data['close'].shift(1)).abs()
        lc = (data['low'] - data['close'].shift(1)).abs()
        data['tr'] = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        data['atr'] = data['tr'].ewm(span=lookback, adjust=False).mean()

        # Compute the Basic Upper and Lower Bands.
        hl2 = (data['high'] + data['low']) / 2
        data['basic_upper'] = hl2 + multiplier * data['atr']
        data['basic_lower'] = hl2 - multiplier * data['atr']

        # Compute Final Upper and Lower Bands recursively.
        data['final_upper'] = self._calculate_final_upper(data)
        data['final_lower'] = self._calculate_final_lower(data)

        # Compute the Supertrend level and trend based on final bands.
        data['supertrend'], data['trend'] = self._calculate_supertrend_trend(data)
        return data

    def _calculate_final_upper(self, data: pd.DataFrame) -> pd.Series:
        """
        Recursively compute the Final Upper Band.
        
        For the first period, it is equal to the Basic Upper Band.
        For subsequent periods, if the current Basic Upper Band is lower than the
        previous Final Upper Band or if the previous close is greater than the previous
        Final Upper Band, then the current Basic Upper Band is used; otherwise,
        the previous Final Upper Band is carried forward.
        
        Args:
            data (pd.DataFrame): DataFrame with columns 'basic_upper' and 'close'.
        
        Returns:
            pd.Series: Series representing the Final Upper Band.
        """
        final_upper = data['basic_upper'].copy()
        for i in range(1, len(final_upper)):
            if (data['basic_upper'].iloc[i] < final_upper.iloc[i-1]) or (data['close'].iloc[i-1] > final_upper.iloc[i-1]):
                final_upper.iloc[i] = data['basic_upper'].iloc[i]
            else:
                final_upper.iloc[i] = final_upper.iloc[i-1]
        return final_upper

    def _calculate_final_lower(self, data: pd.DataFrame) -> pd.Series:
        """
        Recursively compute the Final Lower Band.
        
        For the first period, it is equal to the Basic Lower Band.
        For subsequent periods, if the current Basic Lower Band is higher than the
        previous Final Lower Band or if the previous close is lower than the previous
        Final Lower Band, then the current Basic Lower Band is used; otherwise,
        the previous Final Lower Band is carried forward.
        
        Args:
            data (pd.DataFrame): DataFrame with columns 'basic_lower' and 'close'.
        
        Returns:
            pd.Series: Series representing the Final Lower Band.
        """
        final_lower = data['basic_lower'].copy()
        for i in range(1, len(final_lower)):
            if (data['basic_lower'].iloc[i] > final_lower.iloc[i-1]) or (data['close'].iloc[i-1] < final_lower.iloc[i-1]):
                final_lower.iloc[i] = data['basic_lower'].iloc[i]
            else:
                final_lower.iloc[i] = final_lower.iloc[i-1]
        return final_lower

    def _calculate_supertrend_trend(self, data: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        Determine the Supertrend level and trend direction based on the final bands.
        
        For the first period:
          - If the first close is less than or equal to the Final Upper Band, then
            the Supertrend is set to the Final Upper Band, and the trend is bearish (-1).
          - Otherwise, the Supertrend is set to the Final Lower Band, and the trend is bullish (+1).
        For subsequent periods, based on the previous trend:
          - If the previous trend is bearish (-1) and the current close is less than or equal
            to the Final Upper Band, the trend remains bearish; otherwise, it switches to bullish.
          - If the previous trend is bullish (+1) and the current close is greater than or equal
            to the Final Lower Band, the trend remains bullish; otherwise, it switches to bearish.
        
        Args:
            data (pd.DataFrame): DataFrame with columns 'final_upper', 'final_lower', and 'close'.
        
        Returns:
            Tuple[pd.Series, pd.Series]:
                - Series of the computed Supertrend values.
                - Series of the trend direction (-1 for bearish, +1 for bullish).
        """
        supertrend = np.zeros(len(data))
        trend = np.zeros(len(data))
        if data['close'].iloc[0] <= data['final_upper'].iloc[0]:
            supertrend[0] = data['final_upper'].iloc[0]
            trend[0] = -1
        else:
            supertrend[0] = data['final_lower'].iloc[0]
            trend[0] = 1

        for i in range(1, len(data)):
            if trend[i-1] == -1:
                if data['close'].iloc[i] <= data['final_upper'].iloc[i]:
                    supertrend[i] = data['final_upper'].iloc[i]
                    trend[i] = -1
                else:
                    supertrend[i] = data['final_lower'].iloc[i]
                    trend[i] = 1
            else:
                if data['close'].iloc[i] >= data['final_lower'].iloc[i]:
                    supertrend[i] = data['final_lower'].iloc[i]
                    trend[i] = 1
                else:
                    supertrend[i] = data['final_upper'].iloc[i]
                    trend[i] = -1
        return pd.Series(supertrend, index=data.index), pd.Series(trend, index=data.index)

    def _generate_trading_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate raw trading signals based on Supertrend crossovers.
        
        A buy signal (+1) is generated when the price crosses above the Supertrend from below.
        A sell signal (-1) is generated when the price crosses below the Supertrend from above.
        The signal strength is calculated as:
            signal_strength = (|close - supertrend| / close) * signal.
        In “long only” mode negative signals are clipped and the signal strength is re‐calculated.
        
        Args:
            data (pd.DataFrame): DataFrame containing OHLC data and the computed 'supertrend'.
        
        Returns:
            pd.DataFrame: DataFrame with columns: 'open', 'high', 'low', 'close',
                          'supertrend', 'signal', and 'signal_strength'.
        """
        # Define crossover conditions.
        cross_above = (data['close'].shift(1) < data['supertrend'].shift(1)) & (data['close'] > data['supertrend'])
        cross_below = (data['close'].shift(1) > data['supertrend'].shift(1)) & (data['close'] < data['supertrend'])
        data['signal'] = np.select([cross_above, cross_below], [1, -1], default=0)
        # Calculate signal strength based on the normalized distance.
        data['signal_strength'] = ((data['close'] - data['supertrend']).abs() / data['close']) * data['signal']
        # In long only mode, clip negative signals and re-calculate signal strength.
        if self.long_only:
            data['signal'] = data['signal'].clip(lower=0)
            data['signal_strength'] = ((data['close'] - data['supertrend']).abs() / data['close']) * data['signal']
        return data[['open', 'high', 'low', 'close', 'supertrend', 'signal', 'signal_strength']]

    def _apply_risk_management(self, signals: pd.DataFrame, initial_position: int) -> pd.DataFrame:
        """
        Apply risk management rules on raw signals to compute trade positions,
        realized returns, and cumulative returns.
        
        The RiskManager adjusts the entry price (by incorporating slippage and transaction costs)
        and then sets stop-loss, take-profit, and trailing stop exits. It computes the realized
        trade return for each exit event and updates the trading position accordingly.
        
        Args:
            signals (pd.DataFrame): DataFrame containing raw trading signals and OHLC data.
            initial_position (int): Starting trading position (0, 1, or -1).
        
        Returns:
            pd.DataFrame: DataFrame containing risk-managed outputs along with original OHLC,
                          signals, and risk management annotations (position, return, cumulative_return, exit_type).
        """
        if not signals.empty:
            return self.risk_manager.apply(signals, initial_position=initial_position)
        return pd.DataFrame()