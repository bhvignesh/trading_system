# src/strategies/advanced/enhanced_market_pressure_strat.py

""" TODO: implement 'gap_threshold' parameter- A gap threshold would be used to handle price gaps 
between trading sessions. In stock markets, especially those with limited trading hours like the 
Indian markets, prices can "gap" up or down between the close of one session and the open of the next.

This parameter could be implemented to:

Detect significant overnight gaps (when the opening price differs from the previous closing price by more than the threshold)
Adjust strategy behavior in response to gaps (e.g., avoid entering positions after a large gap or use special handling for stop-loss orders)
Filter out false signals that might occur due to gaps
"""

"""strat credit: https://www.jamessawyer.co.uk/market-pressure-analysis-page.html"""

import logging
import warnings
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from scipy.stats import beta, kstest, norm

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class EnhancedMarketPressureStrategy(BaseStrategy):
    """
    Enhanced Market Pressure Analysis Strategy.
    
    This strategy transforms ordinary price data into statistical insights by analyzing
    the normalized position of closing prices within their daily ranges. It models these
    positions using probability distributions to reveal underlying market forces that
    conventional indicators may miss.
    
    Key features:
    - Normalized position modeling: (close - low) / (high - low)
    - Statistical distribution fitting (Beta and optionally other distributions)
    - Volume-weighted analysis for higher confidence
    - Divergence detection between price trends and pressure trends
    - Statistical significance testing to validate signals
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        """
        Initialize the Enhanced Market Pressure Analysis Strategy.

        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (Optional[Dict]): Strategy parameters. Expected keys include:
                - 'window' (default: 20): Rolling window size for analysis
                - 'volume_weighted' (default: True): Whether to weight by volume
                - 'confidence_threshold' (default: 0.95): Statistical confidence threshold
                - 'pressure_threshold' (default: 0.3): Threshold for pressure to generate signal
                - 'long_only' (default: True): Whether to only take long positions
                - 'use_multiple_dists' (default: False): Use multiple distributions
                - 'price_trend_window' (default: 5): Window for price trend calculation
                - 'pressure_trend_window' (default: 5): Window for pressure trend calculation
                - 'stop_loss_pct' (default: 0.05): Stop loss percentage
                - 'take_profit_pct' (default: 0.10): Take profit percentage
                - 'trailing_stop_pct' (default: 0): Fractional distance from the peak (for long) or trough (for short)
                                   that the price is allowed to reverse before the trade is exited.
                - 'slippage_pct' (default: 0.001): Slippage percentage
                - 'transaction_cost_pct' (default: 0.001): Transaction cost percentage
                - 'bull_div_threshold' (default: 0.01): Threshold for bullish divergence
                - 'bear_div_threshold' (default: 0.01): Threshold for bearish divergence
        """
        default_params = {
            'window': 20, 
            'volume_weighted': True,
            'confidence_threshold': 0.95,
            'pressure_threshold': 0.3,
            'long_only': True,
            'use_multiple_dists': False,
            'price_trend_window': 5,
            'pressure_trend_window': 5,
            'bull_div_threshold': 0.01,
            'bear_div_threshold': 0.01,
        }

        params = params or default_params
        super().__init__(db_config, params)
        
        # Initialize strategy parameters
        self.window = int(params.get('window', default_params['window']))
        self.volume_weighted = params.get('volume_weighted', default_params['volume_weighted'])
        self.confidence_threshold = params.get('confidence_threshold', default_params['confidence_threshold'])
        self.pressure_threshold = params.get('pressure_threshold', default_params['pressure_threshold'])
        self.long_only = params.get('long_only', default_params['long_only'])
        self.use_multiple_dists = params.get('use_multiple_dists', default_params['use_multiple_dists'])
        self.price_trend = int(params.get('price_trend_window', default_params['price_trend_window']))
        self.pressure_trend = int(params.get('pressure_trend_window', default_params['pressure_trend_window']))
        self.bull_div_threshold = params.get('bull_div_threshold', default_params['bull_div_threshold'])
        self.bear_div_threshold = params.get('bear_div_threshold', default_params['bear_div_threshold'])
        
        # Initialize logger
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize RiskManager with risk parameters
        risk_params = {
            'stop_loss_pct': params.get('stop_loss_pct', 0.05),
            'take_profit_pct': params.get('take_profit_pct', 0.10),
            'trailing_stop_pct': params.get('trailing_stop_pct', 0),
            'slippage_pct': params.get('slippage_pct', 0.001),
            'transaction_cost_pct': params.get('transaction_cost_pct', 0.001),
        }
        self.risk_manager = RiskManager(**risk_params)
    
    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False,
    ) -> pd.DataFrame:
        """
        Generate trading signals based on the Enhanced Market Pressure Analysis.
        
        Args:
            ticker (str or List[str]): Stock ticker symbol or list of tickers.
            start_date (str, optional): Backtest start date in 'YYYY-MM-DD' format.
            end_date (str, optional): Backtest end date in 'YYYY-MM-DD' format.
            initial_position (int): Starting trading position (default=0).
            latest_only (bool): If True, returns only the final row (per ticker for multi-ticker scenarios).
            
        Returns:
            pd.DataFrame: DataFrame containing signals and performance metrics.
        """
        try:
            # Define lookback buffer to ensure we have enough data for the window
            lookback_buffer = 2 * self.window
            
            # Retrieve historical price data
            if start_date and end_date:
                data = self.get_historical_prices(ticker, from_date=start_date, to_date=end_date, lookback=lookback_buffer)
            else:
                data = self.get_historical_prices(ticker, lookback=lookback_buffer)
                data = data.sort_index()
            
            # Process multiple tickers if provided
            if isinstance(ticker, list):
                signals_list = []
                for t, group in data.groupby(level=0):
                    if not self._validate_data(group, min_records=self.window):
                        self.logger.warning(f"Insufficient data for {t}: required at least {self.window} records.")
                        continue
                    
                    # Calculate signals for a single ticker
                    sig = self._calculate_signals_single(group)
                    
                    # Apply risk management
                    sig = self.risk_manager.apply(sig, initial_position)
                    
                    # Calculate performance metrics
                    sig['daily_return'] = sig['close'].pct_change().fillna(0)
                    sig['strategy_return'] = sig['daily_return'] * sig['position'].shift(1).fillna(0)
                    
                    # Rename risk-managed return columns for clarity
                    sig.rename(columns={
                        'return': 'rm_strategy_return',
                        'cumulative_return': 'rm_cumulative_return',
                        'exit_type': 'rm_action'
                    }, inplace=True)
                    
                    # Add ticker column
                    sig['ticker'] = t
                    signals_list.append(sig)
                
                if not signals_list:
                    return pd.DataFrame()
                    
                signals = pd.concat(signals_list)
                
                # If latest_only is True, take only the last row per ticker
                if latest_only:
                    signals = signals.groupby('ticker').tail(1)
            else:
                # Process single ticker
                if not self._validate_data(data, min_records=self.window):
                    self.logger.warning(f"Insufficient data for {ticker}: required at least {self.window} records.")
                    return pd.DataFrame()
                
                # Calculate signals for the single ticker
                signals = self._calculate_signals_single(data)
                
                # Apply risk management
                signals = self.risk_manager.apply(signals, initial_position)
                
                # Calculate performance metrics
                signals['daily_return'] = signals['close'].pct_change().fillna(0)
                signals['strategy_return'] = signals['daily_return'] * signals['position'].shift(1).fillna(0)
                
                # Rename risk-managed return columns
                signals.rename(columns={
                    'return': 'rm_strategy_return',
                    'cumulative_return': 'rm_cumulative_return',
                    'exit_type': 'rm_action'
                }, inplace=True)
                
                # Return only the latest signal if requested
                if latest_only:
                    signals = signals.iloc[[-1]].copy()
            
            return signals
            
        except Exception as e:
            self.logger.error(f"Error generating signals for {ticker}: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_signals_single(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the Enhanced Market Pressure Analysis and corresponding trading signals for a single ticker.
        
        This method:
        1. Calculates normalized positions of closes within their ranges
        2. Applies statistical distribution modeling to these positions
        3. Computes buying and selling pressure metrics
        4. Detects divergences between price trends and pressure trends
        5. Generates trading signals based on pressure and divergences
        
        Args:
            data (pd.DataFrame): Historical price data for a single ticker with OHLCV columns.
            
        Returns:
            pd.DataFrame: DataFrame with calculated signals and metrics.
        """
        # Ensure data is sorted by date
        data = data.sort_index()
        df = data.copy()
        
        # Calculate normalized position: (close - low) / (high - low)
        df['range'] = df['high'] - df['low']
        df['norm_pos'] = np.where(
            df['range'] > 0,
            (df['close'] - df['low']) / df['range'],
            0.5  # Default to middle when range is zero
        )
        
        # Calculate volatility metrics
        df['volatility'] = df['range'].rolling(window=self.window).std().fillna(0)
        df['vol_ratio'] = df['volatility'] / df['volatility'].rolling(window=self.window).mean().replace(0, 1e-9).fillna(1)
        
        # Volatility-adjusted position
        df['vap'] = df['norm_pos'] * (1 + df['vol_ratio'])
        
        # Z-score normalization of position
        df['z_pos'] = (df['norm_pos'] - df['norm_pos'].rolling(window=self.window).mean()) / \
                      df['norm_pos'].rolling(window=self.window).std().replace(0, 1e-9).fillna(1)
        
        # Initialize columns for pressure metrics
        df['buying_pressure'] = np.nan
        df['selling_pressure'] = np.nan
        df['market_pressure'] = np.nan
        df['pressure_significance'] = np.nan
        
        # Process data using vectorized operations where possible
        buying_pressure = []
        selling_pressure = []
        market_pressure = []
        pressure_significance = []
        
        # Suppress warnings from scipy during distribution fitting
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            
            # Process each window
            for i in range(self.window, len(df)):
                window_data = df.iloc[i-self.window:i]
                positions = window_data['norm_pos'].values
                
                # Weight by volume if specified
                weights = None
                if self.volume_weighted and 'volume' in window_data.columns:
                    weights = window_data['volume'].values
                    if np.sum(weights) > 0:
                        weights = weights / np.sum(weights)
                    else:
                        weights = None
                
                # Fit distribution and calculate pressure
                try:
                    if self.use_multiple_dists:
                        result = self._fit_multiple_distributions(positions, weights)
                        if result:
                            buy_p, sell_p, pressure, sig = result
                        else:
                            buy_p, sell_p, pressure, sig = self._fit_beta_pressure(positions, weights)
                    else:
                        buy_p, sell_p, pressure, sig = self._fit_beta_pressure(positions, weights)
                        
                    buying_pressure.append(buy_p)
                    selling_pressure.append(sell_p)
                    market_pressure.append(pressure)
                    pressure_significance.append(sig)
                except:
                    # Default values if fitting fails
                    buying_pressure.append(np.nan)
                    selling_pressure.append(np.nan)
                    market_pressure.append(np.nan)
                    pressure_significance.append(np.nan)
        
            # Add calculated metrics to dataframe
            if len(buying_pressure) > 0:
                df.loc[df.index[self.window:], 'buying_pressure'] = buying_pressure
                df.loc[df.index[self.window:], 'selling_pressure'] = selling_pressure
                df.loc[df.index[self.window:], 'market_pressure'] = market_pressure
                df.loc[df.index[self.window:], 'pressure_significance'] = pressure_significance
        
        # Calculate trends for divergence detection
        df['price_trend'] = df['close'].pct_change(5).rolling(window=self.price_trend).mean().fillna(0)
        df['pressure_trend'] = df['market_pressure'].diff(5).rolling(window=self.pressure_trend).mean().fillna(0)
        
        # Detect divergences
        df['divergence'] = 0
        bull_div = (df['price_trend'] < (-1* self.bull_div_threshold)) & (df['pressure_trend'] > self.bull_div_threshold) & (df['pressure_significance'] > self.confidence_threshold * 0.8)
        bear_div = (df['price_trend'] > self.bear_div_threshold) & (df['pressure_trend'] < (-1*self.bear_div_threshold)) & (df['pressure_significance'] > self.confidence_threshold * 0.8)
        df.loc[bull_div, 'divergence'] = 1
        df.loc[bear_div, 'divergence'] = -1
        
        # Generate trading signals based on pressure and divergences
        df['signal'] = 0
        buy_conditions = (
            (df['market_pressure'] > self.pressure_threshold) & 
            (df['pressure_significance'] > self.confidence_threshold) &
            (df['market_pressure'].shift(1) <= self.pressure_threshold)
        ) | (
            (df['divergence'] == 1) & 
            (df['pressure_significance'] > self.confidence_threshold * 0.9)
        )
        
        sell_conditions = (
            (df['market_pressure'] < -self.pressure_threshold) & 
            (df['pressure_significance'] > self.confidence_threshold) &
            (df['market_pressure'].shift(1) >= -self.pressure_threshold)
        ) | (
            (df['divergence'] == -1) & 
            (df['pressure_significance'] > self.confidence_threshold * 0.9)
        )
        
        # Set signals
        df.loc[buy_conditions, 'signal'] = 1
        df.loc[sell_conditions, 'signal'] = -1
        
        # If long_only, convert sell signals to exit signals
        if self.long_only:
            df.loc[df['signal'] == -1, 'signal'] = 0
        
        # Set signal strength based on pressure and significance
        df['signal_strength'] = 0.0
        df.loc[buy_conditions | sell_conditions, 'signal_strength'] = (
            df.loc[buy_conditions | sell_conditions, 'pressure_significance'] * 
            df.loc[buy_conditions | sell_conditions, 'market_pressure'].abs()
        )
        
        # Return the needed columns
        return df[['open', 'close', 'high', 'low', 'norm_pos', 'market_pressure', 
                   'pressure_significance', 'divergence', 'signal', 'signal_strength']]

    def _fit_beta_pressure(self, positions, weights=None):
        """
        Fit a Beta distribution to positions and calculate pressure metrics.
        The significance test is now against a Uniform[0,1] distribution
        to determine if the observed positions are non-random.

        Args:
            positions (np.array): Normalized positions in [0,1] range.
            weights (np.array, optional): Weights for each position.

        Returns:
            tuple: (buying_pressure, selling_pressure, market_pressure, significance)
        """
        # Clip values to ensure they're in the (0,1) range for beta distribution
        # and also for the KS test against 'uniform' which expects values in [0,1]
        # Using a slightly wider clip for KS test against uniform, as it can handle 0 and 1.
        beta_positions = np.clip(positions, 1e-6, 1 - 1e-6) # For beta fitting
        ks_positions = np.clip(positions, 0, 1) # For KS test against uniform

        # Default equal weights if none provided
        if weights is None or len(positions) == 0: # Added check for empty positions
            if len(positions) == 0:
                # If no positions, cannot fit, return neutral and no significance
                return 0.5, 0.5, 0.0, 0.0
            weights = np.ones_like(beta_positions) / len(beta_positions)
        elif np.sum(weights) == 0: # Handle case where sum of weights is zero
            weights = np.ones_like(beta_positions) / len(beta_positions)


        # Method of moments with weights for Beta distribution fitting
        # Using beta_positions for fitting the Beta distribution
        mean = np.sum(weights * beta_positions)
        var = np.sum(weights * (beta_positions - mean)**2)

        # Safeguard against zero variance
        var = max(var, 1e-9)

        # Compute alpha and beta parameters for the Beta distribution
        # (mean * (1-mean) / var) must be > 1 for alpha and beta to be positive
        # Add a small epsilon if it's too close to 1 or less.
        factor = (mean * (1 - mean) / var)
        if factor <= 1:
            # This can happen with very low variance or mean near 0 or 1
            # Fallback to a weakly informative prior or simpler pressure calc
            # For now, let's calculate pressure based on mean if Beta params are tricky
            simple_buying_pressure = mean # If mean is high, buying pressure is high
            simple_selling_pressure = 1 - mean
            simple_market_pressure = simple_buying_pressure - simple_selling_pressure
            
            # Significance calculation can still proceed with ks_positions
            try:
                # Test if ks_positions deviate significantly from a Uniform[0,1] distribution
                # A Uniform[0,1] distribution is equivalent to a Beta(1,1) distribution
                # For scipy.stats.kstest, the 'uniform' distribution is defined on [0,1] by default
                _stat, p_value = kstest(ks_positions, 'uniform')
                significance = 1 - p_value  # High significance if p_value is low (i.e., not uniform)
            except Exception:
                # Fallback significance based on standard deviation
                # 0.288675 is approx. sqrt(1/12), the std of Uniform[0,1]
                if len(ks_positions) > 1:
                    std_dev_positions = np.std(ks_positions)
                    significance = 1 - np.min([1.0, std_dev_positions / 0.288675])
                else:
                    significance = 0.0 # Not enough data for std dev
            return simple_buying_pressure, simple_selling_pressure, simple_market_pressure, significance

        alpha = mean * (factor - 1)
        beta_param = (1 - mean) * (factor - 1)

        # Ensure parameters are positive
        alpha = max(alpha, 0.01)
        beta_param = max(beta_param, 0.01)

        # Calculate pressure metrics using the fitted Beta distribution
        buying_pressure = 1 - beta.cdf(0.5, alpha, beta_param)
        selling_pressure = beta.cdf(0.5, alpha, beta_param)
        market_pressure = buying_pressure - selling_pressure

        # Calculate statistical significance by testing against a Uniform distribution
        try:
            # Test if ks_positions deviate significantly from a Uniform[0,1] distribution
            # A Uniform[0,1] distribution is equivalent to a Beta(1,1) distribution
            # For scipy.stats.kstest, the 'uniform' distribution is defined on [0,1] by default
            _stat, p_value = kstest(ks_positions, 'uniform')
            significance = 1 - p_value  # High significance if p_value is low (i.e., not uniform)
        except Exception: # Catch more generic exceptions from kstest
            # Fallback significance based on standard deviation
            # 0.288675 is approx. sqrt(1/12), the std of Uniform[0,1]
            if len(ks_positions) > 1:
                std_dev_positions = np.std(ks_positions)
                # Ensure std_dev_positions is not zero to avoid division by zero if 0.288675 is also zero (highly unlikely)
                # or if std_dev_positions is extremely small leading to large ratio
                significance = 1 - np.min([1.0, std_dev_positions / 0.288675 if 0.288675 > 1e-9 else 1.0])
            else:
                significance = 0.0 # Not enough data for std dev

        return buying_pressure, selling_pressure, market_pressure, significance
    
    def _fit_multiple_distributions(self, positions, weights=None):
        """
        Fit multiple distributions and select the best one based on fit quality.
        
        This method tries both Beta distribution and transformed Normal distribution,
        then selects the one that provides the better statistical fit to the data.
        
        Args:
            positions (np.array): Normalized positions in [0,1] range.
            weights (np.array, optional): Weights for each position.
            
        Returns:
            tuple: (buying_pressure, selling_pressure, market_pressure, significance)
                  or None if fitting fails
        """
        try:
            # Fit Beta distribution
            beta_result = self._fit_beta_pressure(positions, weights)
            
            # Fit transformed Normal distribution (logit transformation)
            transformed = -np.log(1/np.clip(positions, 1e-6, 1-1e-6) - 1)
            
            # Default equal weights if none provided
            if weights is None:
                weights = np.ones_like(positions) / len(positions)
            
            # Fit normal to transformed data
            mean = np.sum(weights * transformed)
            var = np.sum(weights * (transformed - mean)**2)
            std = np.sqrt(max(var, 1e-9))
            
            # Calculate pressure in transformed space
            # 0.5 in [0,1] space maps to 0 in transformed space
            norm_buying_pressure = 1 - norm.cdf(0, mean, std)
            norm_selling_pressure = norm.cdf(0, mean, std)
            norm_market_pressure = norm_buying_pressure - norm_selling_pressure
            
            # Calculate significance
            try:
                ks_stat, p_value = kstest(transformed, 'norm', args=(mean, std))
                norm_significance = 1 - p_value
            except:
                norm_significance = 0.5
            
            # Select the distribution with better fit (higher significance)
            if norm_significance > beta_result[3]:
                return norm_buying_pressure, norm_selling_pressure, norm_market_pressure, norm_significance
            else:
                return beta_result
        except:
            return None