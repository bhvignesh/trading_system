# trading_system/src/strategies/pairs_trading.py

"""
Module: pairs_trading
---------------------
This module implements a pairs trading strategy with integrated risk management. The 
strategy identifies pairs of statistically cointegrated securities and performs dynamic 
hedge ratio estimation, robust spread normalization, and adaptive signal generation. The 
signals are further adjusted using risk management procedures that account for stop-loss, 
take-profit, slippage, and transaction costs.

Key features:
    - Cointegration testing via the Engle-Granger method or, optionally, the Johansen test.
    - Dynamic hedge ratio estimation using rolling ordinary least squares (OLS).
    - Robust z-score calculation for the spread based on a rolling median and median absolute deviation (MAD).
    - Adaptive entry thresholds scaled by the volatility of the spread.
    - Structural break detection using the CUSUM-OLS test and segmentation-based stationarity verification.
    - Integration with a RiskManager to apply practical risk management on generated signals.
"""

import logging
from itertools import combinations
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import breaks_cusumolsb
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.risk_management import RiskManager


class PairsTradingStrategy(BaseStrategy):
    """
    A pairs trading strategy for identifying and trading cointegrated securities with integrated risk management.

    This class uses historical price data to filter tradable pairs based on cointegration tests. 
    For candidate pairs, it dynamically calculates hedge ratios, computes the spread and its robust
    z-score, and generates trading signals with adaptive thresholds. Risk management rules are then
    applied to the signals before outputting the final trading decisions.

    Attributes:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict): Dictionary with strategy parameters. Default and configurable parameters include:
            - 'lookback_window': Period for calculating rolling statistics (default: 60)
            - 'entry_threshold': Baseline z-score threshold for triggering positions (default: 2.0)
            - 'exit_threshold': Z-score threshold for signaling position exit (default: 0.5)
            - 'correlation_threshold': Minimum required correlation for pair filtering (default: 0.7)
            - 'halflife_threshold': Maximum acceptable half-life for mean reversion (default: 30)
            - 'pvalue_threshold': Significance threshold for the Engle-Granger cointegration test (default: 0.05)
            - 'min_data_points': Minimum necessary data points for analysis (default: 252)
            - 'use_johansen': Flag to enable the Johansen test for cointegration (default: False)
            - Risk parameters: 'stop_loss_pct', 'take_profit_pct', 'trailing_stop_pct','slippage_pct', and 'transaction_cost_pct'
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict[str, Any]] = None):
        # Initialize default strategy parameters, and update with any provided parameters.
        default_params = {
            'lookback_window': 60,
            'entry_threshold': 2.0,
            'exit_threshold': 0.5,
            'correlation_threshold': 0.7,
            'halflife_threshold': 30,
            'pvalue_threshold': 0.05,
            'min_data_points': 252,
            'use_johansen': False,  # Set to True to enable Johansen test for cointegration.
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001
        }
        if params:
            default_params.update(params)
        super().__init__(db_config, default_params)

        # Instantiate the RiskManager with provided risk parameters.
        self.risk_manager = RiskManager(
            stop_loss_pct=self.params['stop_loss_pct'],
            take_profit_pct=self.params['take_profit_pct'],
            trailing_stop_pct=self.params['trailing_stop_pct'],
            slippage_pct=self.params['slippage_pct'],
            transaction_cost_pct=self.params['transaction_cost_pct']
        )

    def find_cointegrated_pairs(self, price_data: pd.DataFrame, tickers: List[str]) -> List[Tuple[str, str, float, float, float]]:
        """
        Identify cointegrated pairs from the provided list of tickers using historical price data.

        The function reshapes the input data if necessary, filters tickers based on data availability, 
        and applies a preliminary correlation screening. For each candidate pair, a cointegration test 
        (Johansen or Engle-Granger) is conducted. If the candidate passes the cointegration test and 
        the estimated mean reversion half-life is within limits, the pair is considered valid.

        Args:
            price_data (pd.DataFrame): DataFrame containing historical price data with at least a 'close' column.
            tickers (List[str]): List of ticker symbols to be analyzed.

        Returns:
            List[Tuple[str, str, float, float, float]]:
                A list of tuples containing:
                    - Ticker 1 (str)
                    - Ticker 2 (str)
                    - Calculated hedge ratio (float)
                    - p-value (or placeholder using Johansen test) (float)
                    - Half-life of mean reversion (float)
        """
        t0 = perf_counter()
        cointegrated_pairs = []

        # If the DataFrame uses a MultiIndex, pivot it so each column corresponds to a ticker's close prices.
        if isinstance(price_data.index, pd.MultiIndex):
            close_prices = price_data['close'].unstack(level=0)
        else:
            close_prices = price_data['close']

        # Filter out tickers that are not available in the data.
        available_tickers = [ticker for ticker in tickers if ticker in close_prices.columns]
        if len(available_tickers) < len(tickers):
            missing = set(tickers) - set(available_tickers)
            self.logger.warning(f"Missing price data for tickers: {missing}")
        close_prices = close_prices[available_tickers].fillna(method='ffill')

        # Ensure the data meets the minimum data point requirement.
        min_points = self.params['min_data_points']
        if len(close_prices) < min_points:
            self.logger.warning(f"Insufficient data points: {len(close_prices)} < {min_points}")
            return []

        # Calculate the correlation matrix to prefilter candidate pairs.
        correlation_matrix = close_prices.corr()

        pairs_tested = 0
        for ticker1, ticker2 in combinations(available_tickers, 2):
            if correlation_matrix.loc[ticker1, ticker2] < self.params['correlation_threshold']:
                continue
            pairs_tested += 1

            stock1 = close_prices[ticker1].dropna()
            stock2 = close_prices[ticker2].dropna()

            # Align the time series indices for the pair.
            common_idx = stock1.index.intersection(stock2.index)
            if len(common_idx) < min_points:
                continue

            stock1 = stock1.loc[common_idx]
            stock2 = stock2.loc[common_idx]

            # --- Perform Cointegration Test ---
            if self.params.get('use_johansen', False):
                # Johansen test requires a 2D array; combine the time series.
                combined = pd.concat([stock1, stock2], axis=1).dropna()
                if len(combined) < self.params['min_data_points']:
                    continue
                johansen_result = coint_johansen(combined.values, det_order=0, k_ar_diff=1)
                # Evaluate cointegration using the test statistic compared with its 5% critical value.
                critical_value = johansen_result.cvt[0, 1]
                if johansen_result.lr1[0] <= critical_value:
                    continue
                # Use a placeholder p-value for Johansen test.
                pvalue = 0.01
            else:
                result = coint(stock1, stock2)
                pvalue = result[1]
                if pvalue >= self.params['pvalue_threshold']:
                    continue

            # Use ordinary least squares (OLS) to estimate a static hedge ratio.
            X = sm.add_constant(stock2)
            model = sm.OLS(stock1, X).fit()
            hedge_ratio = model.params[1]

            # Calculate the spread between the tickers and compute its half-life.
            spread = stock1 - hedge_ratio * stock2
            half_life = self._calculate_half_life(spread)
            if 0 < half_life < self.params['halflife_threshold']:
                cointegrated_pairs.append((ticker1, ticker2, hedge_ratio, pvalue, half_life))

        # Sort pairs by cointegration significance (lower p-value indicates stronger cointegration).
        cointegrated_pairs.sort(key=lambda x: x[3])
        t1 = perf_counter()
        self.logger.info(f"Tested {pairs_tested} pairs, found {len(cointegrated_pairs)} cointegrated pairs in {t1-t0:.2f} seconds")
        return cointegrated_pairs

    def _calculate_half_life(self, spread: pd.Series) -> float:
        """
        Estimate the half-life of mean reversion for a given spread series.

        This function applies an OLS regression on the spread's lagged differences to estimate the 
        speed of mean reversion. The half-life is calculated as the time required to reduce the deviation 
        by half using the formula: half-life = -ln(2)/γ. In case of insufficient data or non-mean-reverting 
        behavior, the function returns infinity.

        Args:
            spread (pd.Series): Time series representing the spread between two securities.
            
        Returns:
            float: Estimated half-life (in days) or infinity if the estimation is not applicable.
        """
        try:
            spread = spread.dropna()
            if len(spread) < 20:
                return float('inf')
            spread_lag = spread.shift(1)
            spread_ret = spread - spread_lag
            spread_ret = spread_ret.dropna()
            spread_lag = spread_lag.dropna()
            model = sm.OLS(spread_ret, sm.add_constant(spread_lag)).fit()
            gamma = model.params[1]
            if gamma >= 0:
                return float('inf')
            half_life = -np.log(2) / gamma
            return half_life
        except Exception as e:
            self.logger.error(f"Error calculating half-life: {str(e)}")
            return float('inf')

    def _check_structural_break(self, spread: pd.Series) -> bool:
        """
        Check for the presence of a structural break in the spread series.

        This method applies the CUSUM-OLS test to detect abrupt shifts in the spread's underlying structure.
        A p-value less than 0.05 indicates a statistically significant structural break.

        Args:
            spread (pd.Series): The spread time series.
            
        Returns:
            bool: True if a structural break is detected; otherwise, False.
        """
        test_result = breaks_cusumolsb(spread)
        if test_result[1] < 0.05:
            self.logger.warning("Structural break detected in spread")
            return True
        return False

    def walk_forward_test(self, spread: pd.Series) -> bool:
        """
        Perform a walk-forward stationarity test on the spread.

        The spread is divided into two segments: an in-sample (70%) and out-of-sample (30%) period. 
        The Augmented Dickey-Fuller test is applied to both segments. The spread is considered stationary 
        if the p-value in both tests is below 0.05.

        Args:
            spread (pd.Series): The spread series.
            
        Returns:
            bool: True if both data segments are statistically stationary; otherwise, False.
        """
        train_size = int(len(spread) * 0.7)
        train, test = spread.iloc[:train_size], spread.iloc[train_size:]
        adf_train = adfuller(train)[1]
        adf_test = adfuller(test)[1]
        return (adf_train < 0.05) and (adf_test < 0.05)

    def generate_pair_signals(
        self,
        ticker1: str,
        ticker2: str,
        price_data: pd.DataFrame,
        hedge_ratio: Optional[float] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate trading signals for a specified pair of tickers.

        The method performs the following steps:
            1. Restructures and filters the price data for the pair.
            2. Dynamically estimates the hedge ratio using rolling OLS (unless a static hedge ratio is provided).
            3. Calculates the spread between the securities.
            4. Computes a robust z-score of the spread via rolling median and MAD.
            5. Determines an adaptive entry threshold based on the spread volatility.
            6. Checks for structural breaks and validates stationarity using a walk-forward test.
            7. Generates entry and exit signals along with signal strength metrics.
            8. Integrates risk management adjustments via the RiskManager.
        
        Args:
            ticker1 (str): The ticker symbol of the first security.
            ticker2 (str): The ticker symbol of the second security.
            price_data (pd.DataFrame): Historical price data for both tickers.
            hedge_ratio (float, optional): Pre-calculated hedge ratio; if not provided, it is dynamically estimated.
            start_date (str, optional): Lower bound for filtering price data (inclusive).
            end_date (str, optional): Upper bound for filtering price data (inclusive).
            initial_position (int): The starting position (e.g., 0 for no position, 1 for long, -1 for short).
            latest_only (bool): If True, returns only the most recent generated trading signal.
            
        Returns:
            pd.DataFrame: DataFrame containing the following columns:
                - open, high, low, close: Price data for ticker1.
                - spread: Calculated spread between ticker1 and ticker2.
                - robust_z: Robust z-score of the spread.
                - signal: Generated trading signal (1 for long, -1 for short, 0 for exit).
                - signal_strength: Normalized measure of signal significance.
                - position: Final position after applying risk management.
                - return: Trading return for the period.
                - cumulative_return: Cumulative return from the trading signals.
                - exit_type: Indicator for the type of exit triggered.
                - ticker1, ticker2: Identifiers for the traded pair.
                - hedge_ratio: Hedge ratio used for spread calculation.
        """
        t0 = perf_counter()

        # Restructure the price data for the two tickers.
        if isinstance(price_data.index, pd.MultiIndex):
            df1 = price_data.loc[ticker1].copy()
            df2 = price_data.loc[ticker2].copy()
            # Align the data to have common dates.
            common_dates = df1.index.intersection(df2.index)
            df1 = df1.loc[common_dates]
            df2 = df2.loc[common_dates]
            merged_data = pd.DataFrame({
                f'{ticker1}_open': df1['open'],
                f'{ticker1}_high': df1['high'],
                f'{ticker1}_low': df1['low'],
                f'{ticker1}_close': df1['close'],
                f'{ticker2}_open': df2['open'],
                f'{ticker2}_high': df2['high'],
                f'{ticker2}_low': df2['low'],
                f'{ticker2}_close': df2['close']
            }, index=common_dates)
        else:
            merged_data = price_data

        # Filter data by the specified start and end dates.
        if start_date:
            merged_data = merged_data[merged_data.index >= start_date]
        if end_date:
            merged_data = merged_data[merged_data.index <= end_date]
        if merged_data.empty or len(merged_data) < self.params['lookback_window']:
            self.logger.warning(f"Insufficient data for pair {ticker1}-{ticker2}")
            return pd.DataFrame()

        lookback_window = self.params['lookback_window']

        # --- Dynamic Hedge Ratio Estimation via Rolling OLS ---
        # Calculate the dynamic hedge ratio as the rolling covariance of ticker1 and ticker2 divided by the variance of ticker2.
        merged_data['hedge_ratio'] = (
            merged_data[f'{ticker1}_close']
            .rolling(window=lookback_window)
            .cov(merged_data[f'{ticker2}_close'])
            / merged_data[f'{ticker2}_close'].rolling(window=lookback_window).var()
        )
        merged_data['hedge_ratio'].fillna(method='bfill', inplace=True)
        # Use the provided hedge_ratio if available; otherwise, use the most recent dynamic estimate.
        if hedge_ratio is None:
            hedge_ratio = merged_data['hedge_ratio'].iloc[-1]

        # --- Spread Calculation ---
        # Compute the spread between the two tickers using the current hedge ratio.
        merged_data['spread'] = merged_data[f'{ticker1}_close'] - merged_data['hedge_ratio'] * merged_data[f'{ticker2}_close']

        # --- Robust Spread Normalization ---
        # Calculate the rolling median and median absolute deviation (MAD) of the spread, then compute a robust z-score.
        merged_data['spread_median'] = merged_data['spread'].rolling(window=lookback_window).median()
        merged_data['MAD'] = (merged_data['spread'] - merged_data['spread_median']).abs().rolling(window=lookback_window).median()
        # Scale factor 0.6745 makes the MAD consistent with the standard deviation for normally distributed data.
        merged_data['robust_z'] = 0.6745 * (merged_data['spread'] - merged_data['spread_median']) / merged_data['MAD']

        # --- Structural Break and Stationarity Check ---
        # Abort signal generation if a structural break is detected or if the walk-forward test fails.
        if self._check_structural_break(merged_data['spread']):
            self.logger.warning("Structural break detected in spread. Aborting signal generation.")
            return pd.DataFrame()
        if not self.walk_forward_test(merged_data['spread']):
            self.logger.warning("Walk-forward test failed: Spread is not stationary in both sample segments.")
            return pd.DataFrame()

        # --- Adaptive Entry Threshold ---
        # Calculate the volatility of the spread and adjust the entry threshold dynamically.
        volatility = merged_data['spread'].pct_change().rolling(window=21).std()
        dynamic_entry = self.params['entry_threshold'] * (volatility / volatility.median())

        # --- Signal Generation ---
        # Generate trading signals based on the robust z-score: long when below negative threshold, short when above threshold.
        merged_data['signal'] = 0
        merged_data.loc[merged_data['robust_z'] < -dynamic_entry, 'signal'] = 1   # Signal to go long on the spread.
        merged_data.loc[merged_data['robust_z'] > dynamic_entry, 'signal'] = -1   # Signal to go short on the spread.
        # Determine exit signals when the z-score reverts past the exit threshold.
        long_exit = (merged_data['robust_z'] > -self.params['exit_threshold']) & (merged_data['robust_z'].shift(1) <= -self.params['exit_threshold'])
        short_exit = (merged_data['robust_z'] < self.params['exit_threshold']) & (merged_data['robust_z'].shift(1) >= self.params['exit_threshold'])
        merged_data.loc[long_exit | short_exit, 'signal'] = 0

        # --- Signal Strength Calculation ---
        # Compute the signal strength by normalizing the robust z-score with its rolling standard deviation.
        z_std = merged_data['robust_z'].rolling(window=lookback_window).std().fillna(1)
        merged_data['signal_strength'] = merged_data['robust_z'].abs() / (z_std + 1e-6)

        # --- Apply Risk Management ---
        # Prepare a DataFrame with the required price and signal data for the RiskManager.
        risk_data = pd.DataFrame({
            'open': merged_data[f'{ticker1}_open'],
            'high': merged_data[f'{ticker1}_high'],
            'low': merged_data[f'{ticker1}_low'],
            'close': merged_data[f'{ticker1}_close'],
            'signal': merged_data['signal']
        })
        risk_managed = self.risk_manager.apply(risk_data, initial_position)
        result = pd.DataFrame({
            'open': merged_data[f'{ticker1}_open'],
            'high': merged_data[f'{ticker1}_high'],
            'low': merged_data[f'{ticker1}_low'],
            'close': merged_data[f'{ticker1}_close'],
            'spread': merged_data['spread'],
            'robust_z': merged_data['robust_z'],
            'signal': merged_data['signal'],
            'signal_strength': merged_data['signal_strength'],
            'position': risk_managed['position'],
            'return': risk_managed['return'],
            'cumulative_return': risk_managed['cumulative_return'],
            'exit_type': risk_managed['exit_type']
        })
        result['ticker1'] = ticker1
        result['ticker2'] = ticker2
        result['hedge_ratio'] = hedge_ratio

        if latest_only:
            result = result.iloc[-1:].copy()

        t1 = perf_counter()
        self.logger.debug(f"Generated pair signals in {t1-t0:.2f} seconds")
        return result

    def generate_signals(
        self, 
        tickers: Union[str, List[str], Tuple[str, str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Main method to generate trading signals using the pairs trading methodology.

        Depending on the format of the tickers input:
            - If a tuple with exactly two tickers is provided, signals will be generated for that specific pair.
            - If a list containing two or more tickers is provided, the method will first identify cointegrated 
              pairs among them and then generate signals for the top-ranked pair.
            - A single ticker (str) is not supported, as pairs trading requires at least two securities.

        Args:
            tickers (Union[str, List[str], Tuple[str, str]]): Ticker symbols to be considered.
            start_date (str, optional): Start date for historical data filtering ('YYYY-MM-DD').
            end_date (str, optional): End date for historical data filtering ('YYYY-MM-DD').
            initial_position (int): The starting trading position.
            latest_only (bool): If True, only the most recent trading signal is returned.
            
        Returns:
            pd.DataFrame: DataFrame containing the following information:
                - Open, high, low, close prices.
                - Calculated spread and its robust normalized z-score.
                - Generated trading signals and signal strength.
                - Final positions after risk management.
                - Trade returns and cumulative performance.
                - Exit type information and associated tickers with the hedge ratio.
        """
        t0 = perf_counter()
        if isinstance(tickers, tuple) and len(tickers) == 2:
            ticker1, ticker2 = tickers
            pairs_to_process = [(ticker1, ticker2, None)]
            search_for_pairs = False
        elif isinstance(tickers, list) and len(tickers) >= 2:
            self.logger.info(f"Finding cointegrated pairs among {len(tickers)} tickers")
            lookback = max(self.params['lookback_window'] * 3, self.params['min_data_points']) if latest_only else None
            price_data = self.get_historical_prices(tickers, lookback, from_date=start_date, to_date=end_date)
            cointegrated_pairs = self.find_cointegrated_pairs(price_data, tickers)
            if not cointegrated_pairs:
                self.logger.warning("No cointegrated pairs found")
                columns = ['open', 'high', 'low', 'close', 'spread', 'robust_z', 
                           'signal', 'signal_strength', 'position', 'return', 
                           'cumulative_return', 'exit_type', 'ticker1', 'ticker2', 'hedge_ratio']
                return pd.DataFrame(columns=columns)
            pairs_to_process = [(pair[0], pair[1], pair[2]) for pair in cointegrated_pairs[:1]]
            search_for_pairs = True
        elif isinstance(tickers, str):
            raise ValueError("Pairs trading requires at least two tickers")
        else:
            raise ValueError("Invalid ticker format")

        result_dfs = []
        for ticker1, ticker2, hedge_ratio in pairs_to_process:
            self.logger.info(f"Processing pair: {ticker1} - {ticker2}")
            if search_for_pairs:
                price_data_pair = price_data
            else:
                lookback = max(self.params['lookback_window'] * 3, self.params['min_data_points']) if latest_only else None
                price_data_pair = self.get_historical_prices([ticker1, ticker2], lookback, from_date=start_date, to_date=end_date)
            pair_result = self.generate_pair_signals(
                ticker1=ticker1,
                ticker2=ticker2,
                price_data=price_data_pair,
                hedge_ratio=hedge_ratio,
                start_date=start_date,
                end_date=end_date,
                initial_position=initial_position,
                latest_only=latest_only
            )
            if not pair_result.empty:
                result_dfs.append(pair_result)
        if not result_dfs:
            self.logger.warning("No valid results generated")
            columns = ['open', 'high', 'low', 'close', 'spread', 'robust_z', 
                       'signal', 'signal_strength', 'position', 'return', 
                       'cumulative_return', 'exit_type', 'ticker1', 'ticker2', 'hedge_ratio']
            return pd.DataFrame(columns=columns)
        final_result = pd.concat(result_dfs) if len(result_dfs) > 1 else result_dfs[0]
        t1 = perf_counter()
        self.logger.info(f"Signal generation completed in {t1-t0:.2f} seconds")
        return final_result