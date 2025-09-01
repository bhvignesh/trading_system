# trading_system/src/optimizer/ticker_level/performance_evaluator.py

"""
Performance metrics calculation module for trading strategy evaluation.

This module provides a comprehensive set of performance metrics for evaluating
trading strategies, including return metrics, risk-adjusted ratios, drawdown
statistics, and volatility measures. It supports both single-asset and
multi-asset portfolio evaluations.
"""

import logging
from typing import Dict, List, Optional, Tuple, Union

import mlflow
import numpy as np
import pandas as pd

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class PerformanceEvaluator:
    """
    Calculates performance metrics for trading strategies based on position data.
    
    This class provides static methods to calculate key trading performance metrics
    including returns, Sharpe ratio, Sortino ratio, drawdowns, and other risk metrics.
    It supports both single-asset analysis and multi-ticker portfolio analysis with
    customizable asset weights.
    """
    
    @staticmethod
    def compute_metrics(df: pd.DataFrame, annualization_factor: int = 252) -> Dict[str, float]:
        """
        Calculate comprehensive performance metrics for a trading strategy.
        
        Args:
            df (pd.DataFrame): DataFrame containing trading signals and pricing data,
                must include 'close' and 'position' columns.
            annualization_factor (int): Factor used to annualize returns and volatility
                (default 252 for daily trading).
            
        Returns:
            Dict[str, float]: Dictionary of computed performance metrics including:
                - cumulative_return: Total return over the period
                - annualized_return: Return annualized to a yearly equivalent
                - sharpe_ratio: Risk-adjusted return measure
                - sortino_ratio: Downside risk-adjusted return measure
                - calmar_ratio: Return relative to maximum drawdown
                - omega_ratio: Probability-weighted ratio of gains vs. losses
                - win_rate: Proportion of positive return periods
                - recovery_factor: Return relative to maximum drawdown
                - max_drawdown: Maximum peak-to-trough decline
                - drawdown_duration: Longest consecutive period in drawdown
                - annualized_volatility: Annualized standard deviation of returns
                - ulcer_index: Measure of drawdown severity
                - harmonic_mean: Harmonic mean of selected positive metrics
        """
        # Validate input data
        if df.empty or 'position' not in df.columns or 'close' not in df.columns:
            logger.warning("Invalid input data for performance calculation")
            return {k: 0.0 for k in PerformanceEvaluator._get_metric_names()}
        
        # Create a working copy to avoid modifying the original
        df = df.copy()
        
        # Calculate returns
        df['daily_return'] = df['close'].pct_change().fillna(0)
        df['strategy_return'] = df['daily_return'] * df['position'].shift(1).fillna(0)
        
        # Compute cumulative and annualized returns
        strategy_returns = df['strategy_return']
        cum_return = (1 + strategy_returns).cumprod().iloc[-1] - 1
        n_periods = len(df)
        ann_return = ((1 + cum_return) ** (annualization_factor / n_periods)) - 1 if n_periods > 0 else 0

        # Calculate risk metrics
        mean_ret = strategy_returns.mean()
        std_ret = strategy_returns.std()
        
        # Sharpe Ratio (assuming 0 risk-free rate for simplicity)
        sharpe = (mean_ret / std_ret * np.sqrt(annualization_factor)) if std_ret > 0 else 0
        
        # Sortino Ratio (focusing on downside risk)
        negative_returns = df.loc[df['strategy_return'] < 0, 'strategy_return']
        downside_std = negative_returns.std() if not negative_returns.empty else 1e-6
        sortino = (mean_ret / downside_std * np.sqrt(annualization_factor)) if downside_std > 0 else 0

        # Drawdown analysis using vectorized operations
        cum_returns = (1 + df['strategy_return']).cumprod()
        rolling_max = cum_returns.cummax()
        drawdowns = (cum_returns / rolling_max - 1)
        max_drawdown = drawdowns.min()
        
        # Calculate drawdown duration using efficient vectorized approach
        in_drawdown = (cum_returns < rolling_max).astype(int)
        # Use run-length encoding approach for detecting longest drawdown period
        # Convert drawdown series to numpy array for faster processing
        drawdown_array = in_drawdown.to_numpy()
        # Identify changes in drawdown state
        change_points = np.diff(np.concatenate(([0], drawdown_array, [0])))
        # Find start and end points of drawdown periods
        starts = np.where(change_points == 1)[0]
        ends = np.where(change_points == -1)[0]
        if len(starts) > 0 and len(ends) > 0:
            # Calculate lengths of all drawdown periods
            durations = ends - starts
            drawdown_duration = int(durations.max())
        else:
            drawdown_duration = 0

        # Calmar Ratio
        calmar = (ann_return / abs(max_drawdown)) if max_drawdown < 0 else 0
        
        # Omega Ratio
        gain = df.loc[df['strategy_return'] > 0, 'strategy_return'].sum()
        loss = abs(df.loc[df['strategy_return'] < 0, 'strategy_return'].sum())
        omega = (gain / loss) if loss > 0 else 10  # Cap at 10 to avoid infinity
        
        # Win Rate
        win_rate = (df['strategy_return'] > 0).mean()
        
        # Recovery Factor
        recovery_factor = (cum_return / abs(max_drawdown)) if max_drawdown < 0 else 0
        
        # Annualized Volatility
        ann_vol = std_ret * np.sqrt(annualization_factor)
        
        # Ulcer Index - measure of drawdown severity
        ulcer_index = np.sqrt((drawdowns ** 2).mean())
        
        # Compile metrics dictionary
        metrics = {
            'cumulative_return': cum_return,
            'annualized_return': ann_return,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'calmar_ratio': calmar,
            'omega_ratio': omega,
            'win_rate': win_rate,
            'recovery_factor': recovery_factor,
            'max_drawdown': max_drawdown,
            'drawdown_duration': drawdown_duration,
            'annualized_volatility': ann_vol,
            'ulcer_index': ulcer_index
        }
        
        # Calculate harmonic mean of key performance metrics
        metrics['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(metrics)
        
        # Log metrics to MLflow if a run is active
        if mlflow.active_run():
            mlflow.log_metrics({f"pe_{k}": v for k, v in metrics.items() if not np.isnan(v)})
            
        return metrics
    
    @staticmethod
    def compute_multi_ticker_metrics(
        signals_dict: Dict[str, pd.DataFrame],
        weights: Optional[Dict[str, float]] = None,
        annualization_factor: int = 252
    ) -> Tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
        """
        Compute performance metrics for multiple tickers and their weighted average.
        
        Args:
            signals_dict (Dict[str, pd.DataFrame]): Dictionary mapping ticker symbols 
                to their signal DataFrames.
            weights (Optional[Dict[str, float]]): Dictionary mapping ticker symbols to 
                their weights. If None, equal weights are used.
            annualization_factor (int): Factor used to annualize performance.
            
        Returns:
            Tuple containing:
                Dict[str, Dict[str, float]]: Dictionary mapping ticker symbols to 
                    their performance metrics.
                Dict[str, float]: Dictionary containing the weighted average metrics.
        """
        if not signals_dict:
            return {}, {}
            
        # Compute metrics for each ticker individually
        ticker_metrics = {}
        for ticker, df in signals_dict.items():
            if df.empty:
                continue
            ticker_metrics[ticker] = PerformanceEvaluator.compute_metrics(df, annualization_factor)
            
        if not ticker_metrics:
            return {}, {}
            
        # Handle ticker weights
        if weights is None:
            # Equal weights if not provided
            ticker_weights = {ticker: 1.0 / len(ticker_metrics) for ticker in ticker_metrics}
        else:
            # Filter and normalize weights to include only tickers with metrics
            ticker_weights = {ticker: weights.get(ticker, 0) for ticker in ticker_metrics}
            total_weight = sum(ticker_weights.values())
            if abs(total_weight) > 1e-10:  # Avoid division by near-zero
                ticker_weights = {ticker: weight / total_weight for ticker, weight in ticker_weights.items()}
            else:
                # Fallback to equal weights
                ticker_weights = {ticker: 1.0 / len(ticker_metrics) for ticker in ticker_metrics}
                
        # Calculate weighted average metrics
        metric_keys = list(ticker_metrics[next(iter(ticker_metrics))].keys())
        weighted_metrics = {}
        
        for key in metric_keys:
            if key == 'harmonic_mean':
                continue  # Skip harmonic mean for now
                
            # Calculate weighted average for the current metric
            weighted_value = sum(
                ticker_metrics[ticker][key] * ticker_weights.get(ticker, 0)
                for ticker in ticker_metrics
            )
            weighted_metrics[key] = weighted_value
        
        # Compute harmonic mean for the weighted metrics
        weighted_metrics['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(weighted_metrics)
        
        # Log weighted metrics to MLflow if a run is active
        if mlflow.active_run():
            mlflow.log_metrics({f"weighted_{k}": v for k, v in weighted_metrics.items() if not np.isnan(v)})
            
        return ticker_metrics, weighted_metrics
    
    @staticmethod
    def harmonic_mean(metrics: Dict[str, float]) -> float:
        """
        Compute the harmonic mean of selected performance metrics.
        
        This function calculates the harmonic mean of key performance ratios to provide
        a balanced assessment of strategy performance. Only positive metrics are included
        in the calculation.
        
        Args:
            metrics (Dict[str, float]): Dictionary of performance metrics.
        
        Returns:
            float: The harmonic mean of the selected metrics, or 0 if no valid metrics.
        """
        # Key metrics to include in the harmonic mean
        required_keys = [
            'sharpe_ratio', 
            'sortino_ratio', 
            'calmar_ratio', 
            'omega_ratio', 
            'win_rate', 
            'recovery_factor'
        ]
        
        # Collect valid (positive) values
        values = []
        for key in required_keys:
            v = metrics.get(key)
            # Only include metrics that are positive
            if v is not None and v > 0:
                values.append(v)
        
        # Return harmonic mean if we have values, otherwise 0
        if not values:
            return 0
        return len(values) / np.sum([1.0 / v for v in values])
    
    @staticmethod
    def _get_metric_names() -> List[str]:
        """
        Return the standard list of metrics computed by this evaluator.
        
        Returns:
            List[str]: Names of all metrics calculated by the performance evaluator.
        """
        return [
            'cumulative_return', 'annualized_return', 'sharpe_ratio',
            'sortino_ratio', 'calmar_ratio', 'omega_ratio', 'win_rate',
            'recovery_factor', 'max_drawdown', 'drawdown_duration', 
            'annualized_volatility', 'ulcer_index', 'harmonic_mean'
        ]