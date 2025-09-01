# trading_system/src/optimizer/strategy_optimizer.py

"""
Strategy optimization module with walk-forward cross-validation.

This module provides a framework for optimizing trading strategy parameters
using walk-forward cross-validation and hyperparameter search. It leverages
MLflow for experiment tracking and implements caching for efficient evaluation.
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool, cpu_count
from typing import Any, Dict, List, Optional, Tuple, Type

import mlflow
import numpy as np
import pandas as pd
from hyperopt import STATUS_OK, Trials, fmin, space_eval, tpe

from optimizer.ticker_level.performance_evaluator import PerformanceEvaluator

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class StrategyOptimizer:
    """
    Universal optimizer for trading strategies using walk-forward cross-validation.
    
    This class performs hyperparameter optimization for trading strategies using
    time-series cross-validation (walk-forward analysis) combined with hyperparameter
    search. It supports multiple tickers, custom weighting, and risk adjustments.
    Results are tracked using MLflow.
    """

    def __init__(
        self,
        strategy_class: Type,
        db_config: Any,
        search_space: Dict[str, Any],
        tickers: List[str],
        start_date: str,
        end_date: str,
        ticker_weights: Optional[Dict[str, float]] = None,
        initial_position: int = 0,
        cv_folds: int = 3,
        risk_thresholds: Optional[Dict[str, float]] = None,
        max_evals: int = 50,
        run_name: str = "StrategyOptimization",
        n_jobs: int = -1
    ):
        """
        Initialize the strategy optimizer.
        
        Args:
            strategy_class (Type): Trading strategy class (should implement required methods).
            db_config (Any): Database configuration object.
            search_space (Dict[str, Any]): Hyperparameter search space (compatible with Hyperopt).
            tickers (List[str]): List of ticker symbols to optimize for.
            start_date (str): Backtesting start date (format 'YYYY-MM-DD').
            end_date (str): Backtesting end date (format 'YYYY-MM-DD').
            ticker_weights (Optional[Dict[str, float]]): Weights for each ticker. If None, equal weights are used.
            initial_position (int): Starting trading position.
            cv_folds (int): Number of folds for walk-forward cross-validation.
            risk_thresholds (Optional[Dict[str, float]]): Dictionary of risk metric thresholds.
            max_evals (int): Maximum number of evaluations for hyperparameter search.
            run_name (str): Name for the MLflow run.
            n_jobs (int): Number of parallel jobs for fold evaluation. If -1, uses all CPU cores.
        """
        self.strategy_class = strategy_class
        self.db_config = db_config
        self.search_space = search_space
        self.tickers = tickers
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.run_name = run_name
        
        # Initialize ticker weights with normalization
        self.ticker_weights = self._normalize_weights(ticker_weights)
        
        self.initial_position = initial_position
        self.cv_folds = cv_folds
        self.max_evals = max_evals
        
        # Set default risk thresholds if not provided
        self.risk_thresholds = risk_thresholds or {
            'max_drawdown': -0.3,          # Maximum allowed drawdown (e.g., -30%)
            'drawdown_duration': 30,       # Maximum allowed consecutive days in drawdown
            'ulcer_index': 0.15,           # Maximum allowed ulcer index
            'annualized_volatility': 0.4   # Annualized volatility threshold
        }
        
        # Determine number of parallel jobs
        self.n_jobs = n_jobs if n_jobs > 0 else max(1, cpu_count())
        
        # Initialize evaluation cache for repeated parameter evaluations
        self._evaluation_cache = {}
        
        # Precompute fold date ranges for consistency
        self._fold_ranges = self._compute_fold_ranges()
        
        logger.info(f"Initialized StrategyOptimizer with {len(tickers)} tickers, "
                   f"{cv_folds} folds, and {max_evals} max evaluations")

    def _normalize_weights(self, weights: Optional[Dict[str, float]]) -> Dict[str, float]:
        """
        Normalize ticker weights to ensure they sum to 1.
        
        Args:
            weights (Optional[Dict[str, float]]): Raw ticker weights.
            
        Returns:
            Dict[str, float]: Normalized ticker weights.
        """
        if weights is None:
            # Equal weights if not provided
            return {ticker: 1.0 / len(self.tickers) for ticker in self.tickers}
        
        # Ensure all tickers have a weight, default to 0 for missing tickers
        normalized_weights = {ticker: weights.get(ticker, 0.0) for ticker in self.tickers}
        
        # Normalize to sum to 1
        total_weight = sum(normalized_weights.values())
        if abs(total_weight) > 1e-10:  # Avoid division by near-zero
            return {ticker: weight / total_weight for ticker, weight in normalized_weights.items()}
        else:
            # Fallback to equal weights if sum is too small
            return {ticker: 1.0 / len(self.tickers) for ticker in self.tickers}

    def _compute_fold_ranges(self) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """
        Compute date ranges for each cross-validation fold.
        
        Returns:
            List[Tuple[pd.Timestamp, pd.Timestamp]]: List of (start, end) date ranges for each fold.
        """
        total_days = (self.end_date - self.start_date).days
        fold_length = total_days // self.cv_folds
        
        fold_ranges = []
        for fold in range(self.cv_folds):
            fold_start = self.start_date + pd.Timedelta(days=fold * fold_length)
            fold_end = (self.start_date + pd.Timedelta(days=(fold + 1) * fold_length - 1) 
                        if fold < self.cv_folds - 1 else self.end_date)
            fold_ranges.append((fold_start, fold_end))
            
        return fold_ranges

    def _params_to_key(self, params: Dict[str, Any]) -> str:
        """
        Convert parameters to a cache key using stable serialization and hashing.
        
        Args:
            params (Dict[str, Any]): Strategy parameters.
            
        Returns:
            str: Cache key string.
        """
        # Sort keys for stable serialization
        serialized = json.dumps(params, sort_keys=True)
        return hashlib.md5(serialized.encode()).hexdigest()

    def _generate_signals_batch(
        self, 
        strategy_instance: Any, 
        fold_start: pd.Timestamp, 
        fold_end: pd.Timestamp
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate signals for all tickers in a single batch for efficiency.
        
        Args:
            strategy_instance (Any): Instance of the strategy class.
            fold_start (pd.Timestamp): Start date of the evaluation window.
            fold_end (pd.Timestamp): End date of the evaluation window.
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping ticker symbols to their signal DataFrames.
        """
        try:
            # Generate signals for all tickers in one call
            df_signals = strategy_instance.generate_signals(
                ticker=self.tickers,
                start_date=fold_start.strftime("%Y-%m-%d"),
                end_date=fold_end.strftime("%Y-%m-%d"),
                initial_position=self.initial_position,
                latest_only=False
            )
            
            # Process the returned DataFrame based on its structure
            signals_dict = {}
            
            # Handle multi-index DataFrame (grouped by ticker)
            if isinstance(df_signals, pd.DataFrame) and isinstance(df_signals.index, pd.MultiIndex):
                signals_dict = {
                    ticker: df.reset_index(level=0, drop=True) 
                    for ticker, df in df_signals.groupby(level=0)
                }
            # Handle single DataFrame with 'ticker' column
            elif isinstance(df_signals, pd.DataFrame) and 'ticker' in df_signals.columns:
                signals_dict = {
                    ticker: group.drop(columns=['ticker']) 
                    for ticker, group in df_signals.groupby('ticker')
                }
            # Handle single DataFrame for a single ticker
            elif isinstance(df_signals, pd.DataFrame) and len(self.tickers) == 1:
                signals_dict = {self.tickers[0]: df_signals}
            # Handle empty DataFrame
            elif isinstance(df_signals, pd.DataFrame) and df_signals.empty:
                logger.warning(f"Strategy returned empty DataFrame for fold {fold_start.date()} to {fold_end.date()}")
                signals_dict = {}
            else:
                logger.warning(
                    f"Unexpected format returned by strategy for fold {fold_start.date()} to {fold_end.date()}")
                signals_dict = {}
            
            # Validate and process each DataFrame in the dictionary
            for ticker, df in list(signals_dict.items()):
                if df.empty:
                    logger.warning(f"No signal data for ticker {ticker} from {fold_start.date()} to {fold_end.date()}")
                    signals_dict.pop(ticker)
                    continue
                
                # Ensure required columns exist
                if 'position' not in df.columns:
                    if 'signal' in df.columns:
                        df['position'] = df['signal']
                    else:
                        logger.warning(f"Ticker {ticker} is missing 'position' and 'signal' columns")
                        signals_dict.pop(ticker)
                        continue
                        
                if 'close' not in df.columns:
                    logger.warning(f"Ticker {ticker} is missing 'close' price data")
                    signals_dict.pop(ticker)
            
            return signals_dict
            
        except Exception as e:
            logger.error(f"Error generating signals: {e}", exc_info=True)
            return {}

    def _evaluate_fold(self, params: Dict[str, Any], fold_idx: int) -> Dict[str, Any]:
        """
        Evaluate strategy performance for a single fold.
        
        Args:
            params (Dict[str, Any]): Strategy parameters.
            fold_idx (int): Index of the fold to evaluate.
            
        Returns:
            Dict[str, Any]: Evaluation results including performance metrics.
        """
        fold_start, fold_end = self._fold_ranges[fold_idx]
        logger.debug(f"Evaluating fold {fold_idx + 1}/{self.cv_folds}: {fold_start.date()} to {fold_end.date()}")
        
        try:
            # Create strategy instance with given parameters
            strategy_instance = self.strategy_class(self.db_config, params)
            
            # Generate signals for this fold
            signals_dict = self._generate_signals_batch(strategy_instance, fold_start, fold_end)
            
            if not signals_dict:
                logger.warning(f"No valid signals generated for fold {fold_idx + 1}")
                return {"status": "error", "fold": fold_idx}
            
            # Calculate performance metrics
            ticker_metrics, weighted_metrics = PerformanceEvaluator.compute_multi_ticker_metrics(
                signals_dict, self.ticker_weights
            )
            
            # Calculate harmonic mean using the weighted metrics
            overall_hm = weighted_metrics.get('harmonic_mean', 0.0)
            
            return {
                "status": "ok",
                "fold": fold_idx, 
                "ticker_metrics": ticker_metrics,
                "weighted_metrics": weighted_metrics,
                "overall_hm": overall_hm
            }
            
        except Exception as e:
            logger.error(f"Error evaluating fold {fold_idx + 1}: {e}", exc_info=True)
            return {"status": "error", "fold": fold_idx}

    def walk_forward_cv(
        self, 
        params: Dict[str, Any]
    ) -> Tuple[Dict[str, Dict[str, float]], Dict[str, float], float]:
        """
        Perform walk-forward cross-validation using the specified parameters.
        
        This method evaluates the strategy across multiple time periods (folds)
        and computes aggregate performance metrics.
        
        Args:
            params (Dict[str, Any]): Strategy parameters.
            
        Returns:
            Tuple:
                ticker_metrics (Dict[str, Dict[str, float]]): Performance metrics per ticker.
                weighted_metrics (Dict[str, float]): Weighted average metrics across tickers.
                overall_hm (float): Overall harmonic mean.
        """
        # Check cache for previous evaluation result
        params_key = self._params_to_key(params)
        if params_key in self._evaluation_cache:
            return self._evaluation_cache[params_key]
        
        # Evaluate each fold - use multiprocessing if multiple folds and jobs
        if self.cv_folds > 1 and self.n_jobs > 1:
            # Create pool with dedicated worker function to evaluate each fold
            with Pool(min(self.n_jobs, self.cv_folds)) as pool:
                fold_eval_func = partial(self._evaluate_fold, params)
                fold_results = pool.map(fold_eval_func, range(self.cv_folds))
        else:
            # Sequential evaluation
            fold_results = [self._evaluate_fold(params, i) for i in range(self.cv_folds)]
        
        # Filter valid results
        valid_results = [r for r in fold_results if r["status"] == "ok"]
        
        if not valid_results:
            logger.warning("No valid fold evaluations for this parameter set")
            empty_result = ({}, {}, 0.0)
            self._evaluation_cache[params_key] = empty_result
            return empty_result
        
        # Aggregate ticker metrics across folds
        all_ticker_metrics = {}
        for result in valid_results:
            for ticker, metrics in result["ticker_metrics"].items():
                if ticker not in all_ticker_metrics:
                    all_ticker_metrics[ticker] = []
                all_ticker_metrics[ticker].append(metrics)
        
        # Calculate average metrics for each ticker
        ticker_metrics = {}
        for ticker, metrics_list in all_ticker_metrics.items():
            if metrics_list:
                # Average across folds for each metric
                ticker_metrics[ticker] = {
                    k: np.mean([m.get(k, 0) for m in metrics_list]) 
                    for k in metrics_list[0].keys()
                }
                # Recalculate harmonic mean on the averaged metrics
                ticker_metrics[ticker]['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(ticker_metrics[ticker])
        
        # Calculate weighted average metrics
        weighted_metrics = {}
        if ticker_metrics:
            # First, get list of all metrics from first ticker
            metric_keys = list(next(iter(ticker_metrics.values())).keys())
            
            # Calculate weighted average for each metric
            for key in metric_keys:
                if key == 'harmonic_mean':
                    continue  # Will calculate this from the averaged metrics
                    
                weighted_metrics[key] = sum(
                    metrics[key] * self.ticker_weights.get(ticker, 0)
                    for ticker, metrics in ticker_metrics.items()
                )
            
            # Calculate harmonic mean for the weighted metrics
            weighted_metrics['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(weighted_metrics)
        
        # Calculate overall harmonic mean
        overall_hm = weighted_metrics.get('harmonic_mean', 0.0)
        
        # Cache the results
        result = (ticker_metrics, weighted_metrics, overall_hm)
        self._evaluation_cache[params_key] = result
        
        return result

    def _calculate_risk_penalty(self, weighted_metrics: Dict[str, float]) -> float:
        """
        Calculate a penalty score based on risk threshold violations.
        
        Args:
            weighted_metrics (Dict[str, float]): Portfolio performance metrics.
            
        Returns:
            float: Risk penalty value.
        """
        penalty = 0.0
        
        # Maximum drawdown penalty (max_drawdown is negative, threshold is negative)
        max_dd = weighted_metrics.get('max_drawdown', 0)
        if max_dd < self.risk_thresholds.get('max_drawdown', -0.3):
            penalty += abs(max_dd - self.risk_thresholds['max_drawdown'])
        
        # Drawdown duration penalty
        dd_duration = weighted_metrics.get('drawdown_duration', 0)
        if dd_duration > self.risk_thresholds.get('drawdown_duration', 30):
            # Scale by 0.01 to make comparable with other percentage-based penalties
            penalty += (dd_duration - self.risk_thresholds['drawdown_duration']) * 0.01
        
        # Ulcer index penalty
        ulcer = weighted_metrics.get('ulcer_index', 0)
        if ulcer > self.risk_thresholds.get('ulcer_index', 0.15):
            penalty += (ulcer - self.risk_thresholds['ulcer_index'])
        
        # Volatility penalty
        vol = weighted_metrics.get('annualized_volatility', 0)
        if vol > self.risk_thresholds.get('annualized_volatility', 0.4):
            penalty += (vol - self.risk_thresholds['annualized_volatility'])
        
        return penalty

    def _objective_function(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Objective function for hyperparameter optimization.
        
        This function evaluates strategy performance and applies risk penalties.
        
        Args:
            params (Dict[str, Any]): Strategy parameters to evaluate.
            
        Returns:
            Dict[str, Any]: Results including loss, status, and metrics.
        """
        with mlflow.start_run(nested=True, run_name="Hyperopt_Trial"):
            # Log parameters
            mlflow.set_tag("strategy_class", self.strategy_class.__name__)
            mlflow.log_params(params)
            
            # Evaluate parameters
            ticker_metrics, weighted_metrics, overall_hm = self.walk_forward_cv(params)
            
            # If evaluation failed, return large loss
            if not weighted_metrics:
                return {
                    'loss': 100.0,  # Large loss value
                    'status': STATUS_OK,
                    'ticker_metrics': {},
                    'weighted_metrics': {}
                }
            
            # Calculate risk penalty
            penalty = self._calculate_risk_penalty(weighted_metrics)
            
            # Calculate final objective score (negative for minimization)
            objective_score = overall_hm - penalty
            
            # Log metrics
            mlflow.log_metric("overall_harmonic_mean", overall_hm)
            mlflow.log_metric("risk_penalty", penalty)
            mlflow.log_metric("objective_score", objective_score)
            
            for key, value in weighted_metrics.items():
                mlflow.log_metric(f"weighted_{key}", value)
                
            return {
                'loss': -objective_score,  # Negative for minimization
                'status': STATUS_OK,
                'ticker_metrics': ticker_metrics,
                'weighted_metrics': weighted_metrics
            }

    def run_optimization(self) -> Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
        """
        Execute hyperparameter search to find optimal strategy parameters.
        
        This method performs hyperparameter optimization using Hyperopt and returns
        the best parameters along with detailed performance reports.
        
        Returns:
            Tuple:
                best_params (Dict[str, Any]): The best parameters discovered.
                ticker_performance (pd.DataFrame): Performance metrics for each ticker.
                param_history (pd.DataFrame): History of parameter evaluations.
        """
        with mlflow.start_run(run_name=self.run_name):
            mlflow.set_tag("start_date", self.start_date.strftime("%Y-%m-%d"))
            mlflow.set_tag("end_date", self.end_date.strftime("%Y-%m-%d"))
            mlflow.set_tag("num_tickers", len(self.tickers))
            mlflow.set_tag("cv_folds", self.cv_folds)
            mlflow.set_tag("strategy_class", self.strategy_class.__name__)
            
            # Initialize trials object to store optimization history
            trials = Trials()
            
            # Run hyperparameter optimization
            logger.info(f"Starting optimization with {self.max_evals} evaluations")
            best = fmin(
                fn=self._objective_function,
                space=self.search_space,
                algo=tpe.suggest,
                max_evals=self.max_evals,
                trials=trials,
                show_progressbar=True
            )
            
            # Convert best parameters from Hyperopt format
            best_params = space_eval(self.search_space, best)
            logger.info(f"Optimization complete. Best parameters found: {best_params}")
            
            # Log best parameters
            mlflow.log_params(best_params)
            
            # Evaluate best parameters over full period
            logger.info("Evaluating best parameters over full period")
            ticker_metrics, weighted_metrics, overall_hm = self.walk_forward_cv(best_params)
            
            # Log final performance metrics
            for key, value in weighted_metrics.items():
                mlflow.log_metric(f"final_{key}", value)
            
            # Get company info for each ticker if available
            company_info = pd.DataFrame()
            try:
                strategy_instance = self.strategy_class(self.db_config, best_params)
                if hasattr(strategy_instance, 'get_company_info'):
                    company_info = strategy_instance.get_company_info(self.tickers)
                    if isinstance(company_info, pd.Series):
                        company_info = company_info.to_frame().T
            except Exception as e:
                logger.error(f"Error retrieving company info: {e}")
            
            # Generate ticker performance report
            report_rows = []
            for ticker, metrics in ticker_metrics.items():
                row = {'ticker': ticker, 'weight': self.ticker_weights.get(ticker, 0)}
                row.update(metrics)
                
                # Add company info if available
                if not company_info.empty and ticker in company_info.index:
                    info = company_info.loc[ticker]
                    for col in ['industry', 'industrykey', 'industrydisp', 
                               'sector', 'sectorkey', 'sectordisp']:
                        row[col] = info.get(col, None)
                
                report_rows.append(row)
            
            # Add weighted average row
            weighted_row = {'ticker': 'WEIGHTED_AVG', 'weight': 1.0}
            weighted_row.update(weighted_metrics)
            report_rows.append(weighted_row)
            
            ticker_performance = pd.DataFrame(report_rows).set_index('ticker')
            
            # Generate parameter history report
            param_history = []
            for trial_idx, trial in enumerate(trials.trials):
                if trial['result']['status'] == 'ok':
                    row = {'trial': trial_idx}
                    
                    # Extract parameters from the Hyperopt trial
                    vals = {k: v[0] for k, v in trial['misc']['vals'].items()}
                    params_trial = space_eval(self.search_space, vals)
                    row.update(params_trial)
                    
                    # Add performance metrics
                    if 'weighted_metrics' in trial['result']:
                        for k, v in trial['result']['weighted_metrics'].items():
                            row[k] = v
                    
                    # Add objective score
                    row['objective_score'] = -trial['result']['loss']
                    
                    param_history.append(row)
            
            param_history_df = pd.DataFrame(param_history)
            
            # Save reports as artifacts
            ticker_performance.to_csv("optimization_artefact" + self.strategy_class.__name__ + "_ticker_performance.csv")
            param_history_df.to_csv("optimization_artefact" + self.strategy_class.__name__ + "_param_history.csv")
            
            mlflow.log_artifact("optimization_artefact" + self.strategy_class.__name__ + "_ticker_performance.csv")
            mlflow.log_artifact("optimization_artefact" + self.strategy_class.__name__ + "_param_history.csv")

            json_string = json.dumps("optimization_artefact" + best_params, indent=4)

            with open("optimization_artefact" + self.strategy_class.__name__ + "_bestparams.json", "w") as file:
                file.write(json_string)
            
            return best_params, ticker_performance, param_history_df
        
    def get_evaluation_cache(self) -> Dict[str, Tuple]:
        """
        Get the cached evaluation results.
        
        The evaluation cache maps parameter hash keys to evaluation results,
        which can be reused by other components like the SensitivityAnalyzer.
        
        Returns:
            Dict[str, Tuple]: The evaluation cache.
        """
        return self._evaluation_cache