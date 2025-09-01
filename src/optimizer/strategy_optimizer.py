"""
Strategy optimization module using walk-forward cross-validation and portfolio metrics.

This module optimizes trading strategy parameters by evaluating performance
on a portfolio level across walk-forward folds. It uses Hyperopt for search
and MLflow for tracking.
"""

import hashlib
import json
import logging
import traceback
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool, cpu_count
from typing import Any, Dict, List, Optional, Tuple, Type

import mlflow
import numpy as np
import pandas as pd
from hyperopt import STATUS_OK, Trials, fmin, space_eval, tpe

from src.optimizer.performance_evaluator import (MetricsDict,
                                                 PerformanceEvaluator)

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Constants for Loss Assignment ---
# Loss assigned if a strategy results in no trades/activity or complete evaluation failure.
# Hyperopt minimizes loss, so a higher positive loss is worse.
DEFAULT_NO_TRADE_LOSS = 100.0
DEFAULT_EVAL_FAILURE_LOSS = 100.0


class StrategyOptimizer:
    """
    Optimizes trading strategy parameters using walk-forward cross-validation
    and portfolio-based performance evaluation.

    Evaluates parameters by:
    1. Generating signals across multiple tickers for different time folds.
    2. Calculating portfolio performance metrics for each fold using PerformanceEvaluator.
    3. Averaging portfolio metrics across folds.
    4. Applying risk penalties based on portfolio metrics.
    5. Using Hyperopt to find parameters maximizing a risk-adjusted objective (e.g., harmonic mean).
    """

    def __init__(
        self,
        strategy_class: Type,
        db_config: Any, # Use appropriate type hint if possible, e.g., DatabaseConfig
        search_space: Dict[str, Any],
        tickers: List[str],
        start_date: str,
        end_date: str,
        initial_position: int = 0,
        cv_folds: int = 3,
        risk_thresholds: Optional[Dict[str, float]] = None,
        optimization_metric: str = 'harmonic_mean',
        max_evals: int = 50,
        run_name: str = "StrategyOptimization",
        n_jobs: int = -1
    ):
        """
        Initialize the strategy optimizer.

        Args:
            strategy_class (Type): Trading strategy class. Must implement `generate_signals`.
            db_config (Any): Database configuration object.
            search_space (Dict[str, Any]): Hyperparameter search space (Hyperopt format).
            tickers (List[str]): List of ticker symbols for the portfolio.
            start_date (str): Backtesting start date ('YYYY-MM-DD').
            end_date (str): Backtesting end date ('YYYY-MM-DD').
            initial_position (int): Starting position passed to the strategy (may not be relevant for portfolio).
            cv_folds (int): Number of folds for walk-forward cross-validation.
            risk_thresholds (Optional[Dict[str, float]]): Thresholds for portfolio risk metrics
                (e.g., {'max_drawdown': -0.2, 'annualized_volatility': 0.3}).
            optimization_metric (str): The portfolio metric to maximize (default: 'harmonic_mean').
            max_evals (int): Maximum evaluations for hyperparameter search.
            run_name (str): Name for the MLflow run.
            n_jobs (int): Parallel jobs for fold evaluation (-1 uses all CPU cores).
        """
        if not tickers:
            raise ValueError("Tickers list cannot be empty.")

        self.strategy_class = strategy_class
        self.db_config = db_config
        self.search_space = search_space
        self.tickers = tickers
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        if self.end_date <= self.start_date:
            raise ValueError("End date must be after start date.")

        self.run_name = run_name
        self.initial_position = initial_position
        self.cv_folds = max(1, cv_folds)
        self.max_evals = max_evals
        self.optimization_metric = optimization_metric

        self.risk_thresholds = risk_thresholds or {
            'max_drawdown': -0.30,
            'annualized_volatility': 0.40,
            'drawdown_duration': 90,
        }
        if 'max_drawdown' in self.risk_thresholds and self.risk_thresholds['max_drawdown'] > 0:
             logger.warning("max_drawdown threshold should be negative. Adjusting.")
             self.risk_thresholds['max_drawdown'] = -abs(self.risk_thresholds['max_drawdown'])

        self.n_jobs = n_jobs if n_jobs > 0 else max(1, cpu_count())
        self._evaluation_cache: Dict[str, Tuple[Optional[MetricsDict], Optional[Dict[str, MetricsDict]]]] = {}
        self._fold_ranges = self._compute_fold_ranges()

        logger.info(f"Initialized StrategyOptimizer for {strategy_class.__name__} with {len(tickers)} tickers.")
        logger.info(f"Period: {start_date} to {end_date}, Folds: {cv_folds}, Max Evals: {max_evals}")
        logger.info(f"Optimizing for portfolio metric: {self.optimization_metric}")
        logger.info(f"Risk Thresholds: {self.risk_thresholds}")


    def _compute_fold_ranges(self) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Computes date ranges for each walk-forward fold."""
        total_duration = self.end_date - self.start_date
        if total_duration.days < self.cv_folds:
             logger.warning(f"Total duration ({total_duration.days} days) is less than cv_folds ({self.cv_folds}). Reducing folds to duration.")
             self.cv_folds = max(1, total_duration.days) if total_duration.days > 0 else 1


        fold_length = total_duration / self.cv_folds if self.cv_folds > 0 else total_duration

        fold_ranges = []
        current_start = self.start_date
        for fold in range(self.cv_folds):
            current_end = current_start + fold_length
            if fold == self.cv_folds - 1:
                current_end = self.end_date
            if current_end <= current_start:
                 current_end = current_start + pd.Timedelta(days=1) 
                 if current_end > self.end_date: 
                      current_end = self.end_date
                      if current_start >= current_end and fold > 0: 
                           if fold_ranges: fold_ranges.pop() 
                           break
            
            if current_end > current_start : # Ensure fold has positive duration
                fold_ranges.append((current_start, current_end))
            else: # If somehow still invalid, log and potentially break if no progress
                logger.warning(f"Skipping invalid fold range generation for fold {fold+1}: start={current_start}, end={current_end}")
                if current_start >= self.end_date: break # Stop if we can't advance start date

            current_start = current_end 

        fold_ranges = [(s, e) for s, e in fold_ranges if e > s]

        if not fold_ranges:
             logger.warning("Could not create valid fold ranges. Using single fold for the entire period.")
             return [(self.start_date, self.end_date)] if self.end_date > self.start_date else []


        logger.debug(f"Computed Fold Ranges: {fold_ranges}")
        return fold_ranges

    def _params_to_key(self, params: Dict[str, Any]) -> str:
        """Convert parameters to a stable cache key."""
        try:
            # Attempt to convert all values to string for params that might not be JSON serializable by default (e.g. numpy types from hp.quniform)
            serializable_params = {k: str(v) if isinstance(v, (np.integer, np.floating)) else v for k, v in params.items()}
            serialized = json.dumps(serializable_params, sort_keys=True, default=str)
            return hashlib.md5(serialized.encode()).hexdigest()
        except TypeError as e:
             logger.error(f"Error creating cache key for params {params}: {e}. Using fallback hash.")
             return hashlib.md5(str(params).encode()).hexdigest()


    def _generate_signals_batch(
        self,
        strategy_instance: Any,
        fold_start: pd.Timestamp,
        fold_end: pd.Timestamp
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate signals for all tickers for a given fold period.

        Args:
            strategy_instance (Any): Instantiated strategy object.
            fold_start (pd.Timestamp): Start date for the fold.
            fold_end (pd.Timestamp): End date for the fold.

        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping tickers to their signal DataFrames.
                                     Returns empty dict on failure.
        """
        try:
            logger.debug(f"Generating signals for fold: {fold_start.date()} to {fold_end.date()} with tickers: {self.tickers}")
            
            df_signals_raw = strategy_instance.generate_signals(
                ticker=self.tickers,
                start_date=fold_start.strftime("%Y-%m-%d"),
                end_date=fold_end.strftime("%Y-%m-%d"),
                initial_position=self.initial_position,
                latest_only=False
            )

            logger.debug(f"Raw signals from strategy for fold {fold_start.date()}-{fold_end.date()}:\n{df_signals_raw.head() if isinstance(df_signals_raw, pd.DataFrame) and not df_signals_raw.empty else 'Empty or not DataFrame'}")

            signals_dict = {}
            if isinstance(df_signals_raw, pd.DataFrame) and not df_signals_raw.empty:
                if isinstance(df_signals_raw.index, pd.MultiIndex):
                    ticker_level_name = df_signals_raw.index.names[0] if df_signals_raw.index.names[0] else 0
                    signals_dict = {
                        str(ticker_key): df.reset_index(level=ticker_level_name, drop=True)
                        for ticker_key, df in df_signals_raw.groupby(level=ticker_level_name)
                    }
                elif 'ticker' in df_signals_raw.columns:
                    signals_dict = {
                        str(ticker_key): group.drop(columns=['ticker']).set_index(group.index.name or 'date')
                        for ticker_key, group in df_signals_raw.groupby('ticker')
                    }
                elif len(self.tickers) == 1: # Single ticker in self.tickers list
                    # Ensure the DataFrame index is DatetimeIndex
                    if not isinstance(df_signals_raw.index, pd.DatetimeIndex):
                        try:
                            df_signals_raw.index = pd.to_datetime(df_signals_raw.index)
                        except Exception as e:
                            logger.warning(f"Failed to convert index to DatetimeIndex for single ticker scenario {self.tickers[0]}: {e}")
                            return {} # Cannot proceed without DatetimeIndex
                    signals_dict = {str(self.tickers[0]): df_signals_raw}
                else:
                     logger.warning(f"Unexpected DataFrame format from strategy for multiple tickers. Cannot map tickers for fold {fold_start.date()}-{fold_end.date()}.")
                     return {}

            elif isinstance(df_signals_raw, dict):
                 signals_dict = {str(k): v for k, v in df_signals_raw.items()} # Ensure keys are strings
            else:
                logger.warning(f"Strategy returned empty or unexpected data type for fold {fold_start.date()} to {fold_end.date()}")
                return {}

            final_signals = {}
            required_cols = ['signal', 'close']
            for ticker_key_orig, df in signals_dict.items():
                ticker = str(ticker_key_orig) # Ensure ticker is string
                if not isinstance(df, pd.DataFrame) or df.empty:
                    logger.debug(f"No signal data for ticker {ticker} in fold.")
                    continue
                if not isinstance(df.index, pd.DatetimeIndex):
                    try:
                        df.index = pd.to_datetime(df.index)
                    except Exception:
                        logger.warning(f"Index for {ticker} is not datetime and could not be converted. Skipping.")
                        continue

                missing_required = [col for col in required_cols if col not in df.columns]
                if missing_required:
                    logger.warning(f"Ticker {ticker} missing required columns: {missing_required}. Skipping.")
                    continue

                if 'position' not in df.columns:
                    logger.debug(f"Ticker {ticker} missing optional 'position' column. Single-asset metrics might not be fully calculable.")

                # Ensure fold_start and fold_end are timezone-naive if df.index is, or vice-versa
                # This is a common pitfall with pandas DatetimeIndex slicing
                current_df_index_tz = df.index.tz
                if current_df_index_tz is not None and fold_start.tzinfo is None:
                    fold_start_aware = fold_start.tz_localize(current_df_index_tz)
                    fold_end_aware = fold_end.tz_localize(current_df_index_tz)
                elif current_df_index_tz is None and fold_start.tzinfo is not None:
                    fold_start_aware = fold_start.tz_localize(None)
                    fold_end_aware = fold_end.tz_localize(None)
                else: # Either both naive or both aware (and hopefully same tz)
                    fold_start_aware = fold_start
                    fold_end_aware = fold_end
                
                df_filtered = df.loc[fold_start_aware:fold_end_aware].copy() 
                if df_filtered.empty:
                    logger.debug(f"No data for ticker {ticker} within the fold range {fold_start_aware.date()}-{fold_end_aware.date()}.")
                    continue

                final_signals[ticker] = df_filtered

            if not final_signals:
                 logger.warning(f"No valid signals generated across all tickers for fold {fold_start.date()} to {fold_end.date()}.")

            return final_signals

        except Exception as e:
            logger.error(f"Error generating signals batch for fold {fold_start.date()}-{fold_end.date()}: {e}")
            logger.error(traceback.format_exc())
            return {}

    def _evaluate_fold(self, params: Dict[str, Any], fold_idx: int) -> Dict[str, Any]:
        """
        Evaluate strategy performance for a single fold using portfolio metrics.

        Args:
            params (Dict[str, Any]): Strategy parameters for this evaluation.
            fold_idx (int): The index of the fold to evaluate.

        Returns:
            Dict[str, Any]: Results dict including status, fold index,
                            portfolio metrics, and individual ticker metrics.
        """
        if fold_idx >= len(self._fold_ranges) or not self._fold_ranges: 
             logger.error(f"Invalid fold index {fold_idx} requested or no fold ranges available.")
             return {"status": "error", "fold": fold_idx, "error": "Invalid fold index or no ranges"}

        fold_start, fold_end = self._fold_ranges[fold_idx]
        fold_label = f"{fold_start.date()}_to_{fold_end.date()}"
        logger.info(f"Evaluating Fold {fold_idx + 1}/{self.cv_folds} ({fold_label}) with params: {params}")

        try:
            strategy_instance = self.strategy_class(self.db_config, params)
            signals_dict = self._generate_signals_batch(strategy_instance, fold_start, fold_end)

            if signals_dict:
                num_tickers_with_signals = 0
                total_rows_fold = 0
                for ticker_df in signals_dict.values():
                    if isinstance(ticker_df, pd.DataFrame) and not ticker_df.empty:
                        num_tickers_with_signals += 1
                        total_rows_fold += len(ticker_df)
                logger.info(
                    f"Fold {fold_idx + 1} ({fold_label}): Evaluator received signals for {num_tickers_with_signals}/{len(signals_dict)} attempted tickers. "
                    f"Total Signal Rows: {total_rows_fold}"
                )
                if total_rows_fold == 0:
                     logger.warning(f"Fold {fold_idx + 1}: signals_dict generated, but contained ZERO signal rows across all tickers.")
            else:
                logger.warning(f"Fold {fold_idx + 1}: No valid signals dictionary generated by _generate_signals_batch.")


            if not signals_dict: 
                logger.warning(f"Fold {fold_idx + 1}: No valid signals generated by strategy for any ticker.")
                zero_portfolio_metrics = {**{k: 0.0 for k in PerformanceEvaluator._get_metric_names()}, 'signal_accuracy': 0.0}
                return {
                    "status": "ok", 
                    "fold": fold_idx,
                    "portfolio_metrics": zero_portfolio_metrics,
                    "ticker_metrics": {},
                    "notes": "No valid signals generated by strategy for any ticker"
                }

            portfolio_metrics = PerformanceEvaluator.compute_portfolio_metrics(signals_dict)

            ticker_metrics_individual = {}
            try:
                 # Check if signals_dict has DataFrames and if any of them have 'position'
                 valid_dfs_for_indiv_metrics = [df for df in signals_dict.values() if isinstance(df, pd.DataFrame) and 'position' in df.columns]
                 if valid_dfs_for_indiv_metrics:
                      # Create a new dict with only valid dfs for this calc
                      valid_signals_dict_for_indiv = {
                          k:v for k,v in signals_dict.items() 
                          if isinstance(v, pd.DataFrame) and 'position' in v.columns
                      }
                      if valid_signals_dict_for_indiv:
                           ticker_metrics_individual, _ = PerformanceEvaluator.compute_multi_ticker_metrics(valid_signals_dict_for_indiv)
                 else:
                      logger.info(f"Fold {fold_idx + 1}: Skipping individual ticker metrics (no 'position' column found in any valid signal DataFrame).")
            except Exception as e_ticker:
                 logger.warning(f"Fold {fold_idx + 1}: Failed to compute informational individual ticker metrics: {e_ticker}")


            return {
                "status": "ok",
                "fold": fold_idx,
                "portfolio_metrics": portfolio_metrics,
                "ticker_metrics": ticker_metrics_individual,
            }

        except Exception as e:
            logger.error(f"Error evaluating fold {fold_idx + 1} ({fold_label}): {e}")
            logger.error(traceback.format_exc())
            return {"status": "error", "fold": fold_idx, "error": str(e)}

    def walk_forward_cv(
        self,
        params: Dict[str, Any]
    ) -> Tuple[Optional[MetricsDict], Optional[Dict[str, MetricsDict]]]:
        """
        Perform walk-forward cross-validation for a given set of parameters.

        Averages portfolio metrics across all successful folds. Also averages
        individual ticker metrics if available.

        Args:
            params (Dict[str, Any]): Strategy parameters to evaluate.

        Returns:
            Tuple[Optional[MetricsDict], Optional[Dict[str, MetricsDict]]]:
                - Average portfolio metrics across folds (or None if all folds failed).
                - Average individual ticker metrics across folds (or None if unavailable/failed).
        """
        params_key = self._params_to_key(params)
        if params_key in self._evaluation_cache:
            logger.debug(f"Cache hit for params: {params}")
            return self._evaluation_cache[params_key]

        logger.info(f"Running Walk-Forward CV for params: {params}")

        if not self._fold_ranges: 
            logger.warning(f"No fold ranges available for CV with params: {params}. Cannot proceed.")
            self._evaluation_cache[params_key] = (None, None)
            return None, None

        if self.cv_folds > 1 and self.n_jobs != 1: # n_jobs != 1 allows for n_jobs = -1 (all cores) or > 1
            try:
                fold_eval_func = partial(self._evaluate_fold, params)
                # If n_jobs is -1, cpu_count() is used. Otherwise, use min(n_jobs, cv_folds).
                num_processes = min(self.n_jobs if self.n_jobs > 0 else cpu_count(), self.cv_folds)
                logger.info(f"Using {num_processes} parallel processes for {self.cv_folds} folds.")
                with Pool(processes=num_processes) as pool:
                     fold_results = pool.map(fold_eval_func, range(self.cv_folds))
            except Exception as pool_error:
                 logger.error(f"Multiprocessing pool failed: {pool_error}. Falling back to sequential execution.")
                 logger.error(traceback.format_exc())
                 fold_results = [self._evaluate_fold(params, i) for i in range(self.cv_folds)]
        else:
            logger.info(f"Running {self.cv_folds} folds sequentially.")
            fold_results = [self._evaluate_fold(params, i) for i in range(self.cv_folds)]

        valid_results = [r for r in fold_results if r.get("status") == "ok"]

        if not valid_results:
            logger.warning(f"No successful folds for params: {params}. Cannot calculate average metrics.")
            self._evaluation_cache[params_key] = (None, None)
            return None, None

        all_portfolio_metrics = [r["portfolio_metrics"] for r in valid_results]
        avg_portfolio_metrics_df = pd.DataFrame(all_portfolio_metrics)
        avg_portfolio_metrics = avg_portfolio_metrics_df.mean().fillna(0.0).to_dict()
        
        if avg_portfolio_metrics: 
            avg_portfolio_metrics['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(avg_portfolio_metrics)
        else: 
            avg_portfolio_metrics = {k: 0.0 for k in PerformanceEvaluator._get_metric_names()}


        all_ticker_metrics_by_fold = [r.get("ticker_metrics", {}) for r in valid_results]
        aggregated_ticker_metrics: Dict[str, List[MetricsDict]] = {}
        for fold_ticker_metrics in all_ticker_metrics_by_fold:
            for ticker, metrics in fold_ticker_metrics.items(): # ticker is str, metrics is MetricsDict
                 # Ensure metrics is a dict before proceeding
                 if not isinstance(metrics, dict):
                     logger.debug(f"Skipping non-dict metrics for ticker {ticker} in fold.")
                     continue
                 if ticker not in aggregated_ticker_metrics:
                      aggregated_ticker_metrics[ticker] = []
                 aggregated_ticker_metrics[ticker].append(metrics)

        avg_ticker_metrics_result: Optional[Dict[str, MetricsDict]] = {} 
        if aggregated_ticker_metrics:
             for ticker, metrics_list in aggregated_ticker_metrics.items():
                  if metrics_list: # Ensure list is not empty
                       avg_metrics_df = pd.DataFrame(metrics_list)
                       avg_metrics_for_ticker = avg_metrics_df.mean().fillna(0.0).to_dict()
                       if avg_metrics_for_ticker: # Check if dict is not empty
                           avg_metrics_for_ticker['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(avg_metrics_for_ticker)
                       if avg_ticker_metrics_result is not None: # mypy check
                           avg_ticker_metrics_result[ticker] = avg_metrics_for_ticker
        else:
             avg_ticker_metrics_result = None


        result = (avg_portfolio_metrics, avg_ticker_metrics_result)
        self._evaluation_cache[params_key] = result
        logger.info(f"Finished CV for params: {params}. Avg Portfolio Harmonic Mean: {avg_portfolio_metrics.get('harmonic_mean', 'N/A'):.4f}")
        return result


    def _calculate_risk_penalty(self, portfolio_metrics: MetricsDict) -> float:
        """
        Calculate a penalty score based on portfolio risk threshold violations.
        Higher penalty means more/worse violations.

        Args:
            portfolio_metrics (MetricsDict): The averaged portfolio metrics.

        Returns:
            float: Risk penalty score (>= 0).
        """
        if not portfolio_metrics:
             return 100.0 

        penalty = 0.0
        violated_thresholds = []

        max_dd_thresh = self.risk_thresholds.get('max_drawdown', -1.0)
        max_dd = portfolio_metrics.get('max_drawdown', 0.0)
        if max_dd < max_dd_thresh: 
            penalty += abs(max_dd - max_dd_thresh) * 2 
            violated_thresholds.append(f"Max Drawdown ({max_dd:.2%} < {max_dd_thresh:.2%})")

        vol_thresh = self.risk_thresholds.get('annualized_volatility', 1.0)
        vol = portfolio_metrics.get('annualized_volatility', 0.0)
        if vol > vol_thresh:
            penalty += (vol - vol_thresh)
            violated_thresholds.append(f"Volatility ({vol:.2%} > {vol_thresh:.2%})")

        dd_dur_thresh = self.risk_thresholds.get('drawdown_duration', 365 * 10)
        dd_dur = portfolio_metrics.get('drawdown_duration', 0.0)
        if dd_dur > dd_dur_thresh:
             penalty += (dd_dur - dd_dur_thresh) / 252.0 
             violated_thresholds.append(f"Drawdown Duration ({dd_dur:.0f} > {dd_dur_thresh:.0f} days)")

        ulcer_thresh = self.risk_thresholds.get('ulcer_index')
        if ulcer_thresh is not None:
             ulcer = portfolio_metrics.get('ulcer_index', 0.0)
             if ulcer > ulcer_thresh:
                  penalty += (ulcer - ulcer_thresh) * 2 
                  violated_thresholds.append(f"Ulcer Index ({ulcer:.3f} > {ulcer_thresh:.3f})")

        if violated_thresholds:
             logger.debug(f"Risk penalty applied: {penalty:.4f}. Violations: {', '.join(violated_thresholds)}")
        return penalty

    def _objective_function(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Objective function for Hyperopt minimization.

        Evaluates parameters using walk_forward_cv, calculates risk penalty,
        and returns a score to be minimized (negative of reward minus penalty).
        Args:
            params (Dict[str, Any]): Parameters suggested by Hyperopt.

        Returns:
            Dict[str, Any]: Dictionary expected by Hyperopt, including 'loss' and 'status'.
        """
        trial_run_name = f"Trial_{params_to_short_str(params)}"
        if len(trial_run_name) > 250 : 
            trial_run_name = trial_run_name[:250]


        with mlflow.start_run(nested=True, run_name=trial_run_name):
            serializable_params_for_mlflow = {}
            for key, value in params.items():
                try:
                    # Convert numpy types that are not directly serializable by MLflow
                    if isinstance(value, (np.integer, np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
                        serializable_params_for_mlflow[key] = int(value)
                    elif isinstance(value, (np.floating)):
                        serializable_params_for_mlflow[key] = float(value)
                    elif isinstance(value, np.ndarray):
                        serializable_params_for_mlflow[key] = str(list(value)) # Log array as string list
                    else:
                        serializable_params_for_mlflow[key] = value
                except Exception: 
                    serializable_params_for_mlflow[key] = str(value)
            
            try:
                mlflow.log_params(serializable_params_for_mlflow)
            except Exception as e_mlflow_param:
                logger.error(f"Failed to log some params to MLflow: {e_mlflow_param}")


            mlflow.set_tag("strategy_class", self.strategy_class.__name__)

            avg_portfolio_metrics, avg_ticker_metrics = self.walk_forward_cv(params)

            if avg_portfolio_metrics is None:
                logger.warning(f"Walk-forward CV failed for params: {params}. Assigning high loss.")
                mlflow.log_metric("objective_score", -DEFAULT_EVAL_FAILURE_LOSS)
                mlflow.log_metric("risk_penalty", 0.0) 
                return {
                    'loss': DEFAULT_EVAL_FAILURE_LOSS,
                    'status': STATUS_OK,
                    'params': params,
                    'portfolio_metrics': None,
                    'ticker_metrics': None,
                    'objective_score': -DEFAULT_EVAL_FAILURE_LOSS,
                    'risk_penalty': 0.0,
                    'notes': 'Walk-forward CV failed'
                }

            is_no_effective_trade = (
                np.isclose(avg_portfolio_metrics.get('annualized_volatility', 0.0), 0.0, atol=1e-7) and
                np.isclose(avg_portfolio_metrics.get('cumulative_return', 0.0), 0.0, atol=1e-7) and
                np.isclose(avg_portfolio_metrics.get('max_drawdown', 0.0), 0.0, atol=1e-7)
            )

            if is_no_effective_trade:
                logger.info(f"Params {params} resulted in no effective trading activity. Assigning high loss.")
                objective_score_no_trade = -DEFAULT_NO_TRADE_LOSS
                mlflow.log_metric("objective_score", objective_score_no_trade)
                mlflow.log_metric("risk_penalty", 0.0) 
                if avg_portfolio_metrics:
                    mlflow.log_metrics({f"avg_portfolio_{k}": v for k, v in avg_portfolio_metrics.items() if pd.notna(v)})
                
                return {
                    'loss': DEFAULT_NO_TRADE_LOSS,
                    'status': STATUS_OK,
                    'params': params,
                    'portfolio_metrics': avg_portfolio_metrics,
                    'ticker_metrics': avg_ticker_metrics,
                    'objective_score': objective_score_no_trade,
                    'risk_penalty': 0.0, 
                    'notes': 'No effective trading activity'
                }

            primary_metric_value = avg_portfolio_metrics.get(self.optimization_metric, 0.0)
            penalty = self._calculate_risk_penalty(avg_portfolio_metrics)
            objective_score = primary_metric_value - penalty
            loss = -objective_score 

            mlflow.log_metric(f"avg_portfolio_{self.optimization_metric}", primary_metric_value)
            mlflow.log_metric("risk_penalty", penalty)
            mlflow.log_metric("objective_score", objective_score)
            mlflow.log_metrics({f"avg_portfolio_{k}": v for k, v in avg_portfolio_metrics.items() if k != self.optimization_metric and pd.notna(v)})
            
            return {
                'loss': loss,
                'status': STATUS_OK,
                'params': params,
                'portfolio_metrics': avg_portfolio_metrics,
                'ticker_metrics': avg_ticker_metrics,
                'objective_score': objective_score,
                'risk_penalty': penalty
            }

    def run_optimization(self) -> Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
        """
        Execute the hyperparameter optimization process.

        Uses Hyperopt (TPE algorithm) to search the parameter space defined
        in `search_space`, guided by the `_objective_function`.

        Returns:
            Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
                - best_params: Dictionary of the best parameters found.
                - portfolio_performance_report: DataFrame summarizing portfolio metrics for best params.
                - param_history_report: DataFrame detailing each evaluation trial.
        """
        main_run_name = f"{self.run_name}_{self.strategy_class.__name__}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with mlflow.start_run(run_name=main_run_name) as run:
            mlflow.log_param("strategy_class", self.strategy_class.__name__)
            mlflow.log_param("start_date", self.start_date.strftime("%Y-%m-%d"))
            mlflow.log_param("end_date", self.end_date.strftime("%Y-%m-%d"))
            mlflow.log_param("tickers", ",".join(self.tickers))
            mlflow.log_param("cv_folds", self.cv_folds)
            mlflow.log_param("max_evals", self.max_evals)
            mlflow.log_param("optimization_metric", self.optimization_metric)
            mlflow.log_param("risk_thresholds", json.dumps(self.risk_thresholds))

            logger.info(f"Starting optimization run: {main_run_name}")
            logger.info(f"Search Space: {self.search_space}")

            trials = Trials()
            try:
                best_hyperparams_raw = fmin(
                    fn=self._objective_function,
                    space=self.search_space,
                    algo=tpe.suggest,
                    max_evals=self.max_evals,
                    trials=trials,
                    show_progressbar=True, # Keep True for user feedback during long runs
                    rstate=np.random.default_rng(42)
                )
                # fmin returns dictionary where values are lists, e.g. {'param': [value]}
                # space_eval expects this format or the direct values.
                # best_params = space_eval(self.search_space, best_hyperparams_raw) # This line is correct
                best_params = space_eval(self.search_space, best_hyperparams_raw)


            except Exception as e:
                 logger.error(f"Hyperopt optimization failed: {e}")
                 logger.error(traceback.format_exc())
                 if trials.best_trial and 'result' in trials.best_trial and 'loss' in trials.best_trial['result']:
                      logger.warning("Optimization interrupted. Using best trial found so far.")
                      best_hyperparams_raw_vals = trials.best_trial['misc']['vals']
                      best_params = space_eval(self.search_space, best_hyperparams_raw_vals)
                 else:
                      logger.error("No usable trials completed. Returning empty results.")
                      return {}, pd.DataFrame(), pd.DataFrame()


            logger.info(f"Optimization complete. Best parameters found (raw from fmin): {best_hyperparams_raw}")
            logger.info(f"Optimization complete. Best parameters (evaluated by space_eval): {best_params}")


            best_trial = trials.best_trial
            portfolio_report_df = pd.DataFrame()
            param_history_df = pd.DataFrame()

            if best_trial and 'result' in best_trial and best_trial['result'].get('status') == STATUS_OK:
                 # Convert numpy types in best_params to native Python types for MLflow logging
                 serializable_best_params_for_mlflow = {}
                 for k, v_val in best_params.items():
                     if isinstance(v_val, (np.integer, np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
                         serializable_best_params_for_mlflow[f"best_{k}"] = int(v_val)
                     elif isinstance(v_val, (np.floating)):
                         serializable_best_params_for_mlflow[f"best_{k}"] = float(v_val)
                     else:
                         serializable_best_params_for_mlflow[f"best_{k}"] = v_val
                 mlflow.log_params(serializable_best_params_for_mlflow)
                 
                 best_trial_result = best_trial['result']
                 mlflow.log_metric("best_objective_score", best_trial_result.get('objective_score', np.nan))
                 mlflow.log_metric("best_risk_penalty", best_trial_result.get('risk_penalty', np.nan))
                 
                 best_portfolio_metrics = best_trial_result.get('portfolio_metrics')
                 if best_portfolio_metrics:
                      mlflow.log_metrics({f"best_portfolio_{k}": v for k, v in best_portfolio_metrics.items() if pd.notna(v)})
                      portfolio_report_df = pd.DataFrame([best_portfolio_metrics])
                      for p_name, p_val in best_params.items():
                           try:
                               portfolio_report_df[f'param_{p_name}'] = p_val.item() if hasattr(p_val, 'item') else p_val
                           except Exception: 
                               portfolio_report_df[f'param_{p_name}'] = str(p_val)
                 
                 param_history = []
                 for i, trial_item in enumerate(trials.trials):
                     result = trial_item.get('result', {})
                     if result.get('status') == STATUS_OK:
                         row = {'trial': i}
                         # Parameters for this trial might be in result['params'] or need re-evaluation from trial_item['misc']['vals']
                         trial_params_from_result = result.get('params')
                         if trial_params_from_result:
                             current_trial_params = trial_params_from_result
                         else: # Fallback if params not stored in result
                             vals_raw = trial_item['misc']['vals']
                             current_trial_params = space_eval(self.search_space, vals_raw)
                         
                         # Ensure params are serializable for DataFrame
                         serializable_trial_params = {}
                         for k_param, v_param in current_trial_params.items():
                             serializable_trial_params[k_param] = v_param.item() if hasattr(v_param, 'item') else v_param
                         row.update(serializable_trial_params)


                         pf_metrics = result.get('portfolio_metrics')
                         if pf_metrics:
                             row.update({f"portfolio_{k}": v for k, v in pf_metrics.items()})

                         row['objective_score'] = result.get('objective_score', np.nan)
                         row['risk_penalty'] = result.get('risk_penalty', np.nan)
                         row['loss'] = result.get('loss', np.nan)
                         row['notes'] = result.get('notes', '')
                         param_history.append(row)
                 param_history_df = pd.DataFrame(param_history)

                 try:
                      report_suffix = f"{self.strategy_class.__name__}"
                      portfolio_csv = f"optimization_artefact_{report_suffix}_portfolio_performance.csv"
                      history_csv = f"optimization_artefact_{report_suffix}_param_history.csv"
                      params_json_file = f"optimization_artefact_{report_suffix}_bestparams.json"

                      if not portfolio_report_df.empty:
                          portfolio_report_df.to_csv(portfolio_csv, index=False)
                          mlflow.log_artifact(portfolio_csv)
                      if not param_history_df.empty:
                          param_history_df.to_csv(history_csv, index=False)
                          mlflow.log_artifact(history_csv)
                      
                      # Serialize best_params for JSON (handle numpy types)
                      serializable_best_params_for_json = {}
                      for k, v_val in best_params.items():
                          if isinstance(v_val, (np.integer, np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
                              serializable_best_params_for_json[k] = int(v_val)
                          elif isinstance(v_val, (np.floating)):
                              serializable_best_params_for_json[k] = float(v_val)
                          elif isinstance(v_val, np.bool_):
                              serializable_best_params_for_json[k] = bool(v_val)
                          elif isinstance(v_val, np.ndarray):
                              serializable_best_params_for_json[k] = v_val.tolist()
                          else:
                              serializable_best_params_for_json[k] = v_val
                      
                      with open(params_json_file, "w") as f:
                           json.dump(serializable_best_params_for_json, f, indent=4)
                      mlflow.log_artifact(params_json_file)
                      logger.info(f"Saved reports: {portfolio_csv}, {history_csv}, {params_json_file}")

                 except Exception as io_error:
                      logger.error(f"Error saving optimization artifacts: {io_error}")
            else:
                 logger.error("Best trial data not found or trial failed. Cannot generate final reports.")
            
            return best_params, portfolio_report_df, param_history_df


    def get_evaluation_cache(self) -> Dict[str, Tuple[Optional[MetricsDict], Optional[Dict[str, MetricsDict]]]]:
        """
        Retrieve the cache of evaluated parameters and their results.

        Returns:
            Dict[str, Tuple[Optional[MetricsDict], Optional[Dict[str, MetricsDict]]]]:
                Cache mapping parameter hash to (portfolio_metrics, ticker_metrics) tuple.
        """
        return self._evaluation_cache

def params_to_short_str(params: Dict[str, Any], max_len: int = 50) -> str:
    """Creates a short string representation of parameters for labels/names."""
    items = []
    for k, v in sorted(params.items()): 
        # Convert numpy types to native Python types for string formatting
        if isinstance(v, (np.integer, np.int_)): v = int(v)
        elif isinstance(v, (np.floating)): v = float(v)
        elif isinstance(v, np.bool_): v = bool(v)

        if isinstance(v, float):
            v_str = f"{v:.3g}" 
        elif isinstance(v, (list, tuple)):
             v_str = "-".join(map(str,v)) 
        else:
            v_str = str(v)
        
        item_str = f"{k}={v_str}"
        if len(item_str) > max_len / 2 : 
            item_str = item_str[:int(max_len/2)] + "~"
        items.append(item_str)

    full_str = "_".join(items)
    return (full_str[:max_len-3] + '...') if len(full_str) > max_len else full_str