# trading_system/src/optimizer/performance_evaluator.py

"""
Performance metrics calculation module for trading strategy evaluation.

This module provides methods to calculate performance metrics, focusing on
portfolio-level evaluation where allocation is based on daily signals across
multiple assets. It also includes a method for single-asset analysis.
"""

import logging
from typing import Dict, List, Optional, Tuple, Union

import mlflow
import numpy as np
import pandas as pd

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Define type alias for metrics dictionary
MetricsDict = Dict[str, float]

class PerformanceEvaluator:
    """
    Calculates performance metrics for trading strategies.

    Provides methods for:
    1. Portfolio-level metrics: Calculates metrics based on a simulated portfolio
       return stream derived from signals across multiple assets.
    2. Single-asset metrics: Calculates metrics for an individual asset's signals.
    """

    @staticmethod
    def _calculate_metrics_from_returns(
        returns: pd.Series,
        annualization_factor: int = 252
    ) -> MetricsDict:
        """
        Calculates standard performance metrics from a given return series.

        Args:
            returns (pd.Series): Time series of returns (e.g., daily).
            annualization_factor (int): Factor for annualizing metrics (default 252).

        Returns:
            MetricsDict: Dictionary of computed performance metrics.
        """
        if returns.empty:
            logger.warning("Input return series is empty. Returning zero metrics.")
            return {k: 0.0 for k in PerformanceEvaluator._get_metric_names() if k != 'signal_accuracy'} # Exclude signal accuracy here

        # --- Basic Return Metrics ---
        cumulative_return = (1 + returns).prod() - 1
        n_periods = len(returns)
        # Handle potential case where n_periods might be zero if returns had only one value (though unlikely with prod)
        if n_periods > 0:
             # Ensure base is non-negative before exponentiation
            base = 1 + cumulative_return
            if base < 0:
                 # Avoid complex numbers if cumulative return is worse than -100%
                 # This might happen with leverage or large losses. Assign a very large negative return.
                annualized_return = -1.0 # Or another indicator of catastrophic loss
                logger.warning("Cumulative return less than -100%. Annualized return capped at -100%.")
            else:
                 annualized_return = (base ** (annualization_factor / n_periods)) - 1
        else:
             annualized_return = 0.0

        mean_ret = returns.mean()
        std_ret = returns.std()

        # --- Risk-Adjusted Ratios (Assuming Risk-Free Rate = 0) ---
        # Sharpe Ratio
        sharpe_ratio = (mean_ret / std_ret * np.sqrt(annualization_factor)) if std_ret > 1e-9 else 0.0

        # Sortino Ratio (Downside Deviation)
        negative_returns = returns[returns < 0]
        downside_std = negative_returns.std() if not negative_returns.empty else 1e-9 # Avoid division by zero
        sortino_ratio = (mean_ret / downside_std * np.sqrt(annualization_factor)) if downside_std > 1e-9 else 0.0

        # --- Drawdown Analysis ---
        equity_curve = (1 + returns).cumprod()
        rolling_max = equity_curve.cummax()
        drawdowns = (equity_curve / rolling_max) - 1
        max_drawdown = drawdowns.min() # Typically negative

        # Drawdown Duration (Vectorized)
        in_drawdown = (equity_curve < rolling_max).astype(int)
        drawdown_streaks = in_drawdown * (in_drawdown.groupby((in_drawdown != in_drawdown.shift()).cumsum()).cumcount() + 1)
        drawdown_duration = drawdown_streaks.max()

        # --- Other Metrics ---
        # Calmar Ratio
        calmar_ratio = (annualized_return / abs(max_drawdown)) if max_drawdown < -1e-9 else 0.0

        # Omega Ratio (Threshold = 0)
        gain = returns[returns > 0].sum()
        loss = abs(returns[returns < 0].sum())
        omega_ratio = (gain / loss) if loss > 1e-9 else (10.0 if gain > 1e-9 else 1.0) # Assign large value if only gains, 1 if no returns

        # Win Rate
        win_rate = (returns > 1e-9).mean() # Use small epsilon to avoid floating point issues with zero

        # Recovery Factor
        recovery_factor = (cumulative_return / abs(max_drawdown)) if max_drawdown < -1e-9 else 0.0

        # Annualized Volatility
        annualized_volatility = std_ret * np.sqrt(annualization_factor)

        # Ulcer Index
        ulcer_index = np.sqrt((drawdowns**2).mean()) if not drawdowns.empty else 0.0

        # Compile metrics
        metrics = {
            'cumulative_return': cumulative_return,
            'annualized_return': annualized_return,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            'omega_ratio': omega_ratio,
            'win_rate': win_rate,
            'recovery_factor': recovery_factor,
            'max_drawdown': max_drawdown,
            'drawdown_duration': float(drawdown_duration), # Ensure it's float
            'annualized_volatility': annualized_volatility,
            'ulcer_index': ulcer_index,
        }

        # Calculate harmonic mean (excluding signal accuracy)
        metrics['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(metrics)

        # Clean metrics (replace NaN/inf)
        for k, v in metrics.items():
            if np.isnan(v) or np.isinf(v):
                logger.warning(f"Metric '{k}' resulted in NaN or Inf, setting to 0.0")
                metrics[k] = 0.0

        return metrics

    @staticmethod
    def compute_portfolio_metrics(
        signals_dict: Dict[str, pd.DataFrame],
        allocation_rule: str = 'equal_weight_active',
        annualization_factor: int = 252
    ) -> MetricsDict:
        """
        Calculates performance metrics for a multi-asset portfolio.
        Handles single-ticker case by delegating.
        """
        if not signals_dict:
            logger.warning("Signals dictionary is empty. Cannot compute portfolio metrics.")
            return {**{k: 0.0 for k in PerformanceEvaluator._get_metric_names()}, 'signal_accuracy': 0.0}

        # --- Handle single ticker case first ---
        if len(signals_dict) == 1:
            ticker, df_single = list(signals_dict.items())[0]
            logger.info(f"Calculating metrics for single ticker: {ticker}. Delegating.")
            if 'position' not in df_single.columns:
                 logger.error(f"Single ticker DataFrame for {ticker} missing 'position' column. Cannot calculate performance.")
                 accuracy = 0.0 # Try to calculate accuracy
                 try:
                      if 'signal' in df_single.columns and 'close' in df_single.columns:
                         df_copy = df_single[['signal', 'close']].copy()
                         df_copy['return'] = df_copy['close'].pct_change()
                         df_copy['signal_prev'] = df_copy['signal'].shift(1)
                         df_copy = df_copy.dropna()
                         correct = (np.sign(df_copy['signal_prev']) == np.sign(df_copy['return'])) & (df_copy['signal_prev'] != 0) & (df_copy['return'] != 0)
                         total_valid = (df_copy['signal_prev'] != 0) & pd.notna(df_copy['signal_prev']) & pd.notna(df_copy['return'])
                         accuracy = correct.sum() / total_valid.sum() if total_valid.sum() > 0 else 0.0
                 except Exception: pass # Ignore accuracy calc error if main data missing
                 return {**{k: 0.0 for k in PerformanceEvaluator._get_metric_names()}, 'signal_accuracy': accuracy}

            metrics_single = PerformanceEvaluator.compute_single_asset_metrics(df_single, annualization_factor)
            try: # Calculate accuracy separately
                df_copy = df_single[['signal', 'close']].copy()
                df_copy['return'] = df_copy['close'].pct_change()
                df_copy['signal_prev'] = df_copy['signal'].shift(1)
                df_copy = df_copy.dropna()
                correct_signals = (np.sign(df_copy['signal_prev']) == np.sign(df_copy['return'])) & (df_copy['signal_prev'] != 0) & (df_copy['return'] != 0)
                total_non_zero_signals = (df_copy['signal_prev'] != 0) & pd.notna(df_copy['signal_prev']) & pd.notna(df_copy['return'])
                signal_accuracy = correct_signals.sum() / total_non_zero_signals.sum() if total_non_zero_signals.sum() > 0 else 0.0
                metrics_single['signal_accuracy'] = signal_accuracy
            except Exception as e_acc:
                logger.warning(f"Could not calculate signal accuracy for single asset {ticker}: {e_acc}")
                metrics_single['signal_accuracy'] = 0.0
            return metrics_single
        # --- End of single ticker handling ---


        # --- Multi-ticker logic starts here ---
        logger.info(f"Calculating portfolio metrics for {len(signals_dict)} tickers.")
        required_cols = ['close', 'signal']
        processed_dfs = {}
        valid_tickers = []

        for ticker, df in signals_dict.items():
            if df.empty or not all(col in df.columns for col in required_cols):
                logger.warning(f"Skipping ticker {ticker} due to missing columns or empty data.")
                continue
            processed_dfs[ticker] = df[required_cols].copy()
            valid_tickers.append(ticker)

        if not processed_dfs:
             logger.warning("No valid data after initial processing of tickers. Cannot compute portfolio metrics.")
             return {**{k: 0.0 for k in PerformanceEvaluator._get_metric_names()}, 'signal_accuracy': 0.0}

        # Combine FIRST
        combined_df = pd.concat(processed_dfs, axis=1, keys=valid_tickers)
        combined_df.index = pd.to_datetime(combined_df.index)
        combined_df = combined_df.sort_index()

        # Calculate AFTER combining
        close_df = combined_df.xs('close', level=1, axis=1)
        signal_df = combined_df.xs('signal', level=1, axis=1)
        returns_df = close_df.pct_change() # First row will be NaN
        signals_prev_df = signal_df.shift(1) # First row will be NaN

        portfolio_returns = pd.Series(index=returns_df.index, dtype=float)

        if allocation_rule == 'equal_weight_active':
            active_signals_count = (signals_prev_df.fillna(0) != 0).sum(axis=1)
            weights = pd.DataFrame(0.0, index=signals_prev_df.index, columns=signals_prev_df.columns)
            for date, count in active_signals_count.items():
                 if count > 0:
                      signals_on_date = signals_prev_df.loc[date].fillna(0)
                      non_zero_mask = signals_on_date != 0
                      weights.loc[date, non_zero_mask] = signals_on_date[non_zero_mask] / count

            # --- DEBUGGING PRINTS ---
            print("\n--- Debugging Portfolio Calculation ---")
            print("Shape of returns_df:", returns_df.shape)
            print("Head of returns_df:\n", returns_df.head())
            print("Tail of returns_df:\n", returns_df.tail())
            print("\nShape of weights:", weights.shape)
            print("Head of weights:\n", weights.head())
            print("Tail of weights:\n", weights.tail())

            # Calculate weighted returns (intermediate step)
            weighted_returns = weights * returns_df
            print("\nShape of weighted_returns:", weighted_returns.shape)
            print("Head of weighted_returns:\n", weighted_returns.head())
            print("Tail of weighted_returns:\n", weighted_returns.tail())
            print("Any NaNs in weighted_returns sum (axis=1)?", weighted_returns.sum(axis=1).isna().any())
            print("--- End Debugging Prints ---")

            # Calculate final portfolio return series
            portfolio_returns = weighted_returns.sum(axis=1, skipna=True)

        else:
            logger.error(f"Unsupported allocation_rule: {allocation_rule}")
            return {**{k: 0.0 for k in PerformanceEvaluator._get_metric_names()}, 'signal_accuracy': 0.0}

        # Drop leading NaNs from pct_change/shift
        portfolio_returns = portfolio_returns.dropna()

        if portfolio_returns.empty:
             logger.warning("Portfolio returns series is empty after dropping initial NaNs. Cannot compute metrics.")
             return {**{k: 0.0 for k in PerformanceEvaluator._get_metric_names()}, 'signal_accuracy': 0.0}

        print("\n--- Final Valid Portfolio Returns Series ---")
        print("Head:\n", portfolio_returns.head())
        print("Tail:\n", portfolio_returns.tail())
        print(f"Length: {len(portfolio_returns)}, Type: {type(portfolio_returns)}, Dtype: {portfolio_returns.dtype}, IndexType: {type(portfolio_returns.index)}")
        print("--- End Final Portfolio Returns Series ---")

        # Calculate metrics
        metrics = PerformanceEvaluator._calculate_metrics_from_returns(portfolio_returns, annualization_factor)

        # Calculate Signal Accuracy for Multi-Ticker
        correct_signals = 0
        total_non_zero_signals = 0
        common_index = signals_prev_df.index.intersection(returns_df.index)
        valid_signals_prev = signals_prev_df.loc[common_index]
        valid_returns = returns_df.loc[common_index]

        for date in common_index:
             for ticker in valid_signals_prev.columns:
                 signal_prev = valid_signals_prev.loc[date, ticker]
                 ret = valid_returns.loc[date, ticker]
                 if pd.notna(signal_prev) and pd.notna(ret) and signal_prev != 0:
                     total_non_zero_signals += 1
                     if np.sign(signal_prev) == np.sign(ret) and ret != 0:
                         correct_signals += 1

        signal_accuracy = (correct_signals / total_non_zero_signals) if total_non_zero_signals > 0 else 0.0
        metrics['signal_accuracy'] = signal_accuracy

        if mlflow.active_run():
            mlflow.log_metrics({f"portfolio_{k}": v for k, v in metrics.items() if pd.notna(v)})

        return metrics


    @staticmethod
    def compute_single_asset_metrics(
        df: pd.DataFrame,
        annualization_factor: int = 252
    ) -> MetricsDict:
        """
        Calculate performance metrics for a single asset strategy simulation.

        Args:
            df (pd.DataFrame): DataFrame with trading signals and pricing data.
                Must include 'close' and 'position' columns. The 'position' column
                represents the holding (-1, 0, 1) for the asset based on its own signal.
            annualization_factor (int): Factor for annualizing metrics (default 252).

        Returns:
            MetricsDict: Dictionary of computed performance metrics.
        """
        # Validate input data
        if df.empty or 'position' not in df.columns or 'close' not in df.columns:
            logger.warning("Invalid input data for single-asset performance calculation")
            # Return default zero metrics, excluding signal accuracy as it's portfolio-specific here
            return {k: 0.0 for k in PerformanceEvaluator._get_metric_names() if k != 'signal_accuracy'}

        df_copy = df.copy()

        # Calculate returns based on the asset's own position
        df_copy['daily_return'] = df_copy['close'].pct_change()
        # Assume 'position' column dictates holding based on *previous* day's signal/decision
        df_copy['strategy_return'] = df_copy['daily_return'] * df_copy['position'].shift(1)
        strategy_returns = df_copy['strategy_return'].fillna(0.0) # Fill NaNs from pct_change and shift
        
        # Calculate metrics using the common helper function
        metrics = PerformanceEvaluator._calculate_metrics_from_returns(strategy_returns, annualization_factor)

        # Log metrics to MLflow if a run is active (distinguish from portfolio)
        if mlflow.active_run():
             # Safely get ticker name if available (assuming it might be passed implicitly)
            ticker_name = df.name if hasattr(df, 'name') else 'single_asset'
            mlflow.log_metrics({f"asset_{ticker_name}_{k}": v for k, v in metrics.items()})

        return metrics

    @staticmethod
    def harmonic_mean(metrics: Dict[str, float]) -> float:
        """
        Compute the harmonic mean of selected positive performance metrics.

        Excludes metrics like max_drawdown (negative) and duration/volatility.

        Args:
            metrics (Dict[str, float]): Dictionary of performance metrics.

        Returns:
            float: The harmonic mean, or 0.0 if no valid positive metrics are found.
        """
        # Key positive performance ratios to include
        positive_metric_keys = [
            'sharpe_ratio',
            'sortino_ratio',
            'calmar_ratio',
            'omega_ratio',
            'win_rate',
            'recovery_factor',
            # Consider adding annualized_return if it's expected to be positive
            # 'annualized_return'
        ]

        values = []
        for key in positive_metric_keys:
            v = metrics.get(key)
            # Only include metrics that are strictly positive and finite
            if v is not None and np.isfinite(v) and v > 1e-9: # Use epsilon for float comparison
                values.append(v)

        if not values:
            return 0.0

        try:
            # Calculate harmonic mean: n / sum(1/x_i)
            return len(values) / np.sum([1.0 / v for v in values])
        except ZeroDivisionError:
             logger.warning("Division by zero encountered during harmonic mean calculation. One of the values might be extremely small.")
             return 0.0


    @staticmethod
    def _get_metric_names() -> List[str]:
        """
        Return the standard list of metrics computed by this evaluator.

        Returns:
            List[str]: Names of all metrics calculated.
        """
        return [
            'cumulative_return', 'annualized_return', 'sharpe_ratio',
            'sortino_ratio', 'calmar_ratio', 'omega_ratio', 'win_rate',
            'recovery_factor', 'max_drawdown', 'drawdown_duration',
            'annualized_volatility', 'ulcer_index', 'harmonic_mean',
            'signal_accuracy' # Added new metric
        ]

    @staticmethod
    def compute_multi_ticker_metrics(
        signals_dict: Dict[str, pd.DataFrame],
        weights: Optional[Dict[str, float]] = None, # Note: These weights are for averaging *individual* metrics, not portfolio construction
        annualization_factor: int = 252
    ) -> Tuple[Dict[str, MetricsDict], MetricsDict]:
        """
        DEPRECATED - Use compute_portfolio_metrics for portfolio evaluation.

        Computes metrics for each ticker individually and then calculates a
        weighted average of these individual metrics. This does NOT represent
        true portfolio performance. Use for informational purposes only.

        Args:
            signals_dict (Dict[str, pd.DataFrame]): Dictionary mapping ticker symbols
                to their signal DataFrames (must include 'close', 'position').
            weights (Optional[Dict[str, float]]): Weights for averaging individual
                ticker metrics. If None, equal weights are used.
            annualization_factor (int): Factor for annualizing performance.

        Returns:
            Tuple containing:
                Dict[str, MetricsDict]: Dictionary mapping ticker symbols to
                    their individual performance metrics.
                MetricsDict: Dictionary containing the weighted average of the
                             individual metrics (NOT portfolio metrics).
        """
        logger.warning("`compute_multi_ticker_metrics` is deprecated for portfolio evaluation. "
                       "It averages individual asset metrics, not true portfolio metrics. "
                       "Use `compute_portfolio_metrics` instead.")

        if not signals_dict:
            return {}, {}

        # Compute metrics for each ticker individually using the single-asset method
        ticker_metrics = {}
        valid_tickers = []
        for ticker, df in signals_dict.items():
            # Ensure 'position' exists for single asset calculation.
            # If strategy doesn't provide it, we might need to infer it or skip.
            # For now, assume it might exist from RiskManager or similar legacy code.
            # If not, this calculation might fail or return zeros.
            if 'position' not in df.columns:
                 logger.warning(f"Ticker {ticker} missing 'position' column for individual metric calculation. Skipping.")
                 continue

            # Use compute_single_asset_metrics which expects 'position'
            metrics = PerformanceEvaluator.compute_single_asset_metrics(df, annualization_factor)
            # Check if metrics calculation was successful (not all zeros due to bad input)
            if any(v != 0.0 for k, v in metrics.items() if k != 'signal_accuracy'): # signal_accuracy is not calculated here
                ticker_metrics[ticker] = metrics
                valid_tickers.append(ticker)

        if not ticker_metrics:
            return {}, {}

        # Handle ticker weights for averaging
        if weights is None:
            num_valid = len(valid_tickers)
            ticker_weights = {ticker: 1.0 / num_valid for ticker in valid_tickers} if num_valid > 0 else {}
        else:
            # Filter and normalize weights for valid tickers
            raw_weights = {ticker: weights.get(ticker, 0) for ticker in valid_tickers}
            total_weight = sum(raw_weights.values())
            if abs(total_weight) > 1e-10:
                ticker_weights = {ticker: w / total_weight for ticker, w in raw_weights.items()}
            else:
                num_valid = len(valid_tickers)
                ticker_weights = {ticker: 1.0 / num_valid for ticker in valid_tickers} if num_valid > 0 else {}


        # Calculate weighted average of individual metrics
        averaged_metrics = {}
        metric_keys = PerformanceEvaluator._get_metric_names()
        # Exclude signal accuracy as it wasn't calculated per ticker
        metric_keys = [k for k in metric_keys if k != 'signal_accuracy']


        for key in metric_keys:
             # Calculate weighted average, defaulting to 0 if metric is missing for a ticker
             weighted_value = sum(
                 ticker_metrics[ticker].get(key, 0.0) * ticker_weights.get(ticker, 0.0)
                 for ticker in valid_tickers
             )
             averaged_metrics[key] = weighted_value

        # Recalculate harmonic mean on the averaged metrics
        if 'harmonic_mean' in averaged_metrics: # Check if key exists before recalculating
             averaged_metrics['harmonic_mean'] = PerformanceEvaluator.harmonic_mean(averaged_metrics)

        # Log averaged metrics to MLflow if a run is active
        if mlflow.active_run():
            mlflow.log_metrics({f"avg_asset_{k}": v for k, v in averaged_metrics.items() if not np.isnan(v)})

        return ticker_metrics, averaged_metrics