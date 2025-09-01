# trading_system/src/optimizer/sensitivity_analyzer.py

"""
Sensitivity analysis module for trading strategy parameters.

This module provides tools for assessing the impact of parameter variations
on trading strategy performance through systematic parameter perturbation
and correlation analysis.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import mlflow
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from tqdm import tqdm

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SensitivityAnalyzer:
    """
    Conducts sensitivity analysis by perturbing strategy parameters.
    
    This class performs a systematic analysis of how variations in strategy
    parameters affect performance, helping identify which parameters have
    the greatest impact on trading outcomes.
    """

    def __init__(
        self,
        strategy_optimizer,
        base_params: Dict[str, Any],
        numeric_perturbation: float = 0.1,
        num_samples: int = 30,
        parallel: bool = True
    ):
        """
        Initialize the sensitivity analyzer.
        
        Args:
            strategy_optimizer: Instance of StrategyOptimizer to reuse for evaluations.
            base_params (Dict[str, Any]): Baseline (optimized) parameters.
            numeric_perturbation (float): Perturbation factor for numeric parameters (default: 0.1 or 10%).
            num_samples (int): Number of parameter perturbation samples.
            parallel (bool): Whether to use parallel processing for evaluations.
        """
        self.optimizer = strategy_optimizer
        self.base_params = base_params.copy()
        self.numeric_perturbation = numeric_perturbation
        self.num_samples = num_samples
        self.parallel = parallel
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Identify numeric and categorical parameters
        self.numeric_params = {k: v for k, v in base_params.items() 
                              if isinstance(v, (int, float))}
        self.categorical_params = {k: v for k, v in base_params.items() 
                                  if not isinstance(v, (int, float))}
        
        logger.info(f"Initialized SensitivityAnalyzer with {len(self.numeric_params)} numeric parameters")
        logger.info(f"Numeric parameters: {list(self.numeric_params.keys())}")
        logger.info(f"Categorical parameters: {list(self.categorical_params.keys())}")

    def _generate_perturbations(self) -> List[Dict[str, Any]]:
        """
        Generate perturbed parameter samples using stratified sampling.
        
        This method creates parameter variations by systematically perturbing
        each parameter while keeping others at their base values, and also
        generates some random combinations for interaction effects.
        
        Returns:
            List[Dict[str, Any]]: List of perturbed parameter sets.
        """
        samples = []
        
        # 1. Start with the base parameters
        samples.append(self.base_params.copy())
        
        # 2. One-at-a-time perturbations for each numeric parameter
        for param_name, base_value in self.numeric_params.items():
            # Positive perturbation
            sample_pos = self.base_params.copy()
            sample_pos[param_name] = base_value * (1 + self.numeric_perturbation)
            samples.append(sample_pos)
            
            # Negative perturbation
            sample_neg = self.base_params.copy()
            sample_neg[param_name] = base_value * (1 - self.numeric_perturbation)
            if isinstance(base_value, int):
                sample_neg[param_name] = max(1, int(sample_neg[param_name]))  # Ensure integers stay positive
            samples.append(sample_neg)
        
        # 3. Random perturbations for combined effects
        num_random_samples = self.num_samples - len(samples)
        for _ in range(max(0, num_random_samples)):
            sample = self.base_params.copy()
            for param_name, base_value in self.numeric_params.items():
                # Apply random perturbation within the range
                perturb = base_value * self.numeric_perturbation
                # For integers, ensure we get integer results
                if isinstance(base_value, int):
                    new_val = np.random.randint(
                        max(1, int(base_value - perturb)), 
                        int(base_value + perturb) + 1
                    )
                else:
                    new_val = np.random.uniform(base_value - perturb, base_value + perturb)
                sample[param_name] = new_val
            samples.append(sample)
        
        return samples

    def _evaluate_sample(self, sample_idx: int, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a single parameter sample.
        
        Args:
            sample_idx (int): Index of the sample being evaluated.
            params (Dict[str, Any]): Parameter set to evaluate.
            
        Returns:
            Dict[str, Any]: Evaluation results including performance metrics.
        """
        try:
            # Get metrics from optimizer using walk-forward CV
            ticker_metrics, weighted_metrics, overall_hm = self.optimizer.walk_forward_cv(params)
            
            # Create result dictionary
            result = {
                'sample': sample_idx,
                'overall_harmonic_mean': overall_hm
            }
            
            # Add parameters with prefix
            for param_name, param_value in params.items():
                result[f'param_{param_name}'] = param_value
                
            # Add metrics with prefix
            for metric_name, metric_value in weighted_metrics.items():
                result[f'metric_{metric_name}'] = metric_value
                
            return result
            
        except Exception as e:
            logger.error(f"Error evaluating sample {sample_idx}: {e}", exc_info=True)
            return {
                'sample': sample_idx,
                'error': str(e)
            }

    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Run sensitivity analysis by evaluating perturbed parameter sets.
        
        This method:
        1. Generates perturbed parameter samples
        2. Evaluates each sample using the optimizer
        3. Computes correlations between parameters and performance metrics
        4. Creates detailed sensitivity reports
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: 
                sensitivity_results: DataFrame with all evaluation results
                parameter_impact: DataFrame showing parameter impact on performance metrics
        """
        with mlflow.start_run(run_name="Sensitivity_Analysis", nested=True):
            # Generate perturbed parameter samples
            samples = self._generate_perturbations()
            logger.info(f"Generated {len(samples)} parameter samples for sensitivity analysis")
            
            # Log the analysis setup
            mlflow.log_param("base_params", str(self.base_params))
            mlflow.log_param("num_samples", len(samples))
            mlflow.log_param("numeric_perturbation", self.numeric_perturbation)
            
            # Evaluate all samples
            results = []
            
            # Check if we should use parallel processing
            if self.parallel and len(samples) > 1:
                with ProcessPoolExecutor() as executor:
                    # Submit all evaluations
                    future_to_idx = {
                        executor.submit(self._evaluate_sample, i, params): i 
                        for i, params in enumerate(samples)
                    }
                    
                    # Collect results as they complete
                    for future in tqdm(as_completed(future_to_idx), total=len(samples), 
                                      desc="Evaluating samples"):
                        try:
                            result = future.result()
                            if 'error' not in result:
                                results.append(result)
                                
                                # Log the individual evaluation
                                with mlflow.start_run(nested=True, run_name=f"Sample_{result['sample']}"):
                                    # Log parameters
                                    params = {k.replace('param_', ''): v for k, v in result.items() 
                                             if k.startswith('param_')}
                                    mlflow.log_params(params)
                                    
                                    # Log metrics
                                    metrics = {k.replace('metric_', ''): v for k, v in result.items() 
                                              if k.startswith('metric_')}
                                    mlflow.log_metrics(metrics)
                                    
                                    # Log overall harmonic mean
                                    if 'overall_harmonic_mean' in result:
                                        mlflow.log_metric('overall_harmonic_mean', result['overall_harmonic_mean'])
                        except Exception as e:
                            logger.error(f"Error processing result: {e}", exc_info=True)
            else:
                # Sequential processing
                for i, params in enumerate(tqdm(samples, desc="Evaluating samples")):
                    result = self._evaluate_sample(i, params)
                    if 'error' not in result:
                        results.append(result)
                        
                        # Log the individual evaluation
                        with mlflow.start_run(nested=True, run_name=f"Sample_{i}"):
                            # Log parameters
                            param_dict = {k.replace('param_', ''): v for k, v in result.items() 
                                         if k.startswith('param_')}
                            mlflow.log_params(param_dict)
                            
                            # Log metrics
                            metric_dict = {k.replace('metric_', ''): v for k, v in result.items() 
                                          if k.startswith('metric_')}
                            mlflow.log_metrics(metric_dict)
                            
                            # Log overall harmonic mean
                            if 'overall_harmonic_mean' in result:
                                mlflow.log_metric('overall_harmonic_mean', result['overall_harmonic_mean'])
            
            # Create results DataFrame
            if not results:
                logger.error("No valid sensitivity results obtained")
                return pd.DataFrame(), pd.DataFrame()
                
            sensitivity_df = pd.DataFrame(results)
            
            # Extract parameter and metric columns
            param_columns = [col for col in sensitivity_df.columns if col.startswith('param_')]
            metric_columns = [col for col in sensitivity_df.columns if col.startswith('metric_')]
            if 'overall_harmonic_mean' in sensitivity_df.columns:
                metric_columns.append('overall_harmonic_mean')
            
            # Calculate correlation between parameters and metrics
            impact_rows = []
            
            for param_col in param_columns:
                param_name = param_col.replace('param_', '')
                impact_row = {'parameter': param_name}
                
                # Check if parameter has variation (required for correlation)
                if sensitivity_df[param_col].nunique() <= 1:
                    logger.info(f"Parameter '{param_name}' has no variation, skipping correlation analysis")
                    continue
                
                for metric_col in metric_columns:
                    # Calculate correlation, using Spearman for robustness to non-linearity
                    try:
                        corr, p_value = spearmanr(sensitivity_df[param_col], sensitivity_df[metric_col])
                        metric_name = metric_col.replace('metric_', '')
                        impact_row[metric_name] = corr
                        impact_row[f'{metric_name}_p_value'] = p_value
                    except Exception as e:
                        logger.warning(f"Error calculating correlation for {param_name} and {metric_col}: {e}")
                
                impact_rows.append(impact_row)
            
            # Create parameter impact DataFrame
            param_impact_df = pd.DataFrame(impact_rows).set_index('parameter')
            
            # Save results as artifacts
            sensitivity_df.to_csv("sensitivity_results.csv", index=False)
            param_impact_df.to_csv("parameter_impact.csv")
            
            mlflow.log_artifact("sensitivity_results.csv")
            mlflow.log_artifact("parameter_impact.csv")
            
            return sensitivity_df, param_impact_df