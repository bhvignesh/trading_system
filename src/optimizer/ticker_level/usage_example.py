# trading_system/src/optimizer/usage_example.py

"""
Usage Example:
  Demonstrates how to initialize the StrategyOptimizer and SensitivityAnalyzer,
  run the hyperparameter search, and log everything using mlflow.
"""

import mlflow
from hyperopt import hp

from optimizer.ticker_level.sensitivity_analyzer import SensitivityAnalyzer
from optimizer.ticker_level.strategy_optimizer import StrategyOptimizer
from src.strategies.momentum.awesome_oscillator_strat import \
    AwesomeOscillatorStrategy

# Example configuration
db_config = {"host": "localhost", "port": 1234}
example_search_space = {
    # Example search space for a momentum strategy. Adapt as needed.
    'window_size': hp.quniform('window_size', 5, 30, 1),
    'threshold': hp.uniform('threshold', 0.0, 1.0)
}

# Define tickers and optional weights.
tickers = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'META']
ticker_weights = {
    'AAPL': 0.3,
    'GOOG': 0.2,
    'MSFT': 0.2,
    'AMZN': 0.2,
    'META': 0.1
}

with mlflow.start_run(run_name="UsageExample"):
    # Initialize the optimizer.
    optimizer = StrategyOptimizer(
        strategy_class=AwesomeOscillatorStrategy,  # Replace with your strategy
        db_config=db_config,
        search_space=example_search_space,
        tickers=tickers,
        ticker_weights=ticker_weights,
        start_date="2020-01-01",
        end_date="2022-12-31",
        max_evals=50
    )

    # Run the hyperparameter optimization.
    best_params, performance_report, param_history = optimizer.run_optimization()
    print("Best Parameters:")
    print(best_params)
    print("\nPerformance Report:")
    print(performance_report)

    # Run sensitivity analysis.
    sensitivity_analyzer = SensitivityAnalyzer(
        strategy_optimizer=optimizer,
        base_params=best_params,
        num_samples=30
    )
    sensitivity_results, parameter_impact = sensitivity_analyzer.run()
    print("\nParameter Impact Analysis:")
    print(parameter_impact)