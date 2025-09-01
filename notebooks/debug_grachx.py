"""
Main script to run hyperparameter optimization and sensitivity analysis
for the Know Sure Thing strategy using the portfolio-based evaluation framework.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.getcwd()))

try:
    from src.database.config import DatabaseConfig
    from src.strategies.advanced.garchx_strat import GarchXStrategyStrategy
    from utils.file_utils import load_tickers_from_yaml
except ImportError as e:
    print("Error importing modules. Make sure the script is run from the project root")
    print("or the 'src' directory is in the Python path.")
    print(f"Import Error: {e}")
    sys.exit(1)

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Data Configuration
TICKER_FILE_PATH = "data/tickers.yml"  # Path relative to project root
MAX_TICKERS = 5  # Limit tickers for faster testing, set to None to use all

# Backtest Period
START_DATE = (datetime.now() - timedelta(days=4 * 365)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

tickers_to_run = load_tickers_from_yaml(TICKER_FILE_PATH, MAX_TICKERS)

tickers_to_run = tickers_to_run[:10]

db_config = DatabaseConfig.default()

cc = GarchXStrategyStrategy(db_config=db_config)

b = cc.generate_signals(ticker=tickers_to_run, start_date=START_DATE, end_date=END_DATE)
