# trading_system/src/collectors/data_collector.py

import logging
from src.database.config import DatabaseConfig
from src.database.engine import create_db_engine
from src.collectors.price_collector import PriceCollector
from src.collectors.info_collector import InfoCollector
from src.collectors.statements_collector import StatementsCollector
from datetime import datetime
import random
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def select_tickers_for_refresh(all_tickers, day_of_month):
    """
    Select a portion of tickers to refresh based on the current day of the month.

    The function ensures that on each day, 1/30th of the tickers are chosen for refresh.
    
    Args:
        all_tickers (list): List of all tickers available.
        day_of_month (int): The current day of the month (1-31).
    
    Returns:
        list: List of selected tickers to refresh for the day.
    """
    random.seed(day_of_month)  # Ensure consistent selection per day
    tickers_per_day = len(all_tickers) // 30 or 1  # Ensure at least one ticker per day
    selected_tickers = random.sample(all_tickers, tickers_per_day)
    return selected_tickers

def refresh_data_for_ticker(ticker, collectors):
    """
    Refresh data for a specific ticker by invoking the corresponding collector methods.
    
    Args:
        ticker (str): The ticker symbol.
        collectors (dict): A dictionary of collector instances to handle different data refresh tasks.
    """
    try:
        # Refresh price data
        collectors['price'].refresh_data(ticker)

        # Refresh company info
        collectors['info'].refresh_data(ticker)

        logger.info(f"Successfully refreshed data for ticker {ticker}.")
    except Exception as e:
        logger.error(f"Error refreshing data for ticker {ticker}: {e}")

def main(all_tickers, batch_size: int = 5):
    """Main function to orchestrate the data collection process."""

    # Use the default configuration for SQLite
    db_config = DatabaseConfig.default()
    db_engine = create_db_engine(db_config)

    # Select tickers to refresh based on the current day of the month
    today = datetime.now().day
    tickers_to_refresh = select_tickers_for_refresh(all_tickers, today)

    logger.info(f"Refreshing data for tickers: {tickers_to_refresh}")

    # Initialize collectors
    price_collector = PriceCollector(db_engine)
    info_collector = InfoCollector(db_engine)
    statements_collector = StatementsCollector(db_engine)
    collectors = {
        'price': price_collector,
        'info': info_collector
    }


    # ThreadPoolExecutor to handle concurrent refresh tasks
    try:
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Refresh data concurrently for selected tickers
            futures = []
            for ticker in tickers_to_refresh:
                future = executor.submit(refresh_data_for_ticker, ticker, collectors)
                futures.append(future)

            # Wait for all futures to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing ticker: {e}")

        # Process financial statements for each ticker
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = []
            for statement_type, fetch_func in statements_collector.financial_statements:
                for ticker in all_tickers:
                    future = executor.submit(
                        statements_collector.fetch_financial_statement,
                        ticker,
                        statement_type,
                        fetch_func
                    )
                    futures.append(future)

            # Wait for financial statement processing to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing financial statement: {e}")

        # Process daily prices for all tickers
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = []
            for ticker in all_tickers:
                future = executor.submit(price_collector.fetch_price_data, ticker)
                futures.append(future)

            # Wait for all price futures to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing price data: {e}")

        logger.info(" Price data collection process completed successfully.")

        # Process company info for all tickers
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = []
            for ticker in all_tickers:
                future = executor.submit(info_collector.fetch_company_info, ticker)
                futures.append(future)

            # Wait for all company info fetch to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing company info: {e}")

        logger.info("Company info collection process completed successfully.")


    except Exception as e:
        logger.error(f"Unexpected error in the main process: {e}")
        raise

if __name__ == "__main__":
    main()
