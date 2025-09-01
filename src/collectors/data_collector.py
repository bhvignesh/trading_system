# trading_system/src/collectors/data_collector.py

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List

from src.collectors.info_collector import InfoCollector
from src.collectors.price_collector import PriceCollector
from src.collectors.statements_collector import StatementsCollector
from src.database.config import DatabaseConfig
from src.database.engine import create_db_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCollectionOrchestrator:
    """
    Orchestrates systematic data collection with state tracking and performance optimization.
    
    Features:
    - Deterministic 30-day refresh cycles with state persistence
    - Concurrent processing with rate limiting
    - Error resilience and retry mechanisms
    - Performance monitoring and logging
    """
    
    def __init__(self, db_config: DatabaseConfig = None, max_workers: int = 5):
        self.db_config = db_config or DatabaseConfig.default()
        self.db_engine = create_db_engine(self.db_config)
        self.max_workers = max_workers
        
        # Initialize collectors
        self.price_collector = PriceCollector(self.db_engine)
        self.info_collector = InfoCollector(self.db_engine)
        self.statements_collector = StatementsCollector(self.db_engine)
        
    def run_collection_cycle(self, all_tickers: List[str]) -> Dict[str, Any]:
        """
        Execute a complete data collection cycle.
        
        Args:
            all_tickers: List of all tickers to process
            
        Returns:
            Dictionary with collection statistics and results
        """
        start_time = time.time()
        today = datetime.now()
        cycle_day = today.day
        
        logger.info(f"Starting data collection cycle for day {cycle_day}")
        
        # Get tickers for full refresh based on deterministic cycle
        refresh_tickers = self.price_collector.get_tickers_for_refresh_cycle(all_tickers, cycle_day)
        
        logger.info(f"Processing {len(all_tickers)} tickers: {len(refresh_tickers)} for refresh, "
                   f"{len(all_tickers) - len(refresh_tickers)} for incremental updates")
        
        results = {
            'start_time': today,
            'cycle_day': cycle_day,
            'total_tickers': len(all_tickers),
            'refresh_tickers': len(refresh_tickers),
            'incremental_tickers': len(all_tickers) - len(refresh_tickers),
            'success_counts': {'price': 0, 'info': 0, 'statements': 0},
            'error_counts': {'price': 0, 'info': 0, 'statements': 0},
            'errors': []
        }
        
        try:
            # Phase 1: Full refresh for selected tickers
            if refresh_tickers:
                logger.info("Phase 1: Full refresh operations")
                self._process_full_refresh(refresh_tickers, results)
            
            # Phase 2: Incremental price updates for all tickers
            logger.info("Phase 2: Incremental price updates")
            self._process_incremental_prices(all_tickers, results)
            
            # Phase 3: Company info updates (less frequent)
            logger.info("Phase 3: Company info updates")
            self._process_company_info(all_tickers, results)
            
            # Phase 4: Financial statements (quarterly updates)
            logger.info("Phase 4: Financial statements updates")
            self._process_financial_statements(all_tickers, results)
            
        except Exception as e:
            logger.error(f"Critical error in collection cycle: {e}")
            results['errors'].append(f"Critical error: {str(e)}")
        
        # Calculate final statistics
        end_time = time.time()
        results['duration'] = end_time - start_time
        results['end_time'] = datetime.now()
        
        self._log_collection_summary(results)
        return results
    
    def _process_full_refresh(self, tickers: List[str], results: Dict):
        """Process full refresh for selected tickers."""
        
        def refresh_ticker_data(ticker):
            errors = []
            successes = []
            
            try:
                # Refresh price data
                self.price_collector.refresh_data(ticker)
                successes.append('price')
                
                # Update refresh state
                self.price_collector.update_refresh_state(ticker, datetime.now(), datetime.now().day)
                
            except Exception as e:
                error_msg = f"Price refresh failed for {ticker}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
            
            try:
                # Refresh company info
                self.info_collector.refresh_data(ticker)
                successes.append('info')
                
            except Exception as e:
                error_msg = f"Info refresh failed for {ticker}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
            
            return ticker, successes, errors
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(refresh_ticker_data, ticker): ticker 
                      for ticker in tickers}
            
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    ticker, successes, errors = future.result()
                    
                    for success_type in successes:
                        results['success_counts'][success_type] += 1
                    
                    for error in errors:
                        results['errors'].append(error)
                        # Count errors by type
                        if 'price' in error.lower():
                            results['error_counts']['price'] += 1
                        elif 'info' in error.lower():
                            results['error_counts']['info'] += 1
                            
                except Exception as e:
                    error_msg = f"Unexpected error processing {ticker}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
    
    def _process_incremental_prices(self, tickers: List[str], results: Dict):
        """Process incremental price updates for all tickers."""
        
        def fetch_incremental_prices(ticker):
            try:
                self.price_collector.fetch_and_save(ticker)
                return ticker, True, None
            except Exception as e:
                error_msg = f"Incremental price fetch failed for {ticker}: {str(e)}"
                logger.error(error_msg)
                return ticker, False, error_msg
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_incremental_prices, ticker): ticker 
                      for ticker in tickers}
            
            for future in as_completed(futures):
                try:
                    ticker, success, error = future.result()
                    if success:
                        results['success_counts']['price'] += 1
                    else:
                        results['error_counts']['price'] += 1
                        if error:
                            results['errors'].append(error)
                except Exception as e:
                    ticker = futures[future]
                    error_msg = f"Unexpected error in incremental price fetch for {ticker}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
    
    def _process_company_info(self, tickers: List[str], results: Dict):
        """Process company info updates (less frequent than price data)."""
        
        def fetch_company_info(ticker):
            try:
                self.info_collector.fetch_and_save(ticker)
                return ticker, True, None
            except Exception as e:
                error_msg = f"Company info fetch failed for {ticker}: {str(e)}"
                logger.error(error_msg)
                return ticker, False, error_msg
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_company_info, ticker): ticker 
                      for ticker in tickers}
            
            for future in as_completed(futures):
                try:
                    ticker, success, error = future.result()
                    if success:
                        results['success_counts']['info'] += 1
                    else:
                        results['error_counts']['info'] += 1
                        if error:
                            results['errors'].append(error)
                except Exception as e:
                    ticker = futures[future]
                    error_msg = f"Unexpected error in company info fetch for {ticker}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
    
    def _process_financial_statements(self, tickers: List[str], results: Dict):
        """Process financial statements updates."""
        
        def fetch_financial_statements(ticker):
            errors = []
            successes = 0
            
            for statement_type, fetch_func in self.statements_collector.financial_statements:
                try:
                    self.statements_collector.fetch_financial_statement(
                        ticker, statement_type, fetch_func
                    )
                    successes += 1
                except Exception as e:
                    error_msg = f"Financial statement {statement_type} failed for {ticker}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)
            
            return ticker, successes, errors
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_financial_statements, ticker): ticker 
                      for ticker in tickers}
            
            for future in as_completed(futures):
                try:
                    ticker, successes, errors = future.result()
                    results['success_counts']['statements'] += successes
                    results['error_counts']['statements'] += len(errors)
                    results['errors'].extend(errors)
                except Exception as e:
                    ticker = futures[future]
                    error_msg = f"Unexpected error in financial statements for {ticker}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
    
    def _log_collection_summary(self, results: Dict):
        """Log comprehensive collection summary."""
        logger.info("="*80)
        logger.info("DATA COLLECTION CYCLE SUMMARY")
        logger.info("="*80)
        logger.info(f"Duration: {results['duration']:.2f} seconds")
        logger.info(f"Total tickers processed: {results['total_tickers']}")
        logger.info(f"Refresh operations: {results['refresh_tickers']}")
        logger.info(f"Incremental operations: {results['incremental_tickers']}")
        logger.info("")
        logger.info("SUCCESS COUNTS:")
        for operation, count in results['success_counts'].items():
            logger.info(f"  {operation.capitalize()}: {count}")
        logger.info("")
        logger.info("ERROR COUNTS:")
        for operation, count in results['error_counts'].items():
            logger.info(f"  {operation.capitalize()}: {count}")
        
        if results['errors']:
            logger.info(f"\nFirst 5 errors:")
            for error in results['errors'][:5]:
                logger.info(f"  - {error}")
        
        logger.info("="*80)

def main(all_tickers: List[str]):
    """
    Main entry point for data collection.
    
    Args:
        all_tickers: List of all ticker symbols to process
    """
    orchestrator = DataCollectionOrchestrator()
    results = orchestrator.run_collection_cycle(all_tickers)
    
    # Return results for monitoring/alerting systems
    return results

# if __name__ == "__main__":
#     # Example usage
#     sample_tickers = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]  # Replace with your ticker list
#     main(sample_tickers)