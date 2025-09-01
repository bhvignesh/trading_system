# trading_system/src/strategies/value_strategy/data_manager.py

"""
Data retrieval and management for value strategy implementation.
Handles caching and efficient data retrieval.
"""

import logging
from datetime import timedelta
from typing import Dict, List, Union

import pandas as pd
from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class FinancialDataManager:
    """
    Financial data retrieval and management class.
    
    Handles efficient retrieval, caching, and alignment of financial data
    from various sources including balance sheets, income statements,
    cash flow statements, and price data.
    """
    
    def __init__(self, strategy: BaseStrategy):
        """
        Initialize the financial data manager.
        
        Args:
            strategy (BaseStrategy): The parent strategy instance used for data retrieval.
        """
        self.strategy = strategy
        self.logger = logging.getLogger(self.__class__.__name__)
        self._cache = {}
    
    def clear_cache(self):
        """Clear all cached data."""
        self._cache = {}
        self.logger.debug("Data cache cleared")
    
    def get_financial_data(self, tickers: Union[str, List[str]], 
                           include_prices: bool = True) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Get all financial data for one or more tickers.
        
        Args:
            tickers (Union[str, List[str]]): Ticker symbol(s).
            include_prices (bool): Whether to include price data aligned with financial dates.
            
        Returns:
            Dict[str, Dict[str, pd.DataFrame]]: Dictionary with financial data by ticker.
        """
        # Convert single ticker to list
        if isinstance(tickers, str):
            tickers_list = [tickers]
        else:
            tickers_list = tickers
        
        result = {}
        for ticker in tickers_list:
            cache_key = f"all_financial_data_{ticker}"
            
            if cache_key in self._cache:
                result[ticker] = self._cache[cache_key]
                continue
            
            try:
                # Get basic financial data
                ticker_data = {
                    'balance_sheet': self.strategy.get_financials(ticker, 'balance_sheet'),
                    'income_stmt': self.strategy.get_financials(ticker, 'income_statement'),
                    'cash_flow': self.strategy.get_financials(ticker, 'cash_flow')
                }
                
                # Get company info
                try:
                    ticker_data['company_info'] = self.strategy.get_company_info(ticker)
                except DataRetrievalError:
                    self.logger.warning(f"Company info not found for {ticker}")
                    ticker_data['company_info'] = pd.Series()
                
                # Find all unique dates from financial statements
                all_fin_dates = pd.DatetimeIndex(pd.concat([
                    pd.Series(ticker_data['balance_sheet'].index),
                    pd.Series(ticker_data['income_stmt'].index),
                    pd.Series(ticker_data['cash_flow'].index)
                ]).unique()).sort_values()
                
                ticker_data['fin_dates'] = all_fin_dates
                
                # Get price data aligned with financial dates if requested
                if include_prices:
                    if not all_fin_dates.empty:
                        from_date = (all_fin_dates.min() - timedelta(days=5)).strftime('%Y-%m-%d')
                        to_date = (all_fin_dates.max() + timedelta(days=5)).strftime('%Y-%m-%d')
                        
                        # Get prices around financial dates
                        try:
                            prices = self.strategy.get_historical_prices(
                                ticker,
                                data_source='yfinance',
                                from_date=from_date,
                                to_date=to_date
                            )
                            
                            # Map each financial date to the closest valid trading date
                            date_map = {}
                            for fin_date in all_fin_dates:
                                # Find closest future price date (within 30 days)
                                future_dates = prices.index[prices.index >= fin_date]
                                if not future_dates.empty:
                                    closest_date = future_dates[0]
                                    if (closest_date - fin_date).days <= 30:
                                        date_map[fin_date] = closest_date
                                
                                # If no future date found, try past dates
                                if fin_date not in date_map:
                                    past_dates = prices.index[prices.index <= fin_date]
                                    if not past_dates.empty:
                                        closest_date = past_dates[-1]
                                        if (fin_date - closest_date).days <= 30:
                                            date_map[fin_date] = closest_date
                            
                            # Create aligned prices with financial dates
                            aligned_prices = pd.DataFrame(index=date_map.keys())
                            for fin_date, price_date in date_map.items():
                                aligned_prices.loc[fin_date] = prices.loc[price_date]
                            
                            ticker_data['prices'] = prices
                            ticker_data['aligned_prices'] = aligned_prices
                            ticker_data['date_map'] = date_map
                        except Exception as e:
                            self.logger.warning(f"Error retrieving price data for {ticker}: {e}")
                            ticker_data['prices'] = pd.DataFrame()
                            ticker_data['aligned_prices'] = pd.DataFrame()
                            ticker_data['date_map'] = {}
                    else:
                        ticker_data['prices'] = pd.DataFrame()
                        ticker_data['aligned_prices'] = pd.DataFrame()
                        ticker_data['date_map'] = {}
                
                # Cache the result
                self._cache[cache_key] = ticker_data
                result[ticker] = ticker_data
                
            except Exception as e:
                self.logger.error(f"Error retrieving financial data for {ticker}: {e}")
                result[ticker] = {
                    'balance_sheet': pd.DataFrame(),
                    'income_stmt': pd.DataFrame(),
                    'cash_flow': pd.DataFrame(),
                    'company_info': pd.Series(),
                    'prices': pd.DataFrame(),
                    'aligned_prices': pd.DataFrame(),
                    'fin_dates': pd.DatetimeIndex([]),
                    'date_map': {}
                }
        
        # If only one ticker was requested, return just that ticker's data
        if isinstance(tickers, str):
            return result[tickers]
        
        return result