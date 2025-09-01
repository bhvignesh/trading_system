# trading_system/src/strategies/value_strategy_screener.py

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class ValueStrategyScreener(BaseStrategy):
    """
    Implementation of various value investing strategies and screeners.
    
    Strategies implemented:
    1. Magic Formula (Joel Greenblatt)
    2. Acquirer's Multiple (Tobias Carlisle)
    3. Deep Value metrics (P/E, P/B, Dividend Yield)
    4. Graham's Defensive Investor criteria (Benjamin Graham)
    5. Piotroski F-Score (Joseph Piotroski)
    6. Dividend Yield screening
    
    This class can calculate individual metrics or generate
    a combined value score based on multiple strategies.
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict[str, Any]] = None):
        """
        Initialize the value strategy screener.
        
        Args:
            db_config (DatabaseConfig): Database configuration settings.
            params (dict, optional): Strategy-specific parameters including:
                - lookback_years (int): Years of financial data to analyze
                - min_market_cap (float): Minimum market capitalization in millions
                - max_market_cap (float): Maximum market capitalization in millions
                - min_liquidity (float): Minimum average daily trading volume
                - exclude_sectors (List[str]): Sectors to exclude
                - weight_magic_formula (float): Weight for Magic Formula in combined score
                - weight_acquirers_multiple (float): Weight for Acquirer's Multiple
                - weight_deep_value (float): Weight for Deep Value metrics
                - weight_graham (float): Weight for Graham criteria
                - weight_piotroski (float): Weight for F-Score
                - weight_dividend (float): Weight for Dividend metrics
        """
        # Default parameters suitable for Indian markets
        default_params = {
            'lookback_years': 5,
            'min_market_cap': 500,  # 500 crores (~$60M)
            'max_market_cap': None,  # No upper limit
            'min_liquidity': 100000,  # Minimum daily volume
            'exclude_sectors': ['Financial Services'],
            'weight_magic_formula': 1.0,
            'weight_acquirers_multiple': 1.0,
            'weight_deep_value': 1.0,
            'weight_graham': 1.0,
            'weight_piotroski': 1.0,
            'weight_dividend': 0.5,  # Lower weight as not all companies pay dividends in India
        }
        
        # Update default params with any provided params
        if params:
            default_params.update(params)
            
        super().__init__(db_config, default_params)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_financial_data(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """
        Get all necessary financial data for a ticker.
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with financial statement DataFrames
        """
        try:
            # Get balance sheet data
            balance_sheet = self.get_financials(ticker, 'balance_sheet')
            
            # Get income statement data
            income_stmt = self.get_financials(ticker, 'income_statement')
            
            # Get cash flow data
            cash_flow = self.get_financials(ticker, 'cash_flow')
            
            # Get company info
            company_info = self.get_company_info(ticker)
            
            # Get historical prices for financial statement dates
            # First, find all unique dates from financial statements
            all_fin_dates = pd.to_datetime(pd.concat([
                balance_sheet.index, 
                income_stmt.index,
                cash_flow.index
            ]).unique()).sort_values()
            
            if all_fin_dates.empty:
                self.logger.warning(f"No financial statement dates found for {ticker}")
                return {}
            
            # Get price data for these dates plus a buffer
            from_date = all_fin_dates.min() - pd.Timedelta(days=5)
            to_date = all_fin_dates.max() + pd.Timedelta(days=5)
            
            prices = self.get_historical_prices(
                ticker, 
                data_source='yfinance',
                from_date=from_date.strftime('%Y-%m-%d'),
                to_date=to_date.strftime('%Y-%m-%d')
            )
            
            # For each financial date, find the closest valid trading date
            date_map = {}
            for fin_date in all_fin_dates:
                closest_date = None
                closest_diff = pd.Timedelta(days=365)  # Start with a large difference
                
                for price_date in prices.index:
                    if price_date >= fin_date:
                        diff = price_date - fin_date
                        if diff < closest_diff:
                            closest_date = price_date
                            closest_diff = diff
                        
                # Only use dates within 30 days of the financial date
                if closest_date is not None and closest_diff <= pd.Timedelta(days=30):
                    date_map[fin_date] = closest_date
            
            # Create aligned prices with financial dates
            aligned_prices = prices.loc[[date_map[d] for d in date_map.keys() if d in date_map]].copy()
            aligned_prices.index = list(date_map.keys())
            
            return {
                'balance_sheet': balance_sheet,
                'income_stmt': income_stmt,
                'cash_flow': cash_flow,
                'company_info': company_info,
                'prices': aligned_prices,
                'date_map': date_map
            }
        
        except DataRetrievalError as e:
            self.logger.error(f"Error retrieving financial data for {ticker}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error processing financial data for {ticker}: {e}")
            return {}

    def _calculate_ev(self, financials: Dict[str, pd.DataFrame], date: pd.Timestamp) -> float:
        """
        Calculate Enterprise Value for a specific date.
        
        Enterprise Value = Market Cap + Total Debt - Cash and Cash Equivalents
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            date (pd.Timestamp): Date to calculate EV for
            
        Returns:
            float: Enterprise Value
        """
        try:
            balance_sheet = financials['balance_sheet']
            prices = financials['prices']
            company_info = financials['company_info']
            
            # Get the market cap
            if date in prices.index:
                price = prices.loc[date, 'close']
                # Try to get shares outstanding from balance sheet first
                shares_outstanding = None
                
                # Look for shares outstanding in balance sheet
                share_columns = [
                    'ordinary_shares_number', 
                    'share_issued'
                ]
                
                for col in share_columns:
                    if col in balance_sheet.columns and date in balance_sheet.index:
                        if not pd.isna(balance_sheet.loc[date, col]) and balance_sheet.loc[date, col] > 0:
                            shares_outstanding = balance_sheet.loc[date, col]
                            break
                
                # If not found in balance sheet, try company_info
                if shares_outstanding is None and 'sharesoutstanding' in company_info:
                    shares_outstanding = company_info['sharesoutstanding']
                
                # Calculate market cap
                if shares_outstanding is not None and shares_outstanding > 0:
                    market_cap = price * shares_outstanding
                else:
                    # Try to use market cap from company_info
                    if 'marketcap' in company_info and not pd.isna(company_info['marketcap']):
                        market_cap = company_info['marketcap']
                    else:
                        self.logger.warning(f"Cannot calculate market cap for {date}, missing shares outstanding")
                        return None
            else:
                self.logger.warning(f"No price data available for {date}")
                return None
            
            # Get total debt
            total_debt = 0
            if date in balance_sheet.index:
                debt_columns = [
                    'total_debt',
                    'long_term_debt',
                    'current_debt'
                ]
                
                for col in debt_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        total_debt = balance_sheet.loc[date, col]
                        break
            
            # Get cash and cash equivalents
            cash = 0
            if date in balance_sheet.index:
                cash_columns = [
                    'cash_and_cash_equivalents',
                    'cash_financial',
                    'cash_cash_equivalents_and_short_term_investments'
                ]
                
                for col in cash_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        cash = balance_sheet.loc[date, col]
                        break
            
            # Calculate Enterprise Value
            enterprise_value = market_cap + total_debt - cash
            return enterprise_value
        
        except Exception as e:
            self.logger.error(f"Error calculating Enterprise Value: {e}")
            return None
        
    # Magic Formula methods
    def calculate_magic_formula(self, ticker: str) -> pd.DataFrame:
        """
        Calculate Magic Formula metrics (Earnings Yield and Return on Capital).
        
        The Magic Formula consists of two key metrics:
        1. Earnings Yield = EBIT / Enterprise Value
        2. Return on Capital = EBIT / (Net Working Capital + Net Fixed Assets)
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with Magic Formula metrics for each date.
        """
        try:
            # Get financial data
            financials = self._get_financial_data(ticker)
            if not financials:
                return pd.DataFrame()
            
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            
            # Find common dates between balance sheet and income statement
            common_dates = balance_sheet.index.intersection(income_stmt.index)
            if common_dates.empty:
                self.logger.warning(f"No common dates between balance sheet and income statement for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=common_dates)
            
            # Calculate metrics for each date
            for date in common_dates:
                # Get EBIT
                ebit = None
                ebit_columns = ['ebit', 'operating_income']
                for col in ebit_columns:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        ebit = income_stmt.loc[date, col]
                        break
                
                if ebit is None or ebit <= 0:
                    self.logger.warning(f"Invalid EBIT for {ticker} on {date}")
                    continue
                
                # Calculate Enterprise Value
                ev = self._calculate_ev(financials, date)
                if ev is None or ev <= 0:
                    self.logger.warning(f"Invalid Enterprise Value for {ticker} on {date}")
                    continue
                
                # Calculate Net Working Capital
                current_assets = None
                if 'current_assets' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_assets']):
                    current_assets = balance_sheet.loc[date, 'current_assets']
                
                current_liabilities = None
                if 'current_liabilities' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_liabilities']):
                    current_liabilities = balance_sheet.loc[date, 'current_liabilities']
                
                if current_assets is not None and current_liabilities is not None:
                    net_working_capital = current_assets - current_liabilities
                else:
                    # Alternative calculation
                    net_working_capital = balance_sheet.loc[date, 'working_capital'] if 'working_capital' in balance_sheet.columns else 0
                
                # Get Net Fixed Assets (Net PPE)
                net_fixed_assets = None
                if 'net_ppe' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'net_ppe']):
                    net_fixed_assets = balance_sheet.loc[date, 'net_ppe']
                else:
                    self.logger.warning(f"Net PPE not found for {ticker} on {date}")
                    continue
                
                # Calculate invested capital
                invested_capital = net_working_capital + net_fixed_assets
                if invested_capital <= 0:
                    self.logger.warning(f"Invalid invested capital for {ticker} on {date}")
                    continue
                
                # Calculate Earnings Yield
                earnings_yield = ebit / ev
                
                # Calculate Return on Capital
                return_on_capital = ebit / invested_capital
                
                # Store results
                result.loc[date, 'EBIT'] = ebit
                result.loc[date, 'Enterprise_Value'] = ev
                result.loc[date, 'Net_Working_Capital'] = net_working_capital
                result.loc[date, 'Net_Fixed_Assets'] = net_fixed_assets
                result.loc[date, 'Earnings_Yield'] = earnings_yield
                result.loc[date, 'Return_on_Capital'] = return_on_capital
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Magic Formula metrics for {ticker}: {e}")
            return pd.DataFrame()

    def rank_by_magic_formula(self, tickers: List[str]) -> pd.DataFrame:
        """
        Rank stocks according to the Magic Formula methodology.
        
        The Magic Formula ranks stocks by a combined score of Earnings Yield and Return on Capital.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with Magic Formula rankings.
        """
        try:
            # Calculate Magic Formula metrics for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_magic_formula(ticker)
                if not metrics.empty:
                    # Use the most recent data
                    all_metrics[ticker] = metrics.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid metrics found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Remove rows with missing data
            df = df.dropna(subset=['Earnings_Yield', 'Return_on_Capital'])
            
            # Rank by Earnings Yield (higher is better)
            df['EY_Rank'] = df['Earnings_Yield'].rank(ascending=False)
            
            # Rank by Return on Capital (higher is better)
            df['ROC_Rank'] = df['Return_on_Capital'].rank(ascending=False)
            
            # Calculate Combined Rank (lower is better)
            df['Combined_Rank'] = df['EY_Rank'] + df['ROC_Rank']
            
            # Sort by Combined Rank
            df = df.sort_values('Combined_Rank')
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error ranking by Magic Formula: {e}")
            return pd.DataFrame()
        
    # Acquirer's Multiple methods
    def calculate_acquirers_multiple(self, ticker: str) -> pd.DataFrame:
        """
        Calculate the Acquirer's Multiple (EV/EBIT).
        
        The Acquirer's Multiple is defined as:
        Enterprise Value / Operating Earnings (EBIT)
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with Acquirer's Multiple for each date.
        """
        try:
            # Get financial data
            financials = self._get_financial_data(ticker)
            if not financials:
                return pd.DataFrame()
            
            income_stmt = financials['income_stmt']
            
            # Find dates with income statement data
            if income_stmt.empty:
                self.logger.warning(f"No income statement data for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=income_stmt.index)
            
            # Calculate metrics for each date
            for date in income_stmt.index:
                # Get EBIT
                ebit = None
                ebit_columns = ['ebit', 'operating_income']
                for col in ebit_columns:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        ebit = income_stmt.loc[date, col]
                        break
                
                if ebit is None or ebit <= 0:
                    self.logger.warning(f"Invalid EBIT for {ticker} on {date}")
                    continue
                
                # Calculate Enterprise Value
                ev = self._calculate_ev(financials, date)
                if ev is None or ev <= 0:
                    self.logger.warning(f"Invalid Enterprise Value for {ticker} on {date}")
                    continue
                
                # Calculate Acquirer's Multiple
                acquirers_multiple = ev / ebit
                
                # Store results
                result.loc[date, 'EBIT'] = ebit
                result.loc[date, 'Enterprise_Value'] = ev
                result.loc[date, 'Acquirers_Multiple'] = acquirers_multiple
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Acquirer's Multiple for {ticker}: {e}")
            return pd.DataFrame()

    def rank_by_acquirers_multiple(self, tickers: List[str]) -> pd.DataFrame:
        """
        Rank stocks by the Acquirer's Multiple.
        
        Lower Acquirer's Multiple is better.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with Acquirer's Multiple rankings.
        """
        try:
            # Calculate Acquirer's Multiple for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_acquirers_multiple(ticker)
                if not metrics.empty:
                    # Use the most recent data
                    all_metrics[ticker] = metrics.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid metrics found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Remove rows with missing data
            df = df.dropna(subset=['Acquirers_Multiple'])
            
            # Rank by Acquirer's Multiple (lower is better)
            df['AM_Rank'] = df['Acquirers_Multiple'].rank()
            
            # Sort by Acquirer's Multiple Rank
            df = df.sort_values('AM_Rank')
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error ranking by Acquirer's Multiple: {e}")
            return pd.DataFrame()
        
    # Deep Value metrics
    def calculate_deep_value_metrics(self, ticker: str) -> pd.DataFrame:
        """
        Calculate deep value metrics (P/E, P/B, Dividend Yield).
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with deep value metrics for each date.
        """
        try:
            # Get financial data
            financials = self._get_financial_data(ticker)
            if not financials:
                return pd.DataFrame()
            
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            prices = financials['prices']
            
            # Find common dates between all data sources
            common_dates = balance_sheet.index.intersection(income_stmt.index).intersection(prices.index)
            if common_dates.empty:
                self.logger.warning(f"No common dates between all data sources for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=common_dates)
            
            # Calculate metrics for each date
            for date in common_dates:
                # Get price
                price = prices.loc[date, 'close']
                
                # Calculate P/E Ratio
                earnings = None
                earnings_columns = ['net_income', 'net_income_common_stockholders']
                for col in earnings_columns:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        earnings = income_stmt.loc[date, col]
                        break
                
                shares_outstanding = None
                share_columns = ['ordinary_shares_number', 'share_issued']
                for col in share_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        shares_outstanding = balance_sheet.loc[date, col]
                        break
                
                if earnings is not None and earnings > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    eps = earnings / shares_outstanding
                    pe_ratio = price / eps
                    result.loc[date, 'EPS'] = eps
                    result.loc[date, 'P_E_Ratio'] = pe_ratio
                
                # Calculate P/B Ratio
                book_value = None
                book_columns = ['common_stock_equity', 'stockholders_equity', 'total_equity_gross_minority_interest']
                for col in book_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        book_value = balance_sheet.loc[date, col]
                        break
                
                if book_value is not None and book_value > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    book_value_per_share = book_value / shares_outstanding
                    pb_ratio = price / book_value_per_share
                    result.loc[date, 'Book_Value_Per_Share'] = book_value_per_share
                    result.loc[date, 'P_B_Ratio'] = pb_ratio
                
                # Calculate Dividend Yield
                dividend_paid = None
                dividend_columns = ['cash_dividends_paid', 'common_stock_dividend_paid']
                
                for cf in [financials['cash_flow']]:
                    if date in cf.index:
                        for col in dividend_columns:
                            if col in cf.columns and not pd.isna(cf.loc[date, col]):
                                dividend_paid = abs(cf.loc[date, col])  # Usually recorded as negative
                                break
                
                if dividend_paid is not None and dividend_paid > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    dividend_per_share = dividend_paid / shares_outstanding
                    dividend_yield = dividend_per_share / price
                    result.loc[date, 'Dividend_Per_Share'] = dividend_per_share
                    result.loc[date, 'Dividend_Yield'] = dividend_yield
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating deep value metrics for {ticker}: {e}")
            return pd.DataFrame()

    def rank_by_deep_value(self, tickers: List[str]) -> pd.DataFrame:
        """
        Rank stocks by combined deep value metrics.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with deep value rankings.
        """
        try:
            # Calculate Deep Value metrics for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_deep_value_metrics(ticker)
                if not metrics.empty:
                    # Use the most recent data
                    all_metrics[ticker] = metrics.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid metrics found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Rank by P/E Ratio (lower is better)
            if 'P_E_Ratio' in df.columns:
                df = df.dropna(subset=['P_E_Ratio'])
                df['P_E_Rank'] = df['P_E_Ratio'].rank()
            
            # Rank by P/B Ratio (lower is better)
            if 'P_B_Ratio' in df.columns:
                df = df.dropna(subset=['P_B_Ratio'])
                df['P_B_Rank'] = df['P_B_Ratio'].rank()
            
            # Rank by Dividend Yield (higher is better)
            if 'Dividend_Yield' in df.columns:
                df = df.dropna(subset=['Dividend_Yield'])
                df['Div_Yield_Rank'] = df['Dividend_Yield'].rank(ascending=False)
            
            # Calculate Combined Rank
            rank_columns = [col for col in df.columns if col.endswith('_Rank')]
            if rank_columns:
                df['Combined_Deep_Value_Rank'] = df[rank_columns].sum(axis=1)
                df = df.sort_values('Combined_Deep_Value_Rank')
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error ranking by deep value metrics: {e}")
            return pd.DataFrame()
        
# Graham's Defensive Investor criteria
    def calculate_graham_criteria(self, ticker: str) -> pd.DataFrame:
        """
        Calculate Graham's Defensive Investor criteria.
        
        Benjamin Graham's criteria for defensive investors include:
        1. Adequate size (not implemented as it varies by time period)
        2. Strong financial condition (Current Ratio > 2)
        3. Earnings stability (positive earnings for past 10 years)
        4. Dividend record (uninterrupted payments for 20 years - relaxed here)
        5. Earnings growth (minimum 30% increase in per-share earnings in past 10 years)
        6. Moderate P/E ratio (< 15)
        7. Moderate P/B ratio (< 1.5)
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with Graham criteria for each date.
        """
        try:
            # Get financial data
            financials = self._get_financial_data(ticker)
            if not financials:
                return pd.DataFrame()
            
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            prices = financials['prices']
            
            # Find common dates between all data sources
            common_dates = balance_sheet.index.intersection(income_stmt.index).intersection(prices.index)
            if common_dates.empty:
                self.logger.warning(f"No common dates between all data sources for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=common_dates)
            
            # Calculate metrics for each date
            for date in common_dates:
                result.loc[date, 'Price'] = prices.loc[date, 'close']
                
                # 2. Strong financial condition (Current Ratio > 2)
                current_assets = None
                current_liabilities = None
                
                if 'current_assets' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_assets']):
                    current_assets = balance_sheet.loc[date, 'current_assets']
                
                if 'current_liabilities' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_liabilities']):
                    current_liabilities = balance_sheet.loc[date, 'current_liabilities']
                
                if current_assets is not None and current_liabilities is not None and current_liabilities > 0:
                    current_ratio = current_assets / current_liabilities
                    result.loc[date, 'Current_Ratio'] = current_ratio
                    result.loc[date, 'Strong_Financial_Condition'] = current_ratio > 2.0
                
                # Get earnings and shares for P/E and EPS calculations
                earnings = None
                earnings_columns = ['net_income', 'net_income_common_stockholders']
                for col in earnings_columns:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        earnings = income_stmt.loc[date, col]
                        break
                
                shares_outstanding = None
                share_columns = ['ordinary_shares_number', 'share_issued']
                for col in share_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        shares_outstanding = balance_sheet.loc[date, col]
                        break
                
                # 3. Earnings stability (positive earnings)
                if earnings is not None:
                    result.loc[date, 'Positive_Earnings'] = earnings > 0
                
                # Calculate EPS for P/E ratio
                if earnings is not None and earnings > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    eps = earnings / shares_outstanding
                    result.loc[date, 'EPS'] = eps
                    
                    # 6. Moderate P/E ratio (< 15)
                    pe_ratio = prices.loc[date, 'close'] / eps
                    result.loc[date, 'P_E_Ratio'] = pe_ratio
                    result.loc[date, 'Moderate_P_E'] = pe_ratio < 15
                
                # 7. Moderate P/B ratio (< 1.5)
                book_value = None
                book_columns = ['common_stock_equity', 'stockholders_equity', 'total_equity_gross_minority_interest']
                for col in book_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        book_value = balance_sheet.loc[date, col]
                        break
                
                if book_value is not None and book_value > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    book_value_per_share = book_value / shares_outstanding
                    pb_ratio = prices.loc[date, 'close'] / book_value_per_share
                    result.loc[date, 'P_B_Ratio'] = pb_ratio
                    result.loc[date, 'Moderate_P_B'] = pb_ratio < 1.5
                
                # 4. Dividend record
                dividend_paid = None
                dividend_columns = ['cash_dividends_paid', 'common_stock_dividend_paid']
                
                for cf in [financials['cash_flow']]:
                    if date in cf.index:
                        for col in dividend_columns:
                            if col in cf.columns and not pd.isna(cf.loc[date, col]):
                                dividend_paid = abs(cf.loc[date, col])  # Usually recorded as negative
                                break
                
                result.loc[date, 'Pays_Dividend'] = dividend_paid is not None and dividend_paid > 0
            
            # Calculate the total Graham score for each date
            criteria_columns = [
                'Strong_Financial_Condition',
                'Positive_Earnings',
                'Moderate_P_E',
                'Moderate_P_B',
                'Pays_Dividend'
            ]
            
            # Convert boolean columns to 1/0 and sum
            for col in criteria_columns:
                if col in result.columns:
                    result[col] = result[col].astype(int)
            
            result['Graham_Score'] = result[criteria_columns].sum(axis=1)
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Graham criteria for {ticker}: {e}")
            return pd.DataFrame()

    def screen_by_graham_criteria(self, tickers: List[str]) -> pd.DataFrame:
        """
        Screen stocks using Graham's Defensive Investor criteria.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with Graham criteria screening results.
        """
        try:
            # Calculate Graham criteria for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_graham_criteria(ticker)
                if not metrics.empty:
                    # Use the most recent data
                    all_metrics[ticker] = metrics.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid metrics found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Sort by Graham Score (descending)
            if 'Graham_Score' in df.columns:
                df = df.sort_values('Graham_Score', ascending=False)
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error screening by Graham criteria: {e}")
            return pd.DataFrame()

# Piotroski F-Score
    def calculate_piotroski_fscore(self, ticker: str) -> pd.DataFrame:
        """
        Calculate the Piotroski F-Score (9-point scale).
        
        The F-Score consists of 9 binary criteria:
        
        Profitability:
        1. Positive Return on Assets (ROA)
        2. Positive Operating Cash Flow
        3. Improving ROA
        4. Cash Flow > ROA (accrual)
        
        Leverage/Liquidity:
        5. Decreasing Leverage (LT Debt/Assets)
        6. Increasing Current Ratio
        7. No new shares issued
        
        Operating Efficiency:
        8. Increasing Gross Margin
        9. Increasing Asset Turnover
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with F-Score components and total for each date.
        """
        try:
            # Get financial data
            financials = self._get_financial_data(ticker)
            if not financials:
                return pd.DataFrame()
            
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            cash_flow = financials['cash_flow']
            
            # Find dates with sufficient data
            common_dates = balance_sheet.index.intersection(income_stmt.index).intersection(cash_flow.index)
            if len(common_dates) < 2:
                self.logger.warning(f"Insufficient data for F-Score calculation for {ticker}")
                return pd.DataFrame()
            
            # Sort dates
            common_dates = sorted(common_dates)
            
            # Create result DataFrame
            result = pd.DataFrame(index=common_dates)
            
            # Calculate metrics for each date except the first one (need previous data)
            for i in range(1, len(common_dates)):
                current_date = common_dates[i]
                prev_date = common_dates[i-1]
                
                # Get total assets for ROA calculation
                total_assets_curr = None
                total_assets_prev = None
                
                if 'total_assets' in balance_sheet.columns:
                    if not pd.isna(balance_sheet.loc[current_date, 'total_assets']):
                        total_assets_curr = balance_sheet.loc[current_date, 'total_assets']
                    if not pd.isna(balance_sheet.loc[prev_date, 'total_assets']):
                        total_assets_prev = balance_sheet.loc[prev_date, 'total_assets']
                
                if total_assets_curr is None or total_assets_prev is None:
                    continue
                
                # 1. Positive ROA
                net_income = None
                if 'net_income' in income_stmt.columns and not pd.isna(income_stmt.loc[current_date, 'net_income']):
                    net_income = income_stmt.loc[current_date, 'net_income']
                
                if net_income is not None and total_assets_curr > 0:
                    roa = net_income / total_assets_curr
                    result.loc[current_date, 'ROA'] = roa
                    result.loc[current_date, 'F1_Positive_ROA'] = roa > 0
                
                # 2. Positive Operating Cash Flow
                op_cash_flow = None
                ocf_columns = ['operating_cash_flow', 'cash_flow_from_continuing_operating_activities']
                for col in ocf_columns:
                    if col in cash_flow.columns and not pd.isna(cash_flow.loc[current_date, col]):
                        op_cash_flow = cash_flow.loc[current_date, col]
                        break
                
                if op_cash_flow is not None:
                    result.loc[current_date, 'Operating_Cash_Flow'] = op_cash_flow
                    result.loc[current_date, 'F2_Positive_CFO'] = op_cash_flow > 0
                
                # 3. Improving ROA
                prev_net_income = None
                if 'net_income' in income_stmt.columns and not pd.isna(income_stmt.loc[prev_date, 'net_income']):
                    prev_net_income = income_stmt.loc[prev_date, 'net_income']
                
                if net_income is not None and prev_net_income is not None and total_assets_curr > 0 and total_assets_prev > 0:
                    prev_roa = prev_net_income / total_assets_prev
                    result.loc[current_date, 'Previous_ROA'] = prev_roa
                    result.loc[current_date, 'F3_Increasing_ROA'] = roa > prev_roa
                
                # 4. Cash Flow > ROA (accrual)
                if op_cash_flow is not None and net_income is not None and total_assets_curr > 0:
                    cfo_to_assets = op_cash_flow / total_assets_curr
                    result.loc[current_date, 'CFO_to_Assets'] = cfo_to_assets
                    result.loc[current_date, 'F4_CFO_Exceeds_ROA'] = cfo_to_assets > roa
                
                # 5. Decreasing Leverage (LT Debt/Assets)
                lt_debt_curr = None
                lt_debt_prev = None
                
                debt_columns = ['long_term_debt', 'long_term_debt_and_capital_lease_obligation']
                for col in debt_columns:
                    if col in balance_sheet.columns:
                        if not pd.isna(balance_sheet.loc[current_date, col]):
                            lt_debt_curr = balance_sheet.loc[current_date, col]
                        if not pd.isna(balance_sheet.loc[prev_date, col]):
                            lt_debt_prev = balance_sheet.loc[prev_date, col]
                        if lt_debt_curr is not None and lt_debt_prev is not None:
                            break
                
                if lt_debt_curr is not None and lt_debt_prev is not None and total_assets_curr > 0 and total_assets_prev > 0:
                    lt_debt_to_assets_curr = lt_debt_curr / total_assets_curr
                    lt_debt_to_assets_prev = lt_debt_prev / total_assets_prev
                    result.loc[current_date, 'LT_Debt_to_Assets'] = lt_debt_to_assets_curr
                    result.loc[current_date, 'Previous_LT_Debt_to_Assets'] = lt_debt_to_assets_prev
                    result.loc[current_date, 'F5_Decreasing_Leverage'] = lt_debt_to_assets_curr <= lt_debt_to_assets_prev
                
                # 6. Increasing Current Ratio
                curr_ratio_curr = None
                curr_ratio_prev = None
                
                if ('current_assets' in balance_sheet.columns and 'current_liabilities' in balance_sheet.columns):
                    ca_curr = balance_sheet.loc[current_date, 'current_assets']
                    cl_curr = balance_sheet.loc[current_date, 'current_liabilities']
                    ca_prev = balance_sheet.loc[prev_date, 'current_assets']
                    cl_prev = balance_sheet.loc[prev_date, 'current_liabilities']
                    
                    if (not pd.isna(ca_curr) and not pd.isna(cl_curr) and 
                        not pd.isna(ca_prev) and not pd.isna(cl_prev) and
                        cl_curr > 0 and cl_prev > 0):
                        curr_ratio_curr = ca_curr / cl_curr
                        curr_ratio_prev = ca_prev / cl_prev
                        result.loc[current_date, 'Current_Ratio'] = curr_ratio_curr
                        result.loc[current_date, 'Previous_Current_Ratio'] = curr_ratio_prev
                        result.loc[current_date, 'F6_Increasing_Current_Ratio'] = curr_ratio_curr > curr_ratio_prev
                
                # 7. No new shares issued
                shares_curr = None
                shares_prev = None
                
                share_columns = ['ordinary_shares_number', 'share_issued']
                for col in share_columns:
                    if col in balance_sheet.columns:
                        if not pd.isna(balance_sheet.loc[current_date, col]):
                            shares_curr = balance_sheet.loc[current_date, col]
                        if not pd.isna(balance_sheet.loc[prev_date, col]):
                            shares_prev = balance_sheet.loc[prev_date, col]
                        if shares_curr is not None and shares_prev is not None:
                            break
                
                if shares_curr is not None and shares_prev is not None:
                    result.loc[current_date, 'Shares_Outstanding'] = shares_curr
                    result.loc[current_date, 'Previous_Shares_Outstanding'] = shares_prev
                    result.loc[current_date, 'F7_No_New_Shares'] = shares_curr <= shares_prev
                
                # 8. Increasing Gross Margin
                gross_margin_curr = None
                gross_margin_prev = None
                
                if ('gross_profit' in income_stmt.columns and 'total_revenue' in income_stmt.columns):
                    gp_curr = income_stmt.loc[current_date, 'gross_profit']
                    rev_curr = income_stmt.loc[current_date, 'total_revenue']
                    gp_prev = income_stmt.loc[prev_date, 'gross_profit']
                    rev_prev = income_stmt.loc[prev_date, 'total_revenue']
                    
                    if (not pd.isna(gp_curr) and not pd.isna(rev_curr) and 
                        not pd.isna(gp_prev) and not pd.isna(rev_prev) and
                        rev_curr > 0 and rev_prev > 0):
                        gross_margin_curr = gp_curr / rev_curr
                        gross_margin_prev = gp_prev / rev_prev
                        result.loc[current_date, 'Gross_Margin'] = gross_margin_curr
                        result.loc[current_date, 'Previous_Gross_Margin'] = gross_margin_prev
                        result.loc[current_date, 'F8_Increasing_Gross_Margin'] = gross_margin_curr > gross_margin_prev
                
                # 9. Increasing Asset Turnover
                if ('total_revenue' in income_stmt.columns and total_assets_curr > 0 and total_assets_prev > 0):
                    rev_curr = income_stmt.loc[current_date, 'total_revenue']
                    rev_prev = income_stmt.loc[prev_date, 'total_revenue']
                    
                    if not pd.isna(rev_curr) and not pd.isna(rev_prev):
                        asset_turnover_curr = rev_curr / total_assets_curr
                        asset_turnover_prev = rev_prev / total_assets_prev
                        result.loc[current_date, 'Asset_Turnover'] = asset_turnover_curr
                        result.loc[current_date, 'Previous_Asset_Turnover'] = asset_turnover_prev
                        result.loc[current_date, 'F9_Increasing_Asset_Turnover'] = asset_turnover_curr > asset_turnover_prev
            
            # Calculate F-Score for each date
            f_score_columns = [col for col in result.columns if col.startswith('F') and len(col) >= 3 and col[1].isdigit()]
            
            for col in f_score_columns:
                result[col] = result[col].fillna(False).astype(int)
            
            if f_score_columns:
                result['F_Score'] = result[f_score_columns].sum(axis=1)
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Piotroski F-Score for {ticker}: {e}")
            return pd.DataFrame()

    def rank_by_piotroski_fscore(self, tickers: List[str]) -> pd.DataFrame:
        """
        Rank stocks by Piotroski F-Score.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with F-Score rankings.
        """
        try:
            # Calculate F-Score for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_piotroski_fscore(ticker)
                if not metrics.empty:
                    # Use the most recent data
                    all_metrics[ticker] = metrics.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid metrics found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Sort by F-Score (descending)
            if 'F_Score' in df.columns:
                df = df.sort_values('F_Score', ascending=False)
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error ranking by Piotroski F-Score: {e}")
            return pd.DataFrame()
        
# Dividend Yield methods
    def calculate_dividend_metrics(self, ticker: str) -> pd.DataFrame:
        """
        Calculate dividend-related metrics.
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with dividend metrics for each date.
        """
        try:
            # Get financial data
            financials = self._get_financial_data(ticker)
            if not financials:
                return pd.DataFrame()
            
            cash_flow = financials['cash_flow']
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            prices = financials['prices']
            
            # Find common dates
            common_dates = cash_flow.index.intersection(balance_sheet.index).intersection(prices.index)
            if common_dates.empty:
                self.logger.warning(f"No common dates between data sources for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=common_dates)
            
            # Calculate metrics for each date
            for date in common_dates:
                # Get price
                price = prices.loc[date, 'close']
                result.loc[date, 'Price'] = price
                
                # Get dividend information
                dividend_paid = None
                dividend_columns = ['cash_dividends_paid', 'common_stock_dividend_paid']
                
                for col in dividend_columns:
                    if col in cash_flow.columns and not pd.isna(cash_flow.loc[date, col]):
                        dividend_paid = abs(cash_flow.loc[date, col])  # Usually recorded as negative
                        break
                
                # Get shares outstanding
                shares_outstanding = None
                share_columns = ['ordinary_shares_number', 'share_issued']
                
                for col in share_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        shares_outstanding = balance_sheet.loc[date, col]
                        break
                
                # Calculate dividend per share and yield
                if dividend_paid is not None and dividend_paid > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    dividend_per_share = dividend_paid / shares_outstanding
                    dividend_yield = dividend_per_share / price
                    
                    result.loc[date, 'Dividend_Paid'] = dividend_paid
                    result.loc[date, 'Shares_Outstanding'] = shares_outstanding
                    result.loc[date, 'Dividend_Per_Share'] = dividend_per_share
                    result.loc[date, 'Dividend_Yield'] = dividend_yield
                    
                    # Calculate dividend payout ratio
                    net_income = None
                    if 'net_income' in income_stmt.columns and date in income_stmt.index:
                        net_income = income_stmt.loc[date, 'net_income']
                    
                    if net_income is not None and net_income > 0:
                        payout_ratio = dividend_paid / net_income
                        result.loc[date, 'Payout_Ratio'] = payout_ratio
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating dividend metrics for {ticker}: {e}")
            return pd.DataFrame()

    def rank_by_dividend_yield(self, tickers: List[str]) -> pd.DataFrame:
        """
        Rank stocks by dividend yield.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with dividend yield rankings.
        """
        try:
            # Calculate dividend metrics for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_dividend_metrics(ticker)
                if not metrics.empty and 'Dividend_Yield' in metrics.columns:
                    # Use the most recent data with a dividend
                    metrics_with_div = metrics.dropna(subset=['Dividend_Yield'])
                    if not metrics_with_div.empty:
                        all_metrics[ticker] = metrics_with_div.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid dividend metrics found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Sort by Dividend Yield (descending)
            if 'Dividend_Yield' in df.columns:
                df = df.sort_values('Dividend_Yield', ascending=False)
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error ranking by dividend yield: {e}")
            return pd.DataFrame()
        
    # Combined value score
    def calculate_combined_value_score(self, ticker: str) -> pd.DataFrame:
        """
        Calculate a combined value score using multiple strategies.
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with combined value score for each date.
        """
        try:
            # Calculate individual metrics
            magic_formula = self.calculate_magic_formula(ticker)
            acquirers_multiple = self.calculate_acquirers_multiple(ticker)
            deep_value = self.calculate_deep_value_metrics(ticker)
            graham = self.calculate_graham_criteria(ticker)
            piotroski = self.calculate_piotroski_fscore(ticker)
            dividend = self.calculate_dividend_metrics(ticker)
            
            # Get all dates from all dataframes
            all_dates = set()
            for df in [magic_formula, acquirers_multiple, deep_value, graham, piotroski, dividend]:
                if not df.empty:
                    all_dates.update(df.index)
            
            if not all_dates:
                self.logger.warning(f"No valid dates found for combined score for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame with all dates
            result = pd.DataFrame(index=sorted(all_dates))
            
            # Add metrics from each strategy
            key_metrics = {
                'Magic Formula': {
                    'source': magic_formula,
                    'metrics': ['Earnings_Yield', 'Return_on_Capital']
                },
                'Acquirers Multiple': {
                    'source': acquirers_multiple,
                    'metrics': ['Acquirers_Multiple']
                },
                'Deep Value': {
                    'source': deep_value,
                    'metrics': ['P_E_Ratio', 'P_B_Ratio', 'Dividend_Yield']
                },
                'Graham': {
                    'source': graham,
                    'metrics': ['Graham_Score']
                },
                'Piotroski': {
                    'source': piotroski,
                    'metrics': ['F_Score']
                },
                'Dividend': {
                    'source': dividend,
                    'metrics': ['Dividend_Yield', 'Payout_Ratio']
                }
            }
            
            # Add metrics to result
            for strategy_name, strategy_data in key_metrics.items():
                source_df = strategy_data['source']
                metrics = strategy_data['metrics']
                
                if not source_df.empty:
                    for metric in metrics:
                        if metric in source_df.columns:
                            result[f"{strategy_name}_{metric}"] = pd.Series(index=result.index)
                            for date in source_df.index:
                                if date in result.index:
                                    result.loc[date, f"{strategy_name}_{metric}"] = source_df.loc[date, metric]
            
            # Calculate normalized scores for each metric
            score_columns = []
            
            # 1. Earnings Yield (higher is better)
            if 'Magic Formula_Earnings_Yield' in result.columns:
                result['Score_Earnings_Yield'] = result['Magic Formula_Earnings_Yield'].rank(pct=True)
                score_columns.append('Score_Earnings_Yield')
            
            # 2. Return on Capital (higher is better)
            if 'Magic Formula_Return_on_Capital' in result.columns:
                result['Score_Return_on_Capital'] = result['Magic Formula_Return_on_Capital'].rank(pct=True)
                score_columns.append('Score_Return_on_Capital')
            
            # 3. Acquirer's Multiple (lower is better)
            if 'Acquirers Multiple_Acquirers_Multiple' in result.columns:
                result['Score_Acquirers_Multiple'] = 1 - result['Acquirers Multiple_Acquirers_Multiple'].rank(pct=True)
                score_columns.append('Score_Acquirers_Multiple')
            
            # 4. P/E Ratio (lower is better)
            if 'Deep Value_P_E_Ratio' in result.columns:
                result['Score_P_E_Ratio'] = 1 - result['Deep Value_P_E_Ratio'].rank(pct=True)
                score_columns.append('Score_P_E_Ratio')
            
            # 5. P/B Ratio (lower is better)
            if 'Deep Value_P_B_Ratio' in result.columns:
                result['Score_P_B_Ratio'] = 1 - result['Deep Value_P_B_Ratio'].rank(pct=True)
                score_columns.append('Score_P_B_Ratio')
            
            # 6. Dividend Yield (higher is better)
            if 'Deep Value_Dividend_Yield' in result.columns:
                result['Score_Dividend_Yield'] = result['Deep Value_Dividend_Yield'].rank(pct=True)
                score_columns.append('Score_Dividend_Yield')
            
            # 7. Graham Score (higher is better)
            if 'Graham_Graham_Score' in result.columns:
                result['Score_Graham'] = result['Graham_Graham_Score'].rank(pct=True)
                score_columns.append('Score_Graham')
            
            # 8. Piotroski F-Score (higher is better)
            if 'Piotroski_F_Score' in result.columns:
                result['Score_Piotroski'] = result['Piotroski_F_Score'].rank(pct=True)
                score_columns.append('Score_Piotroski')
            
            # Get weights from parameters
            weights = {
                'Score_Earnings_Yield': self.params['weight_magic_formula'] / 2,
                'Score_Return_on_Capital': self.params['weight_magic_formula'] / 2,
                'Score_Acquirers_Multiple': self.params['weight_acquirers_multiple'],
                'Score_P_E_Ratio': self.params['weight_deep_value'] / 3,
                'Score_P_B_Ratio': self.params['weight_deep_value'] / 3,
                'Score_Dividend_Yield': self.params['weight_deep_value'] / 3 + self.params['weight_dividend'],
                'Score_Graham': self.params['weight_graham'],
                'Score_Piotroski': self.params['weight_piotroski']
            }
            
            # Calculate weighted average score
            if score_columns:
                result['Combined_Value_Score'] = 0
                total_weight = 0
                
                for col in score_columns:
                    if col in weights:
                        weight = weights[col]
                        result['Combined_Value_Score'] += result[col] * weight
                        total_weight += weight
                
                if total_weight > 0:
                    result['Combined_Value_Score'] = result['Combined_Value_Score'] / total_weight
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating combined value score for {ticker}: {e}")
            return pd.DataFrame()

    def rank_by_combined_value_score(self, tickers: List[str]) -> pd.DataFrame:
        """
        Rank stocks by the combined value score.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            pd.DataFrame: DataFrame with combined value score rankings.
        """
        try:
            # Calculate combined value score for each ticker
            all_metrics = {}
            for ticker in tickers:
                metrics = self.calculate_combined_value_score(ticker)
                if not metrics.empty and 'Combined_Value_Score' in metrics.columns:
                    # Use the most recent data with a combined score
                    metrics_with_score = metrics.dropna(subset=['Combined_Value_Score'])
                    if not metrics_with_score.empty:
                        all_metrics[ticker] = metrics_with_score.iloc[-1].to_dict()
            
            if not all_metrics:
                self.logger.warning("No valid combined value scores found for any ticker")
                return pd.DataFrame()
            
            # Create DataFrame from all metrics
            df = pd.DataFrame.from_dict(all_metrics, orient='index')
            
            # Sort by Combined Value Score (descending)
            if 'Combined_Value_Score' in df.columns:
                df = df.sort_values('Combined_Value_Score', ascending=False)
            
            return df
        
        except Exception as e:
            self.logger.error(f"Error ranking by combined value score: {e}")
            return pd.DataFrame()

    def generate_signals(self, ticker: str, 
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals based on value metrics for the given ticker.
        
        Args:
            ticker (str): Stock ticker symbol.
            start_date (str, optional): Start date in 'YYYY-MM-DD' format.
            end_date (str, optional): End date in 'YYYY-MM-DD' format.
            initial_position (int): Starting position (0, 1, or -1).
            latest_only (bool): If True, returns only the most recent signal.
            
        Returns:
            pd.DataFrame: DataFrame with value metrics and signals.
        """
        try:
            # Calculate combined value score
            value_scores = self.calculate_combined_value_score(ticker)
            if value_scores.empty:
                self.logger.warning(f"No value scores calculated for {ticker}")
                return pd.DataFrame()
            
            # Filter by date range if provided
            if start_date:
                value_scores = value_scores[value_scores.index >= pd.to_datetime(start_date)]
            if end_date:
                value_scores = value_scores[value_scores.index <= pd.to_datetime(end_date)]
            
            if value_scores.empty:
                self.logger.warning(f"No data in the specified date range for {ticker}")
                return pd.DataFrame()
            
            # Get price data
            prices = self.get_historical_prices(
                ticker,
                from_date=value_scores.index.min().strftime('%Y-%m-%d'),
                to_date=value_scores.index.max().strftime('%Y-%m-%d')
            )
            
            # Merge price data with value scores
            merged = pd.DataFrame(index=prices.index)
            merged['close'] = prices['close']
            
            # Reindex value scores to align with prices
            for date in value_scores.index:
                closest_price_date = prices.index[prices.index >= date].min()
                if not pd.isna(closest_price_date):
                    for col in value_scores.columns:
                        merged.loc[closest_price_date, col] = value_scores.loc[date, col]
            
            # Forward fill missing values
            merged = merged.ffill()
            
            # Filter out rows with no value score
            merged = merged.dropna(subset=['Combined_Value_Score'])
            
            if merged.empty:
                self.logger.warning(f"No valid data after merging for {ticker}")
                return pd.DataFrame()
            
            # Generate signals based on value score
            merged['signal'] = 0
            
            # Consider high value score (> 0.7) as buy signal
            merged.loc[merged['Combined_Value_Score'] > 0.7, 'signal'] = 1
            
            # Consider low value score (< 0.3) as sell signal
            merged.loc[merged['Combined_Value_Score'] < 0.3, 'signal'] = -1
            
            # Generate position column starting from initial_position
            merged['position'] = merged['signal'].cumsum().shift(1).fillna(initial_position)
            
            # Ensure position is between -1 and 1
            merged['position'] = merged['position'].clip(-1, 1)
            
            # Return only the latest data point if requested
            if latest_only and not merged.empty:
                return merged.iloc[[-1]]
            
            return merged
        
        except Exception as e:
            self.logger.error(f"Error generating signals for {ticker}: {e}")
            return pd.DataFrame()
        
    # Additional helper methods
    def filter_by_sector(self, tickers: List[str]) -> List[str]:
        """
        Filter tickers by sector based on exclusion list in parameters.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            List[str]: Filtered list of tickers.
        """
        excluded_sectors = self.params.get('exclude_sectors', [])
        if not excluded_sectors:
            return tickers
        
        filtered_tickers = []
        for ticker in tickers:
            try:
                company_info = self.get_company_info(ticker)
                if 'sector' in company_info:
                    sector = company_info['sector']
                    if sector not in excluded_sectors:
                        filtered_tickers.append(ticker)
                else:
                    # If sector info is not available, include the ticker
                    filtered_tickers.append(ticker)
            except Exception as e:
                self.logger.warning(f"Error getting sector for {ticker}: {e}")
        
        return filtered_tickers

    def filter_by_market_cap(self, tickers: List[str]) -> List[str]:
        """
        Filter tickers by market capitalization based on parameters.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            List[str]: Filtered list of tickers.
        """
        min_market_cap = self.params.get('min_market_cap')
        max_market_cap = self.params.get('max_market_cap')
        
        if min_market_cap is None and max_market_cap is None:
            return tickers
        
        filtered_tickers = []
        for ticker in tickers:
            try:
                company_info = self.get_company_info(ticker)
                market_cap = None
                
                # Try to get market cap from company_info
                if 'marketcap' in company_info and not pd.isna(company_info['marketcap']):
                    market_cap = company_info['marketcap']
                
                if market_cap is not None:
                    if min_market_cap is not None and market_cap < min_market_cap:
                        continue
                    if max_market_cap is not None and market_cap > max_market_cap:
                        continue
                    filtered_tickers.append(ticker)
                else:
                    # If market cap info is not available, include the ticker
                    filtered_tickers.append(ticker)
            except Exception as e:
                self.logger.warning(f"Error getting market cap for {ticker}: {e}")
        
        return filtered_tickers

    def filter_by_liquidity(self, tickers: List[str]) -> List[str]:
        """
        Filter tickers by trading liquidity (volume) based on parameters.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            
        Returns:
            List[str]: Filtered list of tickers.
        """
        min_liquidity = self.params.get('min_liquidity')
        if min_liquidity is None:
            return tickers
        
        filtered_tickers = []
        for ticker in tickers:
            try:
                # Get recent price data (last 30 days)
                prices = self.get_historical_prices(ticker, lookback=30)
                if not prices.empty:
                    avg_volume = prices['volume'].mean()
                    if avg_volume >= min_liquidity:
                        filtered_tickers.append(ticker)
            except Exception as e:
                self.logger.warning(f"Error getting volume data for {ticker}: {e}")
        
        return filtered_tickers

    def get_top_value_stocks(self, tickers: List[str], top_n: int = 10) -> List[str]:
        """
        Get the top N value stocks based on combined value score.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            top_n (int): Number of top stocks to return.
            
        Returns:
            List[str]: List of top value stock tickers.
        """
        try:
            # Filter tickers by sector, market cap, and liquidity
            filtered_tickers = self.filter_by_sector(tickers)
            filtered_tickers = self.filter_by_market_cap(filtered_tickers)
            filtered_tickers = self.filter_by_liquidity(filtered_tickers)
            
            if not filtered_tickers:
                self.logger.warning("No tickers remain after filtering")
                return []
            
            # Rank by combined value score
            ranked_stocks = self.rank_by_combined_value_score(filtered_tickers)
            
            if ranked_stocks.empty:
                self.logger.warning("No valid value scores calculated")
                return []
            
            # Get top N tickers
            top_tickers = ranked_stocks.index[:top_n].tolist()
            return top_tickers
        
        except Exception as e:
            self.logger.error(f"Error getting top value stocks: {e}")
            return []
        

# Typical parameter values for Indian markets
INDIAN_MARKET_PARAMS = {
    # Financial filters
    'min_market_cap': 500,  # 500 crores (~$60M)
    'max_market_cap': None,  # No upper limit
    'min_liquidity': 100000,  # Minimum daily volume
    
    # Sector exclusions
    'exclude_sectors': ['Financial Services', 'Banks'],
    
    # Strategy weights
    'weight_magic_formula': 1.0,
    'weight_acquirers_multiple': 1.0,
    'weight_deep_value': 1.0,
    'weight_graham': 1.0,
    'weight_piotroski': 1.0,
    'weight_dividend': 0.5,  # Lower weight as not all companies pay dividends in India
    
    # Lookback period for historical analysis
    'lookback_years': 5
}

# Small-cap focused strategy
SMALL_CAP_PARAMS = {
    'min_market_cap': 100,  # 100 crores (~$12M)
    'max_market_cap': 5000,  # 5000 crores (~$600M)
    'min_liquidity': 50000,  # Lower liquidity requirement
    'exclude_sectors': ['Financial Services', 'Banks'],
    'weight_magic_formula': 1.0,
    'weight_acquirers_multiple': 1.2,  # Higher weight for acquirer's multiple
    'weight_deep_value': 1.2,  # Higher weight for deep value
    'weight_graham': 0.8,  # Lower weight as small caps may not meet all Graham criteria
    'weight_piotroski': 1.2,  # Higher weight for operational efficiency
    'weight_dividend': 0.3,  # Lower weight as small caps often reinvest rather than pay dividends
    'lookback_years': 3  # Shorter lookback for high-growth small caps
}

# Dividend-focused strategy
DIVIDEND_INCOME_PARAMS = {
    'min_market_cap': 1000,  # 1000 crores (~$120M)
    'max_market_cap': None,
    'min_liquidity': 100000,
    'exclude_sectors': [],  # Include all sectors as financials often pay good dividends
    'weight_magic_formula': 0.8,
    'weight_acquirers_multiple': 0.8,
    'weight_deep_value': 1.0,
    'weight_graham': 1.2,  # Higher weight for stability
    'weight_piotroski': 1.0,
    'weight_dividend': 2.0,  # Much higher weight on dividend metrics
    'lookback_years': 7  # Longer lookback to find consistent dividend payers
}

        
