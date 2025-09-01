# trading_system/src/strategies/value_strategy/financial_metrics.py

"""
Financial metrics calculation for value investing strategies.
Implements various ratio and valuation metric calculations.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd


class FinancialMetricsCalculator:
    """
    Financial metrics calculator for value investing strategies.
    
    Calculates various financial ratios and metrics used in value investing
    strategies including profitability ratios, valuation ratios, efficiency ratios,
    and leverage ratios.
    """
    
    def __init__(self):
        """Initialize the financial metrics calculator."""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _safe_divide(self, numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        """
        Safely divide two pandas Series, handling division by zero and NaN values.
        
        Args:
            numerator (pd.Series): The numerator series.
            denominator (pd.Series): The denominator series.
            
        Returns:
            pd.Series: The result of the division, with NaN for invalid results.
        """
        result = pd.Series(np.nan, index=numerator.index)
        valid_mask = (denominator != 0) & (~pd.isna(numerator)) & (~pd.isna(denominator))
        result[valid_mask] = numerator[valid_mask] / denominator[valid_mask]
        return result
    
    def calculate_enterprise_value(self, 
                                  financials: Dict[str, pd.DataFrame], 
                                  date: pd.Timestamp) -> float:
        """
        Calculate Enterprise Value for a specific date.
        
        Enterprise Value = Market Cap + Total Debt - Cash and Cash Equivalents
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            date (pd.Timestamp): Date to calculate EV for
            
        Returns:
            float: Enterprise Value, or None if calculation fails
        """
        try:
            balance_sheet = financials['balance_sheet']
            aligned_prices = financials['aligned_prices']
            company_info = financials['company_info']
            
            # Get the market cap
            if date in aligned_prices.index:
                price = aligned_prices.loc[date, 'close']
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
                if shares_outstanding is None and not isinstance(company_info, pd.DataFrame) and 'sharesoutstanding' in company_info:
                    shares_outstanding = company_info['sharesoutstanding']
                
                # Calculate market cap
                if shares_outstanding is not None and shares_outstanding > 0:
                    market_cap = price * shares_outstanding
                else:
                    # Try to use market cap from company_info
                    if not isinstance(company_info, pd.DataFrame) and 'marketcap' in company_info and not pd.isna(company_info['marketcap']):
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
            return enterprise_value if enterprise_value > 0 else None
        
        except Exception as e:
            self.logger.error(f"Error calculating Enterprise Value: {e}")
            return None

    def calculate_valuation_ratios(self, 
                                  financials: Dict[str, pd.DataFrame], 
                                  dates: Optional[List[pd.Timestamp]] = None) -> pd.DataFrame:
        """
        Calculate key valuation ratios.
        
        Ratios calculated:
        - P/E Ratio (Price to Earnings)
        - P/B Ratio (Price to Book)
        - P/S Ratio (Price to Sales)
        - EV/EBITDA
        - EV/Sales
        - Dividend Yield
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            dates (List[pd.Timestamp], optional): Specific dates to calculate for
                                                 If None, uses all available financial dates
            
        Returns:
            pd.DataFrame: DataFrame with valuation ratios
        """
        try:
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            cash_flow = financials['cash_flow']
            aligned_prices = financials['aligned_prices']
            
            # Use provided dates or all available financial dates
            if dates is None:
                dates = financials['fin_dates']
            
            # Filter to dates present in the price data
            valid_dates = [date for date in dates if date in aligned_prices.index]
            
            if not valid_dates:
                self.logger.warning("No valid dates with price data for valuation ratios")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=valid_dates)
            
            # Add price data
            result['Price'] = aligned_prices.loc[valid_dates, 'close']
            
            # Calculate market cap and enterprise value for each date
            market_caps = []
            enterprise_values = []
            
            for date in valid_dates:
                # Get shares outstanding
                shares_outstanding = None
                share_columns = ['ordinary_shares_number', 'share_issued']
                
                for col in share_columns:
                    if col in balance_sheet.columns and date in balance_sheet.index:
                        if not pd.isna(balance_sheet.loc[date, col]) and balance_sheet.loc[date, col] > 0:
                            shares_outstanding = balance_sheet.loc[date, col]
                            break
                
                if shares_outstanding is not None:
                    market_cap = aligned_prices.loc[date, 'close'] * shares_outstanding
                    market_caps.append(market_cap)
                else:
                    market_caps.append(np.nan)
                
                # Calculate Enterprise Value
                ev = self.calculate_enterprise_value(financials, date)
                enterprise_values.append(ev)
            
            # Add calculated values to result
            result['Market_Cap'] = market_caps
            result['Enterprise_Value'] = enterprise_values
            
            # Calculate P/E Ratio
            earnings = pd.Series(np.nan, index=valid_dates)
            eps = pd.Series(np.nan, index=valid_dates)
            
            for date in valid_dates:
                if date in income_stmt.index:
                    for col in ['net_income', 'net_income_common_stockholders']:
                        if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                            earnings[date] = income_stmt.loc[date, col]
                            
                            # Calculate EPS
                            if not pd.isna(market_caps[valid_dates.index(date)]) and market_caps[valid_dates.index(date)] > 0:
                                shares = market_caps[valid_dates.index(date)] / aligned_prices.loc[date, 'close']
                                if shares > 0:
                                    eps[date] = earnings[date] / shares
                            break
            
            # Calculate P/E Ratio
            result['Earnings'] = earnings
            result['EPS'] = eps
            result['P_E_Ratio'] = self._safe_divide(result['Price'], result['EPS'])
            
            # Calculate P/B Ratio
            book_value = pd.Series(np.nan, index=valid_dates)
            book_value_per_share = pd.Series(np.nan, index=valid_dates)
            
            for date in valid_dates:
                if date in balance_sheet.index:
                    for col in ['common_stock_equity', 'stockholders_equity', 'total_equity_gross_minority_interest']:
                        if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                            book_value[date] = balance_sheet.loc[date, col]
                            
                            # Calculate Book Value Per Share
                            if not pd.isna(market_caps[valid_dates.index(date)]) and market_caps[valid_dates.index(date)] > 0:
                                shares = market_caps[valid_dates.index(date)] / aligned_prices.loc[date, 'close']
                                if shares > 0:
                                    book_value_per_share[date] = book_value[date] / shares
                            break
            
            # Calculate P/B Ratio
            result['Book_Value'] = book_value
            result['Book_Value_Per_Share'] = book_value_per_share
            result['P_B_Ratio'] = self._safe_divide(result['Price'], result['Book_Value_Per_Share'])
            
            # Calculate P/S Ratio
            sales = pd.Series(np.nan, index=valid_dates)
            
            for date in valid_dates:
                if date in income_stmt.index:
                    for col in ['total_revenue', 'revenue']:
                        if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                            sales[date] = income_stmt.loc[date, col]
                            break
            
            # Calculate P/S Ratio and EV/Sales
            result['Sales'] = sales
            sales_per_share = self._safe_divide(sales, result['Market_Cap'] / result['Price'])
            
            result['P_S_Ratio'] = self._safe_divide(result['Price'], sales_per_share)
            result['EV_Sales'] = self._safe_divide(result['Enterprise_Value'], result['Sales'])
            
            # Calculate EV/EBITDA
            ebitda = pd.Series(np.nan, index=valid_dates)
            
            for date in valid_dates:
                if date in income_stmt.index:
                    # Direct EBITDA column
                    if 'ebitda' in income_stmt.columns and not pd.isna(income_stmt.loc[date, 'ebitda']):
                        ebitda[date] = income_stmt.loc[date, 'ebitda']
                    else:
                        # Calculate EBITDA from components
                        ebit = None
                        depreciation = None
                        
                        # Get EBIT
                        for col in ['ebit', 'operating_income']:
                            if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                                ebit = income_stmt.loc[date, col]
                                break
                        
                        # Get Depreciation & Amortization
                        for col in ['depreciation_amortization', 'depreciation_and_amortization']:
                            if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                                depreciation = income_stmt.loc[date, col]
                                break
                            
                        # If we can get both components, calculate EBITDA
                        if ebit is not None and depreciation is not None:
                            ebitda[date] = ebit + depreciation
            
            # Calculate EV/EBITDA
            result['EBITDA'] = ebitda
            result['EV_EBITDA'] = self._safe_divide(result['Enterprise_Value'], result['EBITDA'])
            
            # Calculate Dividend Yield
            dividend_per_share = pd.Series(np.nan, index=valid_dates)
            
            for date in valid_dates:
                # Get dividend information from cash flow statement
                if date in cash_flow.index:
                    for col in ['cash_dividends_paid', 'common_stock_dividend_paid']:
                        if col in cash_flow.columns and not pd.isna(cash_flow.loc[date, col]):
                            total_dividend = abs(cash_flow.loc[date, col])  # Usually recorded as negative
                            
                            # Calculate per share
                            if not pd.isna(market_caps[valid_dates.index(date)]) and market_caps[valid_dates.index(date)] > 0:
                                shares = market_caps[valid_dates.index(date)] / aligned_prices.loc[date, 'close']
                                if shares > 0:
                                    dividend_per_share[date] = total_dividend / shares
                            break
            
            # Calculate Dividend Yield
            result['Dividend_Per_Share'] = dividend_per_share
            result['Dividend_Yield'] = self._safe_divide(result['Dividend_Per_Share'], result['Price'])
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating valuation ratios: {e}")
            return pd.DataFrame()

    def calculate_profitability_ratios(self, 
                                     financials: Dict[str, pd.DataFrame], 
                                     dates: Optional[List[pd.Timestamp]] = None) -> pd.DataFrame:
        """
        Calculate key profitability ratios.
        
        Ratios calculated:
        - Return on Assets (ROA)
        - Return on Equity (ROE)
        - Return on Capital Employed (ROCE)
        - Gross Margin
        - Operating Margin
        - Net Profit Margin
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            dates (List[pd.Timestamp], optional): Specific dates to calculate for
                                                 If None, uses all available financial dates
            
        Returns:
            pd.DataFrame: DataFrame with profitability ratios
        """
        try:
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            
            # Use provided dates or all available financial dates
            if dates is None:
                dates = financials['fin_dates']
            
            # Filter to dates present in both balance sheet and income statement
            bs_dates = set(balance_sheet.index)
            is_dates = set(income_stmt.index)
            valid_dates = [d for d in dates if d in bs_dates and d in is_dates]
            
            if not valid_dates:
                self.logger.warning("No valid dates with both balance sheet and income statement data")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=valid_dates)
            
            # Add income statement data
            for date in valid_dates:
                # Net Income
                for col in ['net_income', 'net_income_common_stockholders']:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        result.loc[date, 'Net_Income'] = income_stmt.loc[date, col]
                        break
                
                # EBIT (Operating Income)
                for col in ['ebit', 'operating_income']:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        result.loc[date, 'EBIT'] = income_stmt.loc[date, col]
                        break
                
                # Gross Profit
                if 'gross_profit' in income_stmt.columns and not pd.isna(income_stmt.loc[date, 'gross_profit']):
                    result.loc[date, 'Gross_Profit'] = income_stmt.loc[date, 'gross_profit']
                
                # Revenue
                for col in ['total_revenue', 'revenue']:
                    if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                        result.loc[date, 'Revenue'] = income_stmt.loc[date, col]
                        break
            
            # Add balance sheet data
            for date in valid_dates:
                # Total Assets
                if 'total_assets' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'total_assets']):
                    result.loc[date, 'Total_Assets'] = balance_sheet.loc[date, 'total_assets']
                
                # Equity
                for col in ['common_stock_equity', 'stockholders_equity', 'total_equity_gross_minority_interest']:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        result.loc[date, 'Equity'] = balance_sheet.loc[date, col]
                        break
                
                # Current Liabilities
                if 'current_liabilities' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_liabilities']):
                    result.loc[date, 'Current_Liabilities'] = balance_sheet.loc[date, 'current_liabilities']
                
                # Non-current Liabilities
                for col in ['long_term_debt', 'non_current_liabilities']:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        result.loc[date, 'Non_Current_Liabilities'] = balance_sheet.loc[date, col]
                        break
            
            # Calculate Capital Employed (Total Assets - Current Liabilities)
            result['Capital_Employed'] = result['Total_Assets'] - result.get('Current_Liabilities', 0)
            
            # Calculate ROA
            result['ROA'] = self._safe_divide(result['Net_Income'], result['Total_Assets'])
            
            # Calculate ROE
            result['ROE'] = self._safe_divide(result['Net_Income'], result['Equity'])
            
            # Calculate ROCE
            result['ROCE'] = self._safe_divide(result['EBIT'], result['Capital_Employed'])
            
            # Calculate Margin Ratios
            result['Gross_Margin'] = self._safe_divide(result['Gross_Profit'], result['Revenue'])
            result['Operating_Margin'] = self._safe_divide(result['EBIT'], result['Revenue'])
            result['Net_Profit_Margin'] = self._safe_divide(result['Net_Income'], result['Revenue'])
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating profitability ratios: {e}")
            return pd.DataFrame()

    def calculate_leverage_ratios(self, 
                                financials: Dict[str, pd.DataFrame], 
                                dates: Optional[List[pd.Timestamp]] = None) -> pd.DataFrame:
        """
        Calculate key financial leverage ratios.
        
        Ratios calculated:
        - Debt to Equity Ratio
        - Debt to Assets Ratio
        - Interest Coverage Ratio
        - Current Ratio
        - Quick Ratio
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            dates (List[pd.Timestamp], optional): Specific dates to calculate for
                                                 If None, uses all available financial dates
            
        Returns:
            pd.DataFrame: DataFrame with leverage ratios
        """
        try:
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            
            # Use provided dates or all available financial dates
            if dates is None:
                dates = financials['fin_dates']
            
            # Filter to dates present in balance sheet
            valid_dates = [d for d in dates if d in balance_sheet.index]
            
            if not valid_dates:
                self.logger.warning("No valid dates with balance sheet data")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=valid_dates)
            
            # Add balance sheet data
            for date in valid_dates:
                # Total Debt
                for col in ['total_debt', 'short_term_debt_plus_long_term_debt']:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        result.loc[date, 'Total_Debt'] = balance_sheet.loc[date, col]
                        break
                
                # Long-term Debt
                if 'long_term_debt' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'long_term_debt']):
                    result.loc[date, 'Long_Term_Debt'] = balance_sheet.loc[date, 'long_term_debt']
                
                # Equity
                for col in ['common_stock_equity', 'stockholders_equity', 'total_equity_gross_minority_interest']:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        result.loc[date, 'Equity'] = balance_sheet.loc[date, col]
                        break
                
                # Total Assets
                if 'total_assets' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'total_assets']):
                    result.loc[date, 'Total_Assets'] = balance_sheet.loc[date, 'total_assets']
                
                # Current Assets
                if 'current_assets' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_assets']):
                    result.loc[date, 'Current_Assets'] = balance_sheet.loc[date, 'current_assets']
                
                # Current Liabilities
                if 'current_liabilities' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'current_liabilities']):
                    result.loc[date, 'Current_Liabilities'] = balance_sheet.loc[date, 'current_liabilities']
                
                # Inventory
                if 'inventory' in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, 'inventory']):
                    result.loc[date, 'Inventory'] = balance_sheet.loc[date, 'inventory']
                else:
                    result.loc[date, 'Inventory'] = 0
            
            # Add income statement data for interest coverage
            for date in valid_dates:
                if date in income_stmt.index:
                    # EBIT
                    for col in ['ebit', 'operating_income']:
                        if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                            result.loc[date, 'EBIT'] = income_stmt.loc[date, col]
                            break
                    
                    # Interest Expense
                    for col in ['interest_expense', 'interest_and_debt_expense']:
                        if col in income_stmt.columns and not pd.isna(income_stmt.loc[date, col]):
                            result.loc[date, 'Interest_Expense'] = abs(income_stmt.loc[date, col])
                            break
            
            # Calculate leverage ratios
            
            # Debt ratios
            if 'Total_Debt' not in result.columns:
                # Try to calculate from long-term + short-term debt
                if 'Long_Term_Debt' in result.columns:
                    short_term_debt = pd.Series(0, index=valid_dates)
                    for date in valid_dates:
                        for col in ['short_term_debt', 'current_debt']:
                            if col in balance_sheet.columns and date in balance_sheet.index and not pd.isna(balance_sheet.loc[date, col]):
                                short_term_debt[date] = balance_sheet.loc[date, col]
                                break
                    
                    result['Total_Debt'] = result['Long_Term_Debt'] + short_term_debt
            
            # Debt to Equity Ratio
            result['Debt_to_Equity'] = self._safe_divide(result.get('Total_Debt', 0), result['Equity'])
            
            # Debt to Assets Ratio
            result['Debt_to_Assets'] = self._safe_divide(result.get('Total_Debt', 0), result['Total_Assets'])
            
            # Interest Coverage Ratio
            result['Interest_Coverage'] = self._safe_divide(result.get('EBIT', 0), result.get('Interest_Expense', np.inf))
            
            # Liquidity Ratios
            
            # Current Ratio
            result['Current_Ratio'] = self._safe_divide(result['Current_Assets'], result['Current_Liabilities'])
            
            # Quick Ratio (Acid-Test Ratio)
            result['Quick_Ratio'] = self._safe_divide(
                result['Current_Assets'] - result['Inventory'], 
                result['Current_Liabilities']
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating leverage ratios: {e}")
            return pd.DataFrame()

    def calculate_all_ratios(self, 
                           financials: Dict[str, pd.DataFrame],
                           dates: Optional[List[pd.Timestamp]] = None) -> pd.DataFrame:
        """
        Calculate all financial ratios and combine into a single DataFrame.
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            dates (List[pd.Timestamp], optional): Specific dates to calculate for
                                                 If None, uses all available financial dates
            
        Returns:
            pd.DataFrame: DataFrame with all financial ratios and metrics
        """
        try:
            # Use provided dates or all available financial dates
            if dates is None:
                dates = financials['fin_dates']
            
            if not dates:
                self.logger.warning("No valid dates for ratio calculations")
                return pd.DataFrame()
            
            # Calculate all ratio types
            valuation_ratios = self.calculate_valuation_ratios(financials, dates)
            profitability_ratios = self.calculate_profitability_ratios(financials, dates)
            leverage_ratios = self.calculate_leverage_ratios(financials, dates)
            
            # Combine all ratios
            all_dfs = [
                valuation_ratios, profitability_ratios, leverage_ratios
            ]
            
            # Filter out empty DataFrames
            valid_dfs = [df for df in all_dfs if not df.empty]
            
            if not valid_dfs:
                self.logger.warning("No valid ratio data calculated")
                return pd.DataFrame()
            
            # Find common dates across all DataFrames
            common_dates = set(valid_dfs[0].index)
            for df in valid_dfs[1:]:
                common_dates = common_dates.intersection(set(df.index))
            
            if not common_dates:
                # If no common dates, use all dates and allow NaN values
                all_dates = set()
                for df in valid_dfs:
                    all_dates.update(df.index)
                result = pd.DataFrame(index=sorted(all_dates))
                for df in valid_dfs:
                    for column in df.columns:
                        result[column] = df[column]
            else:
                # Use common dates across all DataFrames
                result = pd.DataFrame(index=sorted(common_dates))
                for df in valid_dfs:
                    for column in df.columns:
                        result[column] = df.loc[sorted(common_dates), column]
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating all ratios: {e}")
            return pd.DataFrame()