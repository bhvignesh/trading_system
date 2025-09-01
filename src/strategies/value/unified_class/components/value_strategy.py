# trading_system/src/strategies/value_strategy/value_strategy.py

"""
Value investing strategy implementation.
Implements scoring and ranking based on various value metrics.
"""

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from src.database.config import DatabaseConfig
from src.strategies.base_strat import BaseStrategy
from src.strategies.value.unified_class.components.data_manager import \
    FinancialDataManager
from src.strategies.value.unified_class.components.financial_metrics import \
    FinancialMetricsCalculator


class ValueStrategy(BaseStrategy):
    """
    Implementation of a comprehensive value investing strategy.
    
    Combines multiple value investing approaches including:
    1. Magic Formula (Joel Greenblatt)
    2. Acquirer's Multiple (Tobias Carlisle)
    3. Deep Value metrics (P/E, P/B, Dividend Yield)
    4. Graham's Defensive Investor criteria (Benjamin Graham)
    5. Piotroski F-Score (Joseph Piotroski)
    
    The strategy scores and ranks stocks based on a weighted combination
    of these metrics to identify undervalued companies with strong fundamentals.
    """
    
    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict[str, Any]] = None):
        """
        Initialize the value strategy.
        
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
        self.data_manager = FinancialDataManager(self)
        self.metrics_calculator = FinancialMetricsCalculator()
    
    def calculate_magic_formula_score(self, financials: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculate Magic Formula metrics and score.
        
        The Magic Formula consists of:
        1. Earnings Yield (EBIT/Enterprise Value)
        2. Return on Capital (EBIT/Invested Capital)
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data for a ticker
            
        Returns:
            pd.DataFrame: DataFrame with Magic Formula metrics and score
        """
        try:
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            
            # Find common dates between balance sheet and income statement
            common_dates = balance_sheet.index.intersection(income_stmt.index)
            if common_dates.empty:
                self.logger.warning("No common dates between balance sheet and income statement")
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
                    continue
                
                # Calculate Enterprise Value
                ev = self.metrics_calculator.calculate_enterprise_value(financials, date)
                if ev is None or ev <= 0:
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
                    # Look for related columns
                    for col in ['property_plant_equipment_net', 'fixed_assets']:
                        if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                            net_fixed_assets = balance_sheet.loc[date, col]
                            break
                
                if net_fixed_assets is None:
                    continue
                
                # Calculate invested capital
                invested_capital = net_working_capital + net_fixed_assets
                if invested_capital <= 0:
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
                result.loc[date, 'Invested_Capital'] = invested_capital
                result.loc[date, 'Earnings_Yield'] = earnings_yield
                result.loc[date, 'Return_on_Capital'] = return_on_capital
                
                # Calculate normalized scores (higher is better)
                result.loc[date, 'Magic_Formula_Score'] = (
                    (earnings_yield - result['Earnings_Yield'].min()) / 
                    (result['Earnings_Yield'].max() - result['Earnings_Yield'].min() + 1e-10) +
                    (return_on_capital - result['Return_on_Capital'].min()) / 
                    (result['Return_on_Capital'].max() - result['Return_on_Capital'].min() + 1e-10)
                ) / 2
                
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Magic Formula score: {e}")
            return pd.DataFrame()
    
    def calculate_acquirers_multiple_score(self, financials: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculate Acquirer's Multiple metrics and score.
        
        The Acquirer's Multiple is defined as:
        Enterprise Value / Operating Earnings (EBIT)
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data for a ticker
            
        Returns:
            pd.DataFrame: DataFrame with Acquirer's Multiple metrics and score
        """
        try:
            income_stmt = financials['income_stmt']
            
            # Find dates with income statement data
            if income_stmt.empty:
                self.logger.warning("No income statement data")
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
                    continue
                
                # Calculate Enterprise Value
                ev = self.metrics_calculator.calculate_enterprise_value(financials, date)
                if ev is None or ev <= 0:
                    continue
                
                # Calculate Acquirer's Multiple
                acquirers_multiple = ev / ebit
                
                # Store results
                result.loc[date, 'EBIT'] = ebit
                result.loc[date, 'Enterprise_Value'] = ev
                result.loc[date, 'Acquirers_Multiple'] = acquirers_multiple
            
            # Calculate normalized score (lower multiple is better, so invert)
            if not result.empty and 'Acquirers_Multiple' in result.columns:
                result['Acquirers_Multiple_Score'] = 1 - (
                    (result['Acquirers_Multiple'] - result['Acquirers_Multiple'].min()) /
                    (result['Acquirers_Multiple'].max() - result['Acquirers_Multiple'].min() + 1e-10)
                )
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Acquirer's Multiple score: {e}")
            return pd.DataFrame()
    
    def calculate_piotroski_fscore(self, financials: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculate the Piotroski F-Score (9-point scale).
        
        The F-Score consists of 9 binary criteria in three categories:
        
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
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            
        Returns:
            pd.DataFrame: DataFrame with F-Score components and total
        """
        try:
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            cash_flow = financials['cash_flow']
            
            # Find dates with sufficient data
            common_dates = balance_sheet.index.intersection(income_stmt.index).intersection(cash_flow.index)
            if len(common_dates) < 2:
                self.logger.warning("Insufficient data for F-Score calculation")
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
                
                # Calculate normalized score (higher is better)
                result['F_Score_Normalized'] = result['F_Score'] / 9.0
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Piotroski F-Score: {e}")
            return pd.DataFrame()
    
    def calculate_graham_criteria(self, financials: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculate Graham's Defensive Investor criteria.
        
        Benjamin Graham's criteria for defensive investors include:
        1. Strong financial condition (Current Ratio > 2)
        2. Earnings stability (positive earnings)
        3. Dividend record
        4. Earnings growth
        5. Moderate P/E ratio (< 15)
        6. Moderate P/B ratio (< 1.5)
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            
        Returns:
            pd.DataFrame: DataFrame with Graham criteria
        """
        try:
            balance_sheet = financials['balance_sheet']
            income_stmt = financials['income_stmt']
            cash_flow = financials['cash_flow']
            aligned_prices = financials['aligned_prices']
            
            # Find common dates between all data sources
            common_dates = balance_sheet.index.intersection(income_stmt.index).intersection(aligned_prices.index)
            if common_dates.empty:
                self.logger.warning("No common dates between all data sources")
                return pd.DataFrame()
            
            # Create result DataFrame
            result = pd.DataFrame(index=common_dates)
            
            # Calculate metrics for each date
            for date in common_dates:
                result.loc[date, 'Price'] = aligned_prices.loc[date, 'close']
                
                # 1. Strong financial condition (Current Ratio > 2)
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
                
                # 2. Earnings stability (positive earnings)
                if earnings is not None:
                    result.loc[date, 'Positive_Earnings'] = earnings > 0
                
                # Calculate EPS for P/E ratio
                if earnings is not None and earnings > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    eps = earnings / shares_outstanding
                    result.loc[date, 'EPS'] = eps
                    
                    # 5. Moderate P/E ratio (< 15)
                    pe_ratio = aligned_prices.loc[date, 'close'] / eps
                    result.loc[date, 'P_E_Ratio'] = pe_ratio
                    result.loc[date, 'Moderate_P_E'] = pe_ratio < 15
                
                # 6. Moderate P/B ratio (< 1.5)
                book_value = None
                book_columns = ['common_stock_equity', 'stockholders_equity', 'total_equity_gross_minority_interest']
                for col in book_columns:
                    if col in balance_sheet.columns and not pd.isna(balance_sheet.loc[date, col]):
                        book_value = balance_sheet.loc[date, col]
                        break
                
                if book_value is not None and book_value > 0 and shares_outstanding is not None and shares_outstanding > 0:
                    book_value_per_share = book_value / shares_outstanding
                    pb_ratio = aligned_prices.loc[date, 'close'] / book_value_per_share
                    result.loc[date, 'P_B_Ratio'] = pb_ratio
                    result.loc[date, 'Moderate_P_B'] = pb_ratio < 1.5
                
                # 3. Dividend record
                dividend_paid = None
                dividend_columns = ['cash_dividends_paid', 'common_stock_dividend_paid']
                
                if date in cash_flow.index:
                    for col in dividend_columns:
                        if col in cash_flow.columns and not pd.isna(cash_flow.loc[date, col]):
                            dividend_paid = abs(cash_flow.loc[date, col])  # Usually recorded as negative
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
            
            # Calculate normalized score (higher is better)
            result['Graham_Score_Normalized'] = result['Graham_Score'] / len(criteria_columns)
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating Graham criteria: {e}")
            return pd.DataFrame()
    
    def calculate_deep_value_score(self, financials: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculate deep value metrics (P/E, P/B, Dividend Yield) and score.
        
        Args:
            financials (Dict[str, pd.DataFrame]): Financial data dictionary
            
        Returns:
            pd.DataFrame: DataFrame with deep value metrics and score
        """
        try:
            valuation_ratios = self.metrics_calculator.calculate_valuation_ratios(financials)
            
            if valuation_ratios.empty:
                self.logger.warning("No valuation ratios calculated")
                return pd.DataFrame()
            
            # Keep only the core ratios needed for deep value
            core_columns = [
                'Price', 'P_E_Ratio', 'P_B_Ratio', 'Dividend_Yield',
                'EPS', 'Book_Value_Per_Share', 'Dividend_Per_Share'
            ]
            
            result = pd.DataFrame(index=valuation_ratios.index)
            for col in core_columns:
                if col in valuation_ratios.columns:
                    result[col] = valuation_ratios[col]
            
            # Calculate normalized scores for each metric
            
            # P/E Score (lower is better)
            if 'P_E_Ratio' in result.columns:
                valid_pe = result['P_E_Ratio'].dropna()
                if not valid_pe.empty:
                    result['P_E_Score'] = 1 - (
                        (result['P_E_Ratio'] - valid_pe.min()) / 
                        (valid_pe.max() - valid_pe.min() + 1e-10)
                    )
                
            # P/B Score (lower is better)
            if 'P_B_Ratio' in result.columns:
                valid_pb = result['P_B_Ratio'].dropna()
                if not valid_pb.empty:
                    result['P_B_Score'] = 1 - (
                        (result['P_B_Ratio'] - valid_pb.min()) / 
                        (valid_pb.max() - valid_pb.min() + 1e-10)
                    )
            
            # Dividend Yield Score (higher is better)
            if 'Dividend_Yield' in result.columns:
                valid_dy = result['Dividend_Yield'].dropna()
                if not valid_dy.empty:
                    result['Dividend_Yield_Score'] = (
                        (result['Dividend_Yield'] - valid_dy.min()) / 
                        (valid_dy.max() - valid_dy.min() + 1e-10)
                    )
            
            # Calculate combined deep value score
            score_columns = [col for col in result.columns if col.endswith('_Score')]
            if score_columns:
                result['Deep_Value_Score'] = result[score_columns].mean(axis=1)
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating deep value score: {e}")
            return pd.DataFrame()
    
    def calculate_combined_value_score(self, ticker: str) -> pd.DataFrame:
        """
        Calculate a combined value score using multiple strategies.
        
        Args:
            ticker (str): Stock ticker symbol.
            
        Returns:
            pd.DataFrame: DataFrame with combined value score.
        """
        try:
            # Get financial data
            financials = self.data_manager.get_financial_data(ticker)
            if not financials.get('fin_dates').size:
                self.logger.warning(f"No financial dates found for {ticker}")
                return pd.DataFrame()
            
            # Calculate individual strategy scores
            magic_formula = self.calculate_magic_formula_score(financials)
            acquirers_multiple = self.calculate_acquirers_multiple_score(financials)
            piotroski = self.calculate_piotroski_fscore(financials)
            graham = self.calculate_graham_criteria(financials)
            deep_value = self.calculate_deep_value_score(financials)
            
            # Calculate advanced ratios
            all_ratios = self.metrics_calculator.calculate_all_ratios(financials)
            
            # Get all dates from all dataframes
            all_dates = set()
            for df in [magic_formula, acquirers_multiple, piotroski, graham, deep_value, all_ratios]:
                if not df.empty:
                    all_dates.update(df.index)
            
            if not all_dates:
                self.logger.warning(f"No valid dates found for combined score for {ticker}")
                return pd.DataFrame()
            
            # Create result DataFrame with all dates
            result = pd.DataFrame(index=sorted(all_dates))
            
            # Add scores from each strategy
            value_components = {
                'Magic_Formula': {
                    'source': magic_formula,
                    'score_column': 'Magic_Formula_Score',
                    'weight': self.params['weight_magic_formula']
                },
                'Acquirers_Multiple': {
                    'source': acquirers_multiple,
                    'score_column': 'Acquirers_Multiple_Score',
                    'weight': self.params['weight_acquirers_multiple']
                },
                'Piotroski': {
                    'source': piotroski,
                    'score_column': 'F_Score_Normalized',
                    'weight': self.params['weight_piotroski']
                },
                'Graham': {
                    'source': graham,
                    'score_column': 'Graham_Score_Normalized',
                    'weight': self.params['weight_graham']
                },
                'Deep_Value': {
                    'source': deep_value,
                    'score_column': 'Deep_Value_Score',
                    'weight': self.params['weight_deep_value']
                }
            }
            
            # Add component scores to result
            for component_name, component_data in value_components.items():
                source_df = component_data['source']
                score_column = component_data['score_column']
                
                if not source_df.empty and score_column in source_df.columns:
                    column_name = f"{component_name}_Score"
                    result[column_name] = np.nan
                    
                    for date in source_df.index:
                        if date in result.index and not pd.isna(source_df.loc[date, score_column]):
                            result.loc[date, column_name] = source_df.loc[date, score_column]
            
            # Add key ratios and metrics
            important_ratios = [
                'ROA', 'ROE', 'Current_Ratio', 'Debt_to_Equity',
                'Operating_Margin', 'Net_Profit_Margin',
                'P_E_Ratio', 'P_B_Ratio', 'EV_EBITDA', 'Dividend_Yield'
            ]
            
            for ratio in important_ratios:
                if not all_ratios.empty and ratio in all_ratios.columns:
                    result[ratio] = np.nan
                    for date in all_ratios.index:
                        if date in result.index:
                            result.loc[date, ratio] = all_ratios.loc[date, ratio]
            
            # Calculate combined score
            score_columns = [f"{component}_Score" for component in value_components.keys() if f"{component}_Score" in result.columns]
            
            if score_columns:
                # Calculate weighted score
                result['Combined_Value_Score'] = 0
                total_weight = 0
                
                for component, data in value_components.items():
                    column = f"{component}_Score"
                    if column in result.columns:
                        weight = data['weight']
                        result['Combined_Value_Score'] += result[column].fillna(0) * weight
                        total_weight += weight
                
                if total_weight > 0:
                    result['Combined_Value_Score'] = result['Combined_Value_Score'] / total_weight
            
            # Forward fill the most recent scores to ensure we have values for all dates
            result = result.ffill()
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error calculating combined value score for {ticker}: {e}")
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
        
