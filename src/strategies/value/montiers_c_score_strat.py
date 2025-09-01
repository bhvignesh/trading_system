# trading_strategy/src/strategies/value/montiers_c_score_strat.py

# TODO: Use with deep value strats

# TODO: Verify

"""
Implementation of Montier's C-Score (For Avoiding Value Traps) Strategy.

James Montier’s C-Score (Cheat Score) is designed to identify accounting red flags 
that might signal a value trap. This implementation uses six signals based on fundamental
data from the income statement, cash flow and balance sheet. For each reporting date,
the following red flags are computed:
  
  1. High accruals: If accruals (net income minus operating cash flow) divided by total assets
     exceed a threshold (default 10%).
  2. Negative net income: If net income is zero or negative.
  3. Negative operating cash flow: If operating cash flow is zero or negative.
  4. Abnormally high receivables growth: If quarter-over-quarter percentage growth in accounts receivable
     exceeds a threshold (default 30%).
  5. Abnormally high inventory growth: If quarter-over-quarter percentage growth in inventory
     exceeds a threshold (default 30%).
  6. Marked deterioration in working capital: If working capital declines by more than a threshold 
     (default 10% drop quarter-over-quarter).

Each red flag is scored as 1 if the condition is met and 0 otherwise. The final C-Score is 
the sum of these six flags. In addition, the script retrieves historical prices for the financial 
dates so that one could monitor the trend over time.

This strategy complies with the BaseStrategy abstract class.

Default parameters provided below are optimized for the Indian market (NSE/BSE), but you can override
them via the params dict.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class MontiersCScoreStrategy(BaseStrategy):
    """
    Montier's C-Score Strategy to avoid potential value traps by detecting fundamental red flags.

    The strategy computes six red flags using historical data from the income statement, cash flow, and
    balance sheet. In addition, it obtains historical closing prices corresponding to the reporting dates.
    
    Attributes:
        params (dict): Dictionary containing strategy parameters:
            - 'accrual_ratio_threshold': Threshold for accrual ratio (default 0.1).
            - 'receivables_growth_threshold': Threshold for receivables percentage growth (default 0.3).
            - 'inventory_growth_threshold': Threshold for inventory percentage growth (default 0.3).
            - 'working_capital_decline_threshold': Threshold for working capital decline (default 0.1).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Montier's C-Score strategy.
        
        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override default parameters.
        """
        super().__init__(db_config, params)
        # Set default parameters optimized for the Indian market (NSE/BSE)
        default_params = {
            'accrual_ratio_threshold': 0.10,         # accrual = (net_income - operating_cash_flow) / total_assets
            'receivables_growth_threshold': 0.30,      # >30% growth flags as red flag
            'inventory_growth_threshold': 0.30,        # >30% growth flags as red flag
            'working_capital_decline_threshold': 0.10    # >10% decline quarter-over-quarter flags as red flag
        }
        # Update defaults with any custom parameters provided
        self.params = {**default_params, **(params or {})}

    def generate_signals(
        self,
        tickers: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate historical trading signals based on Montier's C-Score strategy.
        
        Retrieves financial statements (income statement, cash flow, balance sheet) for the provided
        ticker(s), aligns the financial reporting dates with historical closing prices, computes six
        red flags (each scored 0/1) and outputs the final C-Score (ranging from 0 to 6).
        
        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent available signal per ticker.
            
        Returns:
            pd.DataFrame: DataFrame containing:
                ['ticker', 'date', 'net_income', 'operating_cash_flow', 'total_assets', 
                 'accrual_ratio', 'flag_accrual', 'flag_negative_net_income',
                 'flag_negative_ocf', 'receivables_growth', 'flag_receivables_growth',
                 'inventory_growth', 'flag_inventory_growth', 'working_capital_change', 
                 'flag_working_capital_decline', 'c_score', 'close']
        """
        try:
            # Retrieve all available financial data for the ticker(s)
            income_df = self.get_financials(tickers, 'income_statement', lookback=None)
            cf_df = self.get_financials(tickers, 'cash_flow', lookback=None)
            bs_df = self.get_financials(tickers, 'balance_sheet', lookback=None)

            # Ensure we have sufficient data across all three statements
            if income_df.empty or cf_df.empty or bs_df.empty:
                self.logger.warning(f"Incomplete financial data for {tickers}")
                return pd.DataFrame()

            # Reset indices to ensure 'ticker' and 'date' are columns
            if isinstance(income_df.index, pd.MultiIndex):
                income_df = income_df.reset_index()
            if isinstance(cf_df.index, pd.MultiIndex):
                cf_df = cf_df.reset_index()
            if isinstance(bs_df.index, pd.MultiIndex):
                bs_df = bs_df.reset_index()

            # Merge income statement and cash flow data on ticker and date
            merged = pd.merge(
                income_df[['ticker', 'date', 'net_income']],
                cf_df[['ticker', 'date', 'operating_cash_flow']],
                on=['ticker', 'date'],
                how='inner'
            )

            # Merge with balance sheet data, retaining columns needed for the C-Score.
            bs_cols = ['ticker', 'date', 'total_assets', 'accounts_receivable', 'inventory', 'working_capital']
            merged = pd.merge(
                merged,
                bs_df[bs_cols],
                on=['ticker', 'date'],
                how='inner'
            )

            # Ensure the date column is in datetime format.
            merged['date'] = pd.to_datetime(merged['date'])

            # ---------------------- RED FLAG CALCULATIONS ---------------------------

            # Flag 1: High Accruals
            # Compute accrual = net_income - operating_cash_flow, then accrual_ratio = accrual / total_assets.
            # (If total_assets is zero, replace it with NaN to avoid division by zero.)
            merged['accrual'] = merged['net_income'] - merged['operating_cash_flow']
            merged['accrual_ratio'] = merged['accrual'] / merged['total_assets'].replace(0, np.nan)
            flag_accrual = (merged['accrual_ratio'] > self.params.get('accrual_ratio_threshold', 0.10)).astype(int)

            # Flag 2: Negative Net Income
            flag_negative_net_income = (merged['net_income'] <= 0).astype(int)

            # Flag 3: Negative Operating Cash Flow
            flag_negative_ocf = (merged['operating_cash_flow'] <= 0).astype(int)

            # For flags 4-6 we need to compare the current quarter with the previous quarter.
            # Ensure data are sorted by ticker and date.
            merged.sort_values(['ticker', 'date'], inplace=True)

            # Flag 4: Abnormal Receivables Growth (> threshold)
            # Calculate percentage change in accounts_receivable.
            merged['receivables_growth'] = merged.groupby('ticker')['accounts_receivable'].pct_change()
            flag_receivables_growth = merged['receivables_growth'].fillna(0).apply(
                lambda x: 1 if x > self.params.get('receivables_growth_threshold', 0.30) else 0
            ).astype(int)

            # Flag 5: Abnormal Inventory Growth (> threshold)
            merged['inventory_growth'] = merged.groupby('ticker')['inventory'].pct_change()
            flag_inventory_growth = merged['inventory_growth'].fillna(0).apply(
                lambda x: 1 if x > self.params.get('inventory_growth_threshold', 0.30) else 0
            ).astype(int)

            # Flag 6: Decline in Working Capital
            # Compute percentage change in working capital. If it declines by
            # more than the threshold, flag the observation.
            merged['working_capital_change'] = merged.groupby('ticker')['working_capital'].pct_change()
            flag_working_capital_decline = merged['working_capital_change'].fillna(0).apply(
                lambda x: 1 if x <= -self.params.get('working_capital_decline_threshold', 0.10) else 0
            ).astype(int)

            # Sum up all six red flags to create the final C-Score.
            merged['c_score'] = (
                flag_accrual +
                flag_negative_net_income +
                flag_negative_ocf +
                flag_receivables_growth +
                flag_inventory_growth +
                flag_working_capital_decline
            )

            # Add flag columns to the DataFrame for clarity.
            merged['flag_accrual'] = flag_accrual
            merged['flag_negative_net_income'] = flag_negative_net_income
            merged['flag_negative_ocf'] = flag_negative_ocf
            merged['flag_receivables_growth'] = flag_receivables_growth
            merged['flag_inventory_growth'] = flag_inventory_growth
            merged['flag_working_capital_decline'] = flag_working_capital_decline

            # ------------------- PRICE DATA ALIGNMENT -------------------------------
            # Retrieve historical price data for the financial statement dates.
            price_data = self._get_price_data(merged, tickers)
            if price_data.empty:
                self.logger.warning("Price data retrieval failed for the given date range.")
            else:
                # Merge the price data using merge_asof to align each financial date with the
                # most recent closing price.
                merged = pd.merge_asof(
                    merged.sort_values(['ticker', 'date']),
                    price_data.sort_values(['ticker', 'date']),
                    on='date',
                    by='ticker',
                    direction='backward'
                )

            # ------------------- DATE FILTERING & FINAL OUTPUT ----------------------
            # Apply date range filtering if provided.
            if start_date:
                merged = merged[merged['date'] >= pd.to_datetime(start_date)]
            if end_date:
                merged = merged[merged['date'] <= pd.to_datetime(end_date)]

            # If only the latest signal is desired, select the most recent record per ticker.
            if latest_only:
                merged = merged.sort_values('date').groupby('ticker').tail(1)

            # Optional: Rearrange or select columns to return.
            final_columns = [
                'ticker', 'date', 'net_income', 'operating_cash_flow', 'total_assets',
                'accrual_ratio', 'flag_accrual', 'flag_negative_net_income', 'flag_negative_ocf',
                'receivables_growth', 'flag_receivables_growth',
                'inventory_growth', 'flag_inventory_growth',
                'working_capital_change', 'flag_working_capital_decline',
                'c_score', 'close'
            ]
            return merged[final_columns].reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error generating Montier's C-Score for tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _get_price_data(self, merged_data: pd.DataFrame, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve historical closing price data for financial statement dates.
        
        Uses the BaseStrategy's get_historical_prices method to pull closing price data
        for the period covering the financial statement dates and then prepares the data
        so that it can be aligned with the fundamentals.
        
        Args:
            merged_data (pd.DataFrame): DataFrame containing financial statement dates.
            tickers (Union[str, List[str]]): Single ticker or list of tickers.
        
        Returns:
            pd.DataFrame: DataFrame with columns ['ticker', 'date', 'close'].
        """
        if merged_data.empty:
            return pd.DataFrame()

        # Determine the date range from the fundamentals data.
        min_date = merged_data['date'].min().strftime('%Y-%m-%d')
        max_date = merged_data['date'].max().strftime('%Y-%m-%d')

        # Retrieve historical price data over the determined date range.
        price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
        if price_df.empty:
            return pd.DataFrame()

        # Reset index (if necessary) so that 'ticker' and 'date' are explicit columns.
        price_df = price_df.reset_index()
        if 'ticker' not in price_df.columns:
            # For single ticker cases.
            price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

        price_df['date'] = pd.to_datetime(price_df['date'])
        price_df.sort_values(['ticker', 'date'], inplace=True)
        return price_df[['ticker', 'date', 'close']]