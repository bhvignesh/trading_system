# trading_system/src/strategies/magic_formula_strategy.py

"""
Magic Formula Strategy Module

This module implements Joel Greenblatt's Magic Formula strategy with historical analysis.
It processes the three main financial statements—income statement, balance sheet,
and cash flow—merges them with key price data available on the reporting dates,
and then calculates financial metrics needed for the Magic Formula.
For multi-ticker analyses, the strategy computes ranking (final score) for each reporting date,
which can be further used for ranking various tickers later in your trading system.

The strategy uses vectorized operations for speed and falls back to company info
only when the required fields are missing from the financial statements.
"""

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class MagicFormulaStrategy(BaseStrategy):
    """
    Implementation of Joel Greenblatt's Magic Formula strategy
    with historic analysis based on three financial statements.

    The strategy:
      - Processes historical income statement, balance sheet, and cash flow data.
      - Merges these statements on reporting dates and supplements missing 'cash'
        from cash flow data.
      - Retrieves historical price data for the dates in the financial statements
        (using merge_asof to obtain the nearest previous available close price).
      - Calculates key metrics: market cap, enterprise value, earnings yield, and return on capital.
      - Applies strategy-specific filters and, for multi-ticker inputs, computes a final score.
      - Supports both single and multiple ticker inputs using vectorized operations.

    Parameters:
        db_config: Database configuration settings.
        params: Optional strategy-specific parameters. Default parameters used are:
            - min_market_cap: $50M (default)
            - min_ebit: $0 (default)
            - min_enterprise_value: $0 (default)
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        super().__init__(db_config, params)
        self.params = {
            'min_market_cap': 50e6,
            'min_ebit': 0,
            'min_enterprise_value': 0
        }
        if params:
            self.params.update(params)

    def generate_signals(self, tickers: Union[str, List[str]], start_date: Optional[str] = None,
                         end_date: Optional[str] = None, initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate Magic Formula signals for the given ticker(s)
        across available financial statement dates.

        For multi-ticker input, the DataFrame is indexed by both ticker and date,
        and the final score is computed by ranking earnings yield and return on capital
        within each reporting date group.

        Args:
            tickers: Single ticker symbol (str) or list of ticker symbols.
            start_date: Not used (maintained for base class compatibility).
            end_date: Not used (maintained for base class compatibility).
            initial_position: Not used (maintained for base class compatibility).
            latest_only: If True, returns only the latest signal per ticker (for multi-ticker)
                         or the latest overall (for a single ticker).

        Returns:
            pd.DataFrame: DataFrame with columns including:
                - ebit, enterprise_value, earnings_yield, return_on_capital,
                  signal, and final_score.
        """
        try:
            # Retrieve and merge financial statements for the given ticker(s)
            financial_data = self._get_processed_financials(tickers)
            if financial_data.empty:
                return pd.DataFrame()

            # Merge with historical price data (using merge_asof to align dates)
            merged_data = self._merge_price_data(financial_data, tickers)
            if merged_data.empty:
                return pd.DataFrame()

            # Calculate financial ratios using vectorized operations
            calculated_data = self._calculate_metrics(merged_data)
            if calculated_data.empty:
                return pd.DataFrame()

            # Apply strategy filters based on thresholds
            filtered_data = self._apply_filters(calculated_data)

            # Generate signals and compute final score (ranking for multi-ticker)
            signals = self._generate_signals(filtered_data)

            # Return only the most recent signal(s) if requested
            if latest_only and not signals.empty:
                if isinstance(tickers, list):
                    # For multi-ticker, get most recent reporting date per ticker
                    signals = signals.reset_index().sort_values(by='date')\
                        .groupby('ticker', group_keys=False).tail(1).set_index(['ticker', 'date'])
                else:
                    signals = signals.iloc[[-1]]
                return signals.reset_index()
            return signals.reset_index()
        except DataRetrievalError as e:
            self.logger.error(f"Data retrieval error for {tickers}: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error processing {tickers}: {str(e)}")
            return pd.DataFrame()

    def _get_processed_financials(self, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and harmonize financial statement data from the
        income statement, balance sheet, and cash flow tables.

        The function standardizes the following required fields:
            - ebit (from income statement)
            - current_assets, current_liabilities, net_ppe,
              short_term_debt, long_term_debt, cash, shares_outstanding (from balance sheet)
            - (Optional) cash from cash flow is obtained from 'end_cash_position'
              as a fallback if the balance sheet field is missing.

        If any required field is not available in the three financial statements,
        the function attempts to use the company_info table to fill the gap.

        Args:
            tickers: Single ticker symbol or list of ticker symbols.

        Returns:
            pd.DataFrame: A merged DataFrame indexed by reporting date (or by ticker and date),
                          containing the standardized columns.
        """
        # Retrieve income statement and standardize the 'ebit' column
        income_stmt = self.get_financials(tickers, 'income_statement')
        if income_stmt.empty:
            return pd.DataFrame()
        income_stmt = self._standardize_columns(income_stmt, 'income', ['ebit'])

        # Retrieve balance sheet and standardize key columns
        balance_sheet = self.get_financials(tickers, 'balance_sheet')
        if balance_sheet.empty:
            return pd.DataFrame()
        balance_sheet = self._standardize_columns(balance_sheet, 'balance', [
            'current_assets', 'current_liabilities', 'net_ppe',
            'short_term_debt', 'long_term_debt', 'cash', 'shares_outstanding'
        ])

        # Retrieve cash flow statements; we will use 'end_cash_position' to fill missing cash data.
        cash_flow = self.get_financials(tickers, 'cash_flow')

        # Merge income statement and balance sheet on reporting date
        merged_fp = income_stmt.merge(balance_sheet, left_index=True, right_index=True, how='inner')

        # If cash flow data is available, join and fill missing 'cash' values.
        if not cash_flow.empty and 'end_cash_position' in cash_flow.columns:
            merged_fp = merged_fp.join(cash_flow[['end_cash_position']], how='left')
            merged_fp['cash'] = merged_fp['cash'].fillna(merged_fp['end_cash_position'])
            merged_fp.drop(columns=['end_cash_position'], inplace=True)

        # For any missing required columns, attempt to use fallback data from company_info.
        for col in ['ebit', 'current_assets', 'current_liabilities', 'net_ppe',
                    'short_term_debt', 'long_term_debt', 'cash', 'shares_outstanding']:
            if col not in merged_fp.columns or merged_fp[col].isna().all():
                try:
                    comp_info = self.get_company_info(tickers)
                    # For multi-ticker, comp_info is a DataFrame indexed by ticker;
                    # for a single ticker, comp_info is a Series.
                    def fill_value(row):
                        ticker_val = row.name[0] if isinstance(row.name, tuple) else None
                        if ticker_val and ticker_val in comp_info.index:
                            if col == 'shares_outstanding' and 'sharesoutstanding' in comp_info.columns:
                                return comp_info.loc[ticker_val]['sharesoutstanding']
                        # Otherwise, no value
                        return row[col] if col in row else np.nan

                    merged_fp[col] = merged_fp.apply(fill_value, axis=1)
                except Exception as e:
                    self.logger.warning(f"Missing {col} and unable to retrieve fallback from company_info: {e}")
        return merged_fp

    def _standardize_columns(self, df: pd.DataFrame, stmt_type: str, fields: List[str]) -> pd.DataFrame:
        """
        Standardize and map alternative column names from raw financial statement data
        to the names needed for Magic Formula calculations.

        Args:
            df: Input DataFrame containing raw financial data.
            stmt_type: One of 'income' or 'balance' (or 'cash_flow' if needed).
            fields: List of standardized field names to extract.

        Returns:
            pd.DataFrame: DataFrame containing only the standardized fields.
        """
        column_map = {
            'income': {
                'ebit': ['ebit', 'operating_income']
            },
            'balance': {
                'current_assets': ['current_assets', 'total_current_assets'],
                'current_liabilities': ['current_liabilities', 'total_current_liabilities'],
                'net_ppe': ['net_ppe', 'property_plant_equipment_net'],
                'short_term_debt': ['short_term_debt', 'current_debt'],
                'long_term_debt': ['long_term_debt', 'noncurrent_debt'],
                'cash': ['cash', 'cash_and_cash_equivalents'],
                'shares_outstanding': ['shares_outstanding', 'common_stock_shares_outstanding']
            },
            'cash_flow': {
                'cash': ['end_cash_position']
            }
        }

        standardized = pd.DataFrame(index=df.index)
        for field in fields:
            if stmt_type not in column_map or field not in column_map[stmt_type]:
                self.logger.warning(f"No mapping for field '{field}' in statement type '{stmt_type}'")
                continue
            possible_names = column_map[stmt_type][field]
            available = [name for name in possible_names if name in df.columns]
            if not available:
                self.logger.warning(f"Missing field '{field}' in {stmt_type} data")
                return pd.DataFrame()
            standardized[field] = df[available[0]]
        return standardized

    def _merge_price_data(self, financial_data: pd.DataFrame,
                          tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Merge the financial statement data with historical price data
        for the corresponding reporting dates. Uses a merge_asof operation
        (with an optional 'by' for multi-ticker data) so that if the exact reporting
        date is not available in the price data, the nearest previous price is used.

        Args:
            financial_data: DataFrame containing financial data and reporting dates.
            tickers: A single ticker symbol or list of ticker symbols.

        Returns:
            pd.DataFrame: Merged DataFrame with an additional 'close' price column.
        """
        # Reset index to bring 'date' (and possibly 'ticker') to columns.
        df = financial_data.reset_index()
        # Determine boundary dates from financial statement dates.
        min_date = df['date'].min().strftime('%Y-%m-%d')
        max_date = df['date'].max().strftime('%Y-%m-%d')
        if isinstance(tickers, list):
            prices = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
            prices = prices.reset_index()
            prices = prices.sort_values(by=['ticker', 'date'])
            df = df.sort_values(by=['ticker', 'date'])
            # Use merge_asof to align each financial date with the nearest price (backward)
            merged = pd.merge_asof(df, prices, on='date', by='ticker', direction='backward')
        else:
            prices = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
            prices = prices.reset_index()
            prices = prices.sort_values(by='date')
            df = df.sort_values(by='date')
            merged = pd.merge_asof(df, prices, on='date', direction='backward')
        # Reset index appropriately: use a MultiIndex if multi-ticker input.
        if 'ticker' in merged.columns:
            merged.set_index(['ticker', 'date'], inplace=True)
        else:
            merged.set_index('date', inplace=True)
        merged = merged.dropna(subset=['close'])
        return merged

    def _calculate_metrics(self, merged_data: pd.DataFrame) -> pd.DataFrame:
        """
        Compute key Magic Formula metrics via vectorized operations.

        Metrics computed:
          - market_cap = shares_outstanding * close price
          - enterprise_value = market_cap + short_term_debt + long_term_debt - cash
          - earnings_yield = ebit / enterprise_value
          - return_on_capital = ebit / [(current_assets - current_liabilities) + net_ppe]

        Rows with infinite or missing values are dropped.

        Args:
            merged_data: DataFrame containing financial data merged with price data.

        Returns:
            pd.DataFrame: DataFrame including the computed metrics.
        """
        data = merged_data.copy()
        data['market_cap'] = data['shares_outstanding'] * data['close']
        data['enterprise_value'] = data['market_cap'] + data['short_term_debt'] + data['long_term_debt'] - data['cash']
        data['earnings_yield'] = data['ebit'] / data['enterprise_value']
        # Note: Working capital = current_assets - current_liabilities
        data['return_on_capital'] = data['ebit'] / ((data['current_assets'] - data['current_liabilities']) + data['net_ppe'])
        data = data.replace([np.inf, -np.inf], np.nan).dropna()
        return data

    def _apply_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply strategy-specific filters to the data.

        The filters are:
          - market_cap must be above or equal to min_market_cap
          - ebit must be at least min_ebit
          - enterprise_value must be above or equal to min_enterprise_value
          - (current_assets - current_liabilities + net_ppe) > 0

        Args:
            data: DataFrame with computed financial metrics.

        Returns:
            pd.DataFrame: DataFrame containing only rows that pass the filters.
        """
        mask = (
            (data['market_cap'] >= self.params['min_market_cap']) &
            (data['ebit'] >= self.params['min_ebit']) &
            (data['enterprise_value'] >= self.params['min_enterprise_value']) &
            ((data['current_assets'] - data['current_liabilities'] + data['net_ppe']) > 0)
        )
        return data[mask]

    def _generate_signals(self, filtered_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals and a final score based on financial metrics.

        For multi-ticker input, the function:
          - Resets the index to identify reporting dates and tickers.
          - Ranks earnings_yield and return_on_capital (descending rank—higher is better)
            within each reporting date.
          - Computes final_score as the sum of the two ranks (a lower combined rank is better).
          - Sets a binary signal (1 if both earnings_yield and return_on_capital are positive; else 0).

        For single-ticker input, the final_score is computed as the sum of
        earnings_yield and return_on_capital.

        Args:
            filtered_data: DataFrame that passed all filters.

        Returns:
            pd.DataFrame: DataFrame containing columns for ebit, enterprise_value,
                          earnings_yield, return_on_capital, signal, final_score,
                          and (for multi-ticker) the rank columns.
        """
        data = filtered_data.copy()
        if 'ticker' in data.index.names:
            # For multi-ticker: compute ranking within each reporting date group.
            data = data.reset_index()
            data['ey_rank'] = data.groupby('date')['earnings_yield'].rank(method='min', ascending=False)
            data['roc_rank'] = data.groupby('date')['return_on_capital'].rank(method='min', ascending=False)
            data['final_score'] = data['ey_rank'] + data['roc_rank']
            data['signal'] = np.where((data['earnings_yield'] > 0) & (data['return_on_capital'] > 0), 1, 0)
            return data.set_index(['ticker', 'date'])
        else:
            # For single ticker input.
            data['signal'] = np.where((data['earnings_yield'] > 0) & (data['return_on_capital'] > 0), 1, 0)
            data['final_score'] = data['earnings_yield'] + data['return_on_capital']
            return data