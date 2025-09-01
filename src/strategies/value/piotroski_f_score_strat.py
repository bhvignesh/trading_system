# trading_system/src/strategies/piotroski_fscore_strategy.py

from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class PiotroskiFScoreStrategy(BaseStrategy):
    """
    Implements the Piotroski F-Score strategy using vectorized operations for multiple tickers 
    and multiple financial dates.

    The strategy computes nine binary signals based on changes in profitability, leverage, liquidity,
    equity dilution, and operational efficiency. Specifically, the criteria are:

    Profitability:
      1. Positive Return on Assets (ROA = Net Income / Total Assets)
      2. Positive Operating Cash Flow (OCF > 0)
      3. Improvement in ROA (current ROA > previous period ROA)
      4. OCF exceeds Net Income (OCF > Net Income)

    Leverage & Liquidity:
      5. Declining long-term debt ratio (Long Term Debt / Total Assets decreases)
      6. Improving liquidity (Current Ratio = Current Assets / Current Liabilities increases)

    Equity Dilution:
      7. No new shares issued (shares outstanding, taken here as ordinary_shares_number, does not increase)

    Operational Efficiency:
      8. Improving gross margin (Gross Margin = Gross Profit / Total Revenue increases)
      9. Improving asset turnover (Asset Turnover = Total Revenue / Total Assets increases)

    The final F-Score is the sum of these binary signals. The output DataFrame includes:
      - ticker: Stock ticker symbol.
      - date: Financial statement date.
      - market_cap: Calculated as shares outstanding * price.
      - f_score: Sum of the binary criteria.
      - signal: Trading signal (1 for strong, 0.5 for moderate, 0 otherwise).
      - price: Price on the financial statement date.
    
    Attributes:
        db_config: Database configuration settings.
        params: Strategy-specific parameters including:
            - min_market_cap (float): Minimum market cap threshold.
            - strong_score (int): F-Score threshold for a strong signal.
            - moderate_score (int): F-Score threshold for a moderate signal.
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Piotroski F-Score strategy.

        Args:
            db_config: Database configuration settings.
            params (Optional[Dict]): Strategy-specific parameters.
                - min_market_cap (float): Minimum market capitalization threshold.
                - strong_score (int): F-Score threshold for a strong signal.
                - moderate_score (int): F-Score threshold for a moderate signal.
        """
        super().__init__(db_config, params)
        self.params = params or {
            'min_market_cap': 100e6,
            'strong_score': 7,
            'moderate_score': 5
        }
        # Updated balance sheet mapping: Use 'ordinary_shares_number'
        self._column_map = {
            'income': ['net_income', 'gross_profit', 'total_revenue'],
            'balance': ['total_assets', 'long_term_debt', 'current_assets', 'current_liabilities', 'ordinary_shares_number'],
            'cashflow': ['operating_cash_flow']
        }

    def generate_signals(self, tickers: Union[str, List[str]], start_date: Optional[str] = None,
                         end_date: Optional[str] = None, initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate historical Piotroski F-Score trading signals for provided ticker(s) across available financial
        statement dates.

        Args:
            tickers (Union[str, List[str]]): A single ticker symbol or list of ticker symbols to process.
            start_date (str, optional): Start date (YYYY-MM-DD) for filtering price data.
            end_date (str, optional): End date (YYYY-MM-DD) for filtering price data.
            initial_position (int): Starting position (unused in this strategy).
            latest_only (bool): If True, returns only the most recent signal per ticker.

        Returns:
            pd.DataFrame: DataFrame containing columns [ticker, date, market_cap, f_score, signal, price].
        """
        try:
            # Retrieve combined financial data (from income, balance, and cash flow statements)
            financial_data = self._get_combined_financials(tickers)
            if financial_data.empty:
                return pd.DataFrame()

            # Enrich with historical price data and compute market capitalization.
            enriched_data = self._enrich_with_prices(tickers, financial_data, start_date, end_date)
            if enriched_data.empty:
                return pd.DataFrame()

            # Calculate all F-Score components using vectorized (grouped) operations.
            scored_data = self._calculate_fscore_components(enriched_data)
            
            # Filter by market capitalization threshold.
            filtered_data = scored_data[scored_data['market_cap'] >= self.params['min_market_cap']]
            if filtered_data.empty:
                self.logger.info(f"Tickers {tickers} below market cap threshold")
                return pd.DataFrame()

            # Format final output and optionally return only the latest data per ticker.
            final_output = self._format_output(filtered_data, tickers, latest_only)
            return final_output

        except DataRetrievalError as e:
            self.logger.error(f"Data retrieval error for {tickers}: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Processing error for {tickers}: {str(e)}")
            return pd.DataFrame()

    def _get_combined_financials(self, tickers: Union[str, List[str]]) -> pd.DataFrame:
        """
        Retrieve and combine financial statement data from income statement, balance sheet, and cash flow.
        The data are aligned on the financial statement dates (using a MultiIndex if multiple tickers) and
        forward-filled groupwise.

        Args:
            tickers (Union[str, List[str]]): A ticker symbol or list of ticker symbols.

        Returns:
            pd.DataFrame: Combined DataFrame with columns named like '<column>_<statement_type>'.
        """
        statements = {
            'income': self.get_financials(tickers, 'income_statement', lookback=None),
            'balance': self.get_financials(tickers, 'balance_sheet', lookback=None),
            'cashflow': self.get_financials(tickers, 'cash_flow', lookback=None)
        }

        # Validate that all financial statements are retrieved.
        if any(df.empty for df in statements.values()):
            self.logger.warning(f"Incomplete financial data for tickers: {tickers}")
            return pd.DataFrame()

        # Append a suffix (e.g., _income, _balance, _cashflow) to each column and concatenate.
        combined = pd.concat(
            [df.add_suffix(f'_{stmt}') for stmt, df in statements.items()],
            axis=1
        ).sort_index()

        # Forward-fill missing values; if multi-ticker, perform groupwise ffill.
        if isinstance(combined.index, pd.MultiIndex) and 'ticker' in combined.index.names:
            combined = combined.groupby(level=0).ffill()
        else:
            combined = combined.ffill()

        # Select only the required combined financial columns.
        required_cols = self._get_required_columns()
        combined = combined[required_cols]
        return combined

    def _get_required_columns(self) -> list:
        """
        Generate a list of required columns from the combined financials based on the defined column map.

        Returns:
            list: List of column names (e.g. 'net_income_income', 'total_assets_balance', ...).
        """
        return [f"{col}_{stmt}" 
                for stmt, cols in self._column_map.items() 
                for col in cols]

    def _enrich_with_prices(self, tickers: Union[str, List[str]], financial_data: pd.DataFrame,
                            start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Merge the combined financial data with historical price data (from the daily_prices table)
        based on the financial statement dates. Also computes market capitalization using shares outstanding.
        
        Args:
            tickers (Union[str, List[str]]): A ticker symbol or list of ticker symbols.
            financial_data (pd.DataFrame): Combined financial statement data.
            start_date (str, optional): Filter start date (YYYY-MM-DD) for price data.
            end_date (str, optional): Filter end date (YYYY-MM-DD) for price data.
        
        Returns:
            pd.DataFrame: The enriched DataFrame now containing price and market_cap.
        """
        # Determine the date range from financial statement dates.
        if isinstance(financial_data.index, pd.MultiIndex):
            date_index = financial_data.index.get_level_values(-1)
        else:
            date_index = financial_data.index

        # Use provided date filters if available; otherwise take min/max from financial dates.
        from_date = start_date if start_date else date_index.min().strftime("%Y-%m-%d")
        to_date = end_date if end_date else date_index.max().strftime("%Y-%m-%d")

        prices = self.get_historical_prices(
            tickers, 
            from_date=from_date, 
            to_date=to_date
        )
        
        # Rename the 'close' column to 'price' for clarity.
        prices = prices.rename(columns={'close': 'price'})
        if prices.empty:
            self.logger.warning(f"No price data for tickers: {tickers}")
            return pd.DataFrame()

        # Join price data with financial data using their indices.
        enriched = financial_data.join(prices['price'], how='left')
        
        # Determine the correct shares field for market cap calculation.
        shares_field = next((col for col in ['ordinary_shares_number_balance', 'common_stock_balance', 'share_issued_balance']
                             if col in enriched.columns), None)
        if not shares_field:
            self.logger.warning("Shares field not found in financial data.")
            return pd.DataFrame()

        enriched['market_cap'] = enriched[shares_field] * enriched['price']
        return enriched.dropna(subset=['market_cap', 'price'])

    def _calculate_fscore_components(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Compute each of the nine Piotroski F-Score components using vectorized calculations.
        For multi-ticker data the shifts are done groupwise.

        Args:
            data (pd.DataFrame): Enriched financial data with price and market cap.
        
        Returns:
            pd.DataFrame: DataFrame with additional columns for each criterion and the final f_score.
        """
        # Profitability metrics
        data['roa'] = data['net_income_income'] / data['total_assets_balance']
        data['positive_roa'] = (data['roa'] > 0).astype(int)
        data['positive_ocf'] = (data['operating_cash_flow_cashflow'] > 0).astype(int)
        prev_roa = data.groupby(level=0)['roa'].shift(1) if 'ticker' in data.index.names else data['roa'].shift(1)
        data['increasing_roa'] = (data['roa'] > prev_roa).astype(int)
        data['ocf_exceeds_ni'] = (data['operating_cash_flow_cashflow'] > data['net_income_income']).astype(int)

        # Leverage & Liquidity metrics
        data['debt_ratio'] = data['long_term_debt_balance'] / data['total_assets_balance']
        prev_debt_ratio = data.groupby(level=0)['debt_ratio'].shift(1) if 'ticker' in data.index.names else data['debt_ratio'].shift(1)
        data['decreasing_leverage'] = (data['debt_ratio'] < prev_debt_ratio).astype(int)
        data['current_ratio'] = data['current_assets_balance'] / data['current_liabilities_balance']
        prev_current_ratio = data.groupby(level=0)['current_ratio'].shift(1) if 'ticker' in data.index.names else data['current_ratio'].shift(1)
        data['improving_liquidity'] = (data['current_ratio'] > prev_current_ratio).astype(int)

        # Share issuance metric (no dilution)
        prev_shares = data.groupby(level=0)['ordinary_shares_number_balance'].shift(1) if 'ticker' in data.index.names else data['ordinary_shares_number_balance'].shift(1)
        data['no_new_shares'] = (data['ordinary_shares_number_balance'] <= prev_shares).astype(int)

        # Operational efficiency metrics
        data['gross_margin'] = data['gross_profit_income'] / data['total_revenue_income']
        prev_gross_margin = data.groupby(level=0)['gross_margin'].shift(1) if 'ticker' in data.index.names else data['gross_margin'].shift(1)
        data['improving_margin'] = (data['gross_margin'] > prev_gross_margin).astype(int)
        data['asset_turnover'] = data['total_revenue_income'] / data['total_assets_balance']
        prev_asset_turnover = data.groupby(level=0)['asset_turnover'].shift(1) if 'ticker' in data.index.names else data['asset_turnover'].shift(1)
        data['improving_turnover'] = (data['asset_turnover'] > prev_asset_turnover).astype(int)

        # Sum all criteria to obtain the final F-Score.
        criteria = ['positive_roa', 'positive_ocf', 'increasing_roa', 'ocf_exceeds_ni',
                    'decreasing_leverage', 'improving_liquidity', 'no_new_shares',
                    'improving_margin', 'improving_turnover']
        data['f_score'] = data[criteria].sum(axis=1)
        return data

    def _format_output(self, data: pd.DataFrame, tickers: Union[str, List[str]], latest_only: bool) -> pd.DataFrame:
        """
        Format the final output DataFrame with key columns and generate trading signals based on the F-Score.

        Args:
            data (pd.DataFrame): DataFrame with calculated f_score and market cap.
            tickers (Union[str, List[str]]): The processed ticker(s).
            latest_only (bool): If True, returns only the most recent signal per ticker.

        Returns:
            pd.DataFrame: Final DataFrame with columns [ticker, date, market_cap, f_score, signal, price].
        """
        # Define signals: strong (f_score >= strong_score) and moderate (>= moderate_score but below strong).
        strong_signal = data['f_score'] >= self.params['strong_score']
        moderate_signal = (data['f_score'] >= self.params['moderate_score']) & (data['f_score'] < self.params['strong_score'])
        data['signal'] = np.select(
            [strong_signal, moderate_signal],
            [1, 0.5],
            default=0
        )
        # Reset the index (if MultiIndex, to have ticker and date as columns).
        data = data.reset_index()
        if 'date' not in data.columns:
            data.rename(columns={'index': 'date'}, inplace=True)
        # In case of a single ticker, add a ticker column.
        if 'ticker' not in data.columns:
            data['ticker'] = tickers if isinstance(tickers, str) else tickers[0]
        
        # If only the most recent signal is desired, group by ticker and pick the last record.
        if latest_only:
            data = data.sort_values(by='date').groupby('ticker', as_index=False).last()

        return data[['ticker', 'date', 'market_cap', 'f_score', 'signal', 'price']]