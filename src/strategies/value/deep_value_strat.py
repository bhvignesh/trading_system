# trading_system/src/strategies/deep_value_strategy.py

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class DeepValueStrategy(BaseStrategy):
    """
    Optimized Deep Value investing strategy using vectorized operations and historical financial statement dates.
    
    The strategy retrieves and merges data from the three key financial statements:
    balance sheet, income statement, and cash flow. It then obtains historical closing prices 
    on these reporting dates and computes four key metrics:
    
        - PE Ratio: price / diluted_eps (fallback to trailingeps if missing)
        - PB Ratio: price / (tangible_book_value / ordinary_shares_number) [fallback using company_info bookvalue/sharesoutstanding]
        - Dividend Yield: (cash_dividends_paid / ordinary_shares_number) / price (fallback using company_info dividendrate)
        - Debt-to-Equity: total_debt / stockholders_equity (fallback using company_info debttoequity)
    
    The metrics are scored (using configurable thresholds and weights), and a composite value score is computed.
    Final output includes market cap, calculated ratios, composite value_score, and a signal based on the score.
    
    Attributes:
        db_config (DatabaseConfig): Database configuration settings.
        params (dict): Strategy-specific parameters.
        logger (logging.Logger): Logger instance.
        db_engine: SQLAlchemy engine for database connections.
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize DeepValueStrategy with database configuration and parameters.
        
        Args:
            db_config: Database configuration settings.
            params (dict, optional): Additional strategy parameters.
                The following default parameters are used if not overridden:
                    - min_market_cap: $100M minimum market capitalization.
                    - pe_thresholds: [10, 15, 20]
                    - pb_thresholds: [1.0, 1.5, 2.0]
                    - div_thresholds: [0.04, 0.03, 0.02]
                    - de_thresholds: [0.3, 0.6, 1.0]
                    - score_weights: [1.0, 1.0, 0.8, 0.7] for PE, PB, Dividend, DE metrics.
                    - min_statements: Minimum count of fundamental fields required per row.
        """
        super().__init__(db_config, params)
        self.ratio_columns = ['pe_ratio', 'pb_ratio', 'dividend_yield', 'debt_to_equity']
        self.default_params = {
            'min_market_cap': 1e8,  # $100M default market cap cutoff
            'pe_thresholds': [10, 15, 20],
            'pb_thresholds': [1.0, 1.5, 2.0],
            'div_thresholds': [0.04, 0.03, 0.02],
            'de_thresholds': [0.3, 0.6, 1.0],
            'score_weights': [1.0, 1.0, 0.8, 0.7],  # Weights for PE, PB, Dividend, DE respectively
            'min_statements': 3  # Minimum count of fundamental fields required
        }
        self.params = {**self.default_params, **(params or {})}

    def generate_signals(self, tickers: Union[str, List[str]], 
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         initial_position: int = 0,
                         latest_only: bool = False) -> pd.DataFrame:
        """
        Generate trading signals for one or more tickers using financial statement dates.
        
        The method performs the following steps:
            1. Retrieves and merges financial statements across balance sheet, income statement, and cash flow.
            2. Obtains historical prices on these financial reporting dates.
            3. Merges fundamental data with price data.
            4. Calculates key ratios (PE, PB, Dividend Yield, and Debt-to-Equity) using primary data
               from the financial statements with fallbacks to company info where necessary.
            5. Computes vectorized scores for each metric and a composite value score.
            6. Filters by market cap and generates a final signal.
        
        Args:
            tickers: Single ticker (str) or list of tickers.
            start_date: Not used (for ABC compatibility).
            end_date: Not used (for ABC compatibility).
            initial_position: Not used in this strategy.
            latest_only: If True, returns only the most recent signal per ticker.
            
        Returns:
            pd.DataFrame: DataFrame indexed by ticker and date with the following columns:
                market_cap, price, pe_ratio, pb_ratio, dividend_yield, debt_to_equity,
                value_score, and signal.
        """
        ticker_list = [tickers] if isinstance(tickers, str) else tickers
        try:
            # Step 1: Retrieve and merge financial statement data across the three statements.
            combined_fin = self._get_all_financials_with_dates(ticker_list)
            if combined_fin.empty:
                return pd.DataFrame()

            # Step 2: Retrieve historical prices for the range of financial report dates.
            report_dates = combined_fin.index.get_level_values('date').unique().tolist()
            prices = self.get_historical_prices(
                tickers=ticker_list,
                from_date=min(report_dates).strftime('%Y-%m-%d'),
                to_date=max(report_dates).strftime('%Y-%m-%d')
            )
            
            # Step 3: Merge financial data with closing prices.
            merged = self._merge_financials_prices(combined_fin, prices)
            if merged.empty:
                return pd.DataFrame()

            # Step 4: Calculate key ratios and score them.
            scored = self._calculate_vectorized_scores(merged)
            
            # Step 5: Format and return signals.
            return self._format_output(scored, latest_only)

        except Exception as e:
            self.logger.error(f"Error in DeepValue strategy: {str(e)}")
            return pd.DataFrame()

    def _get_all_financials_with_dates(self, tickers: List[str]) -> pd.DataFrame:
        """
        Retrieve and merge financial statements (balance sheet, income statement, cash flow)
        for multiple tickers using an outer join on (ticker, date). This provides a union of all
        available financial reporting dates and fundamental metrics.
        
        Filters out rows where fewer than the minimum required fundamental fields (set by 'min_statements') 
        are present.
        
        Args:
            tickers: List of ticker symbols.
        
        Returns:
            pd.DataFrame: Merged DataFrame with a MultiIndex of (ticker, date) containing fundamental fields.
        """
        dfs = []
        for stmt_type in ['balance_sheet', 'income_statement', 'cash_flow']:
            try:
                df_stmt = self.get_financials(tickers, stmt_type)
                if not df_stmt.empty:
                    dfs.append(df_stmt.reset_index())
            except DataRetrievalError as dr_err:
                self.logger.warning(f"Could not retrieve {stmt_type} for tickers {tickers}: {dr_err}")
                continue

        if not dfs:
            return pd.DataFrame()

        # Merge the financial statements on ticker and date using an outer join.
        # First merge balance_sheet and income_statement.
        merged = pd.merge(dfs[0], dfs[1], on=['ticker', 'date'], how='outer', suffixes=('_bs', '_is'))
        # Merge with cash_flow. (Suffixes adjust automatically for any overlapping names.)
        merged = pd.merge(merged, dfs[2], on=['ticker', 'date'], how='outer')
        # Create a count of key fundamental fields available:
        merged['fin_count'] = merged[['tangible_book_value', 'diluted_eps', 'cash_dividends_paid', 'total_debt', 'stockholders_equity']].notnull().sum(axis=1)
        merged = merged[merged['fin_count'] >= self.params['min_statements']]
        merged = merged.drop(columns=['fin_count'])
        merged['date'] = pd.to_datetime(merged['date'])
        merged = merged.set_index(['ticker', 'date']).sort_index()
        return merged

    def _merge_financials_prices(self, financials: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Merge the fundamental data with historical price data.
        
        The method ensures the price data uses a MultiIndex of (ticker, date) and uses forward/backward fill 
        where necessary. It then merges closing prices on the nearest available date from the fundamental data.
        It also computes the market capitalization using the number of shares outstanding. If the intrinsic 
        'ordinary_shares_number' is not present in the fundamental data, a fallback is provided using 
        company_info.
        
        Args:
            financials (pd.DataFrame): Merged financial statement data with MultiIndex (ticker, date).
            prices (pd.DataFrame): Historical price data (daily_prices) with at least 'close' column.
        
        Returns:
            pd.DataFrame: Combined DataFrame containing all fundamental data plus 'price' and 'market_cap'.
        """
        # Ensure prices have a MultiIndex.
        if not isinstance(prices.index, pd.MultiIndex):
            prices = prices.reset_index().set_index(['ticker', 'date'])

        # Unstack prices so that each ticker is a column.
        closes = prices['close'].unstack('ticker').ffill().bfill()

        # Reindex the price data to the unique financial statement dates using method='nearest'.
        unique_dates = financials.index.get_level_values('date').unique()
        aligned_prices = closes.reindex(index=unique_dates, method='nearest').stack()

        # Merge the financials with closing prices.
        merged = financials.join(aligned_prices.rename('price'), how='left')
        merged = merged[merged['price'].notna() & (merged['price'] > 0)]

        # Determine shares outstanding.
        if 'ordinary_shares_number' in merged.columns:
            merged['shares_outstanding'] = merged['ordinary_shares_number']
        else:
            merged['shares_outstanding'] = np.nan

        # If shares outstanding is still mostly missing, fill from company_info.
        if merged['shares_outstanding'].isnull().all():
            comp_info = self.get_company_info(merged.index.get_level_values('ticker').unique().tolist())
            if not comp_info.empty:
                if isinstance(comp_info, pd.Series):
                    comp_info = comp_info.to_frame().reset_index()
                else:
                    comp_info = comp_info.reset_index()
                # Use the company_info column 'sharesoutstanding' (as per provided table columns).
                if 'sharesoutstanding' in comp_info.columns:
                    comp_info = comp_info[['ticker', 'sharesoutstanding']]
                    comp_info = comp_info.set_index('ticker')
                    merged['shares_outstanding'] = merged.index.get_level_values('ticker').map(comp_info['sharesoutstanding'])
        
        # Calculate market capitalization.
        merged['market_cap'] = merged['price'] * merged['shares_outstanding']
        merged = merged.dropna(subset=['market_cap'])
        return merged

    def _calculate_vectorized_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate key financial ratios and assign a composite value score using vectorized operations.
        
        For each ticker-date row, the following calculations are performed using available financial data;
        if any value is missing in the financial statements, the method falls back to company_info:
            
            - EPS: Uses 'diluted_eps' from income_statement; falls back to company_info 'trailingeps'.
            - Book Value Per Share: Computed as tangible_book_value / ordinary_shares_number; falls back to 
              company_info 'bookvalue' / 'sharesoutstanding'.
            - Dividend Per Share: Computed as cash_dividends_paid / ordinary_shares_number; falls back to 
              company_info 'dividendrate'.
            - Debt-to-Equity: Computed as total_debt / stockholders_equity; falls back to company_info 'debttoequity'.
        
        The derived ratios are:
            pe_ratio = price / eps
            pb_ratio = price / book_per_share
            dividend_yield = dividend_per_share / price
            debt_to_equity = direct computed ratio
        
        Each metric is scored based on preset thresholds and weighted into a composite score.
        A final binary signal is generated based on whether the composite score exceeds 0.6.
        
        Args:
            data (pd.DataFrame): Combined fundamental and price data.
        
        Returns:
            pd.DataFrame: DataFrame with new columns for calculated ratios, individual scores, the composite
                          'value_score', and the generated 'signal'.
        """
        # Retrieve company info for fallback values.
        ticker_list = data.reset_index()['ticker'].unique().tolist()
        comp_info = self.get_company_info(ticker_list)
        if isinstance(comp_info, pd.Series):
            comp_info = comp_info.to_frame().reset_index()
        else:
            comp_info = comp_info.reset_index()
        comp_info = comp_info[['ticker', 'trailingeps', 'bookvalue', 'sharesoutstanding', 'dividendrate', 'debttoequity']]
        
        # Merge company info with financial data on ticker.
        data = data.reset_index().merge(comp_info, on='ticker', how='left').set_index(['ticker', 'date'])
        
        # Compute EPS: use financial diluted_eps if available, else fallback to trailingeps.
        data['eps'] = data['diluted_eps'].fillna(data['trailingeps'])
        
        # Compute Book Value per Share: primary source from balance sheet; fallback if missing.
        data['book_per_share'] = (data['tangible_book_value'] / data['ordinary_shares_number']).fillna(
            data['bookvalue'] / data['sharesoutstanding']
        )
        
        # Compute Dividend Per Share: primary from cash_flow; fallback to dividendrate.
        data['dividend_per_share'] = (data['cash_dividends_paid'] / data['ordinary_shares_number']).fillna(
            data['dividendrate'].fillna(0)
        )
        
        # Compute Debt-to-Equity: primary from balance sheet; fallback to company_info.
        data['debt_equity'] = (data['total_debt'] / data['stockholders_equity']).fillna(
            data['debttoequity']
        )
        
        # Calculate fundamental ratios.
        data['pe_ratio'] = data['price'] / data['eps']
        data['pb_ratio'] = data['price'] / data['book_per_share']
        data['dividend_yield'] = data['dividend_per_share'] / data['price']
        data['debt_to_equity'] = data['debt_equity']

        # Score each metric based on thresholds.
        metrics = ['pe_ratio', 'pb_ratio', 'dividend_yield', 'debt_to_equity']
        thresholds = [
            self.params['pe_thresholds'],
            self.params['pb_thresholds'],
            self.params['div_thresholds'],
            self.params['de_thresholds']
        ]
        # For PE, PB, and DE lower is better; for Dividend Yield, higher is better.
        directions = [False, False, True, False]
        for metric, thresh, higher_is_better in zip(metrics, thresholds, directions):
            data[f'{metric}_score'] = self._vectorized_score(
                data[metric], 
                thresholds=thresh,
                higher_is_better=higher_is_better
            )
        
        # Compute composite weighted value score.
        weights = np.array(self.params['score_weights'])
        score_cols = [f'{m}_score' for m in metrics]
        data['value_score'] = data[score_cols].dot(weights) / (3 * weights.sum())
        
        # Apply market cap filter.
        data = data[data['market_cap'] >= self.params['min_market_cap']]
        data['signal'] = (data['value_score'] >= 0.6).astype(int)
        return data

    def _vectorized_score(self, series: pd.Series, thresholds: List[float], higher_is_better: bool) -> pd.Series:
        """
        Compute scores for a given metric in a vectorized way.
        
        For each metric, three conditions are evaluated against provided thresholds.
        When 'higher_is_better' is False, lower values get higher scores.
        When True, higher values are considered superior.
        
        Args:
            series: A pandas Series of metric values.
            thresholds: A list of three threshold values.
            higher_is_better: Boolean indicating the direction of desirability.
            
        Returns:
            pd.Series: Score values (3, 2, 1, or a default) for each element in the input series.
        """
        conditions = [
            series < thresholds[0] if not higher_is_better else series > thresholds[0],
            series < thresholds[1] if not higher_is_better else series > thresholds[1],
            series < thresholds[2] if not higher_is_better else series > thresholds[2]
        ]
        scores = np.select(
            condlist=conditions,
            choicelist=[3, 2, 1],
            default=0 if not higher_is_better else 3
        )
        return pd.Series(scores, index=series.index)

    def _format_output(self, data: pd.DataFrame, latest_only: bool) -> pd.DataFrame:
        """
        Format the final output DataFrame containing key metrics and signals.
        
        The columns include market_cap, price, all ratio metrics, the composite value_score,
        and the signal. Optionally, only the latest (most recent) row for each ticker is returned.
        
        Args:
            data (pd.DataFrame): DataFrame with computed ratios and scores.
            latest_only (bool): If True, only return the most recent date per ticker.
            
        Returns:
            pd.DataFrame: Formatted DataFrame ready for further ranking/use.
        """
        cols = [
            'market_cap', 'price', 'pe_ratio', 'pb_ratio',
            'dividend_yield', 'debt_to_equity', 'value_score', 'signal'
        ]
        result = data.reset_index()[['ticker', 'date'] + cols]
        result['date'] = pd.to_datetime(result['date'])
        if latest_only:
            result = result.sort_values('date').groupby('ticker').last().reset_index()
        return result

    def __del__(self):
        """
        Ensure proper cleanup by calling the base destructor.
        """
        super().__del__()