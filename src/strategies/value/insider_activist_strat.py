# trading_system/src/strategies/value/insider_activist_strat.py

# TODO: Verify

"""
This module implements the Insider Buying & Activist Investor Screener strategy.
The strategy identifies companies where insiders (e.g., CEOs, CFOs) appear to be 
buying shares and where known activist investors (e.g., Carl Icahn, Bill Ackman) 
have taken a stake. It does so by merging historical financial statement dates from 
the balance sheet with company info (fallback) data, aligning these dates with 
historical closing prices, and then scoring each observation.

The final score is the sum of a weighted insider buying signal and an activist investor
signal. This score can be used to rank and compare tickers for special situation investing,
and default parameters are optimized for the Indian NSE/BSE markets.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class InsiderActivistInvestorStrategy(BaseStrategy):
    """
    Insider Buying & Activist Investor Screener strategy.

    This strategy scans for companies where insiders (CEOs, CFOs) are buying shares
    and where activist investors (e.g., Carl Icahn, Bill Ackman) have taken a stake.
    It retrieves historical financial statement dates from the balance sheet and merges these
    dates with company fundamental data (fallback to company_info) and historical closing prices.
    A final score is computed as a weighted sum of:
      - Insider metric: Based on the company's insider ownership (heldpercentinsiders)
        if above a chosen threshold.
      - Activist metric: Set to a fixed weight if the company officers mention known activist names.
    This produces a trend of the score over available historical statement dates.

    Attributes:
        params (dict): Strategy parameters including:
            - 'insider_weight': Weight factor for the insider buying metric (default: 1.0).
            - 'activist_weight': Weight factor for the activist investor metric (default: 1.0).
            - 'insider_threshold': Minimum insider ownership (as a fraction) required to contribute to the score
                                   (default: 0.10 for 10%).
            - 'activist_names': List of names to search for in the company officers field
                                 (default: ["Carl Icahn", "Bill Ackman"]).
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the InsiderActivistInvestorStrategy.

        Args:
            db_config: Database configuration settings.
            params (dict, optional): Strategy-specific parameters to override defaults.
                Default parameters:
                    {
                        'insider_weight': 1.0,
                        'activist_weight': 1.0,
                        'insider_threshold': 0.10,  # 10% minimum insider ownership
                        'activist_names': ["Carl Icahn", "Bill Ackman"]
                    }
        """
        super().__init__(db_config, params)
        # Set default parameters optimized for the Indian market (NSE/BSE), which you can override.
        self.params = params or {
            'insider_weight': 1.0,
            'activist_weight': 1.0,
            'insider_threshold': 0.10,  # Insider ownership must be at least 10%
            'activist_names': ["Carl Icahn", "Bill Ackman"]
        }

    def generate_signals(
        self,
        tickers: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False
    ) -> pd.DataFrame:
        """
        Generate historical trading signals based on the Insider Buying & Activist Investor Screener strategy.

        The method performs the following steps:
          1. Retrieve all available balance sheet data (as a proxy for historical financial statement dates)
             for the provided ticker(s).
          2. Retrieve company info data for each ticker to obtain insider ownership and company officer details.
             Note: The balance sheet is the primary source of quarterly dates; if a required value is missing here,
                   the company info data is used as a fallback.
          3. Merge the balance sheet dates with the company info data and then compute:
                - insider_score: If the company's heldpercentinsiders is at or above a given threshold,
                                 the value is weighted by 'insider_weight' (otherwise, it is 0).
                - activist_score: Set to 'activist_weight' if the company_officers field contains any known
                                  activist investor names (case-insensitive); otherwise, 0.
          4. Retrieve historical closing price data for the date range spanning the balance sheet dates.
          5. Align the financial statement dates with closing price data using a merge_asof join.
          6. Optionally apply date filters or return only the most recent record per ticker.
          7. Output a final DataFrame useful for further ranking.

        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date in 'YYYY-MM-DD' format to filter the results.
            end_date (str, optional): End date in 'YYYY-MM-DD' format to filter the results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent available signal per ticker.

        Returns:
            pd.DataFrame: A DataFrame with the columns:
                ['ticker', 'date', 'heldpercentinsiders', 'insider_score', 'activist_flag',
                 'activist_score', 'final_score', 'close']
            where 'final_score' is the sum of insider and activist scores.
        """
        try:
            # Step 1: Retrieve balance sheet financial statement dates.
            bs = self.get_financials(tickers, 'balance_sheet', lookback=None)
            if bs.empty:
                self.logger.warning("No balance sheet data retrieved for tickers: %s", tickers)
                return pd.DataFrame()

            # If the DataFrame is multi-indexed (ticker, date), reset index to have explicit columns.
            if isinstance(bs.index, pd.MultiIndex):
                bs = bs.reset_index()
            # Ensure we have the necessary 'ticker' and 'date' columns.
            if 'ticker' not in bs.columns or 'date' not in bs.columns:
                self.logger.error("Balance sheet data must contain 'ticker' and 'date' columns.")
                return pd.DataFrame()

            bs['date'] = pd.to_datetime(bs['date'])
            # For our purposes, we only need ticker and date, but more columns (if needed) can be included.
            bs = bs[['ticker', 'date']].sort_values(['ticker', 'date'])

            # Step 2: Retrieve company info for ticker(s) to get insider and activist information.
            cinfo = self.get_company_info(tickers)
            # Ensure cinfo is a DataFrame with a 'ticker' column.
            if isinstance(cinfo, pd.Series):
                # Single ticker; convert to DataFrame.
                cinfo = cinfo.to_frame().T
                cinfo['ticker'] = tickers
            else:
                cinfo = cinfo.reset_index()  # 'ticker' becomes a column
            # Ensure necessary columns; if missing, fill with defaults.
            if 'heldpercentinsiders' not in cinfo.columns:
                cinfo['heldpercentinsiders'] = 0.0
            if 'company_officers' not in cinfo.columns:
                cinfo['company_officers'] = ''

            # Step 3: Merge balance sheet dates with company info.
            merged = pd.merge(bs, cinfo[['ticker', 'heldpercentinsiders', 'company_officers']],
                              on='ticker', how='left')

            # Compute insider signal.
            insider_threshold = self.params.get('insider_threshold', 0.10)
            insider_weight = self.params.get('insider_weight', 1.0)
            # Insider score is set only if heldpercentinsiders meets or exceeds the threshold.
            merged['insider_score'] = merged['heldpercentinsiders'].apply(
                lambda x: x * insider_weight if x >= insider_threshold else 0.0
            )

            # Compute activist signal by vectorized pattern matching in company_officers column.
            # If any of the activist names appear, assign a flag value.
            activist_names = self.params.get('activist_names', ["Carl Icahn", "Bill Ackman"])
            pattern = '|'.join(activist_names)
            merged['activist_flag'] = merged['company_officers'].str.contains(pattern, case=False, na=False).astype(int)
            activist_weight = self.params.get('activist_weight', 1.0)
            merged['activist_score'] = merged['activist_flag'] * activist_weight

            # Final score is the sum of the weighted insider and activist scores.
            merged['final_score'] = merged['insider_score'] + merged['activist_score']

            # Step 4: Align with historical closing price data.
            # Determine the date range to retrieve price data.
            min_date = merged['date'].min().strftime('%Y-%m-%d')
            max_date = merged['date'].max().strftime('%Y-%m-%d')
            price_data = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
            if price_data.empty:
                self.logger.warning("No price data retrieved for tickers: %s", tickers)
                merged['close'] = np.nan
            else:
                # Reset index if necessary so that 'ticker' and 'date' become explicit columns.
                if isinstance(price_data.index, pd.MultiIndex):
                    price_data = price_data.reset_index()
                price_data['date'] = pd.to_datetime(price_data['date'])
                price_data = price_data.sort_values(['ticker', 'date'])
                merged = merged.sort_values(['ticker', 'date'])
                # Use merge_asof to align each financial statement date with the most recent available closing price.
                merged = pd.merge_asof(
                    merged,
                    price_data[['ticker', 'date', 'close']],
                    on='date',
                    by='ticker',
                    direction='backward'
                )

            # Step 5: Optional date filtering.
            if start_date:
                merged = merged[merged['date'] >= pd.to_datetime(start_date)]
            if end_date:
                merged = merged[merged['date'] <= pd.to_datetime(end_date)]

            # Step 6: If only the latest signal for each ticker is desired, pick the most recent record.
            if latest_only:
                merged = merged.sort_values('date').groupby('ticker', as_index=False).tail(1)

            # Select and order the final columns.
            final_columns = ['ticker', 'date', 'heldpercentinsiders', 'insider_score',
                             'activist_flag', 'activist_score', 'final_score', 'close']
            return merged[final_columns].reset_index(drop=True)

        except Exception as e:
            self.logger.error(f"Error processing tickers {tickers}: {str(e)}", exc_info=True)
            return pd.DataFrame()