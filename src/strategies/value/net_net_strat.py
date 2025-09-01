# trading_strategy/src/strategies/value/net_net_start.py

# TODO: Verify

"""
Net-Net Strategy (Benjamin Graham)

This module implements Benjamin Graham's Net-Net strategy as a subclass of BaseStrategy.
The strategy identifies stocks trading below their Net Current Asset Value (NCAV).
NCAV is computed as:
    
    NCAV = current_assets - total_liabilities

A stock is considered deeply undervalued if its closing price is less than a given threshold (default 66%)
of NCAV (i.e., price < 0.66 * NCAV).

The script retrieves balance sheet data (from which it uses the columns 'current_assets' and 
'total_liabilities_net_minority_interest'), then obtains historical closing prices corresponding 
to the financial statement dates (using merge_asof), computes the net‑net ratio (price / NCAV) and 
finally generates a binary signal along with a final score (net_net_ratio) that can be used for ranking.

Default parameters are tuned for the Indian market (NSE and BSE).

The returned DataFrame contains the following columns:
    ['ticker', 'date', 'current_assets', 'total_liabilities', 'NCAV', 'price', 'net_net_ratio', 'signal']
    
 where:
   • NCAV = current_assets - total_liabilities,
   • net_net_ratio = price / NCAV, and 
   • signal = 1 if price < (0.66 * NCAV), else 0.
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.strategies.base_strat import BaseStrategy, DataRetrievalError


class NetNetStrategy(BaseStrategy):
    """
    Implementation of Benjamin Graham's Net‑Net Strategy.
    
    This strategy identifies stocks trading at a discount to their Net Current Asset Value (NCAV).
    NCAV is calculated as:
    
        NCAV = current_assets - total_liabilities
        
    A stock is considered deeply undervalued if its closing price is less than the threshold percentage
    (default 66%) of NCAV. The computed net‑net ratio (price/NCAV) can be used for ranking tickers.
    
    Attributes:
        params (dict): Strategy parameters, including:
            - 'netnet_threshold': The fraction (default 0.66) of NCAV that the stock price must be below
                                  to generate a buy signal.
    """

    def __init__(self, db_config, params: Optional[Dict] = None):
        """
        Initialize the Net‑Net Strategy.
        
        Args:
            db_config: Database configuration settings.
            params (dict, optional): Dictionary to override default parameters.
                                     Default: {'netnet_threshold': 0.66}
        """
        # Initialize the BaseStrategy which sets up logging, caching, and db connection.
        super().__init__(db_config, params)
        self.params = params or {
            'netnet_threshold': 0.66  # For example: price must be less than 66% of NCAV.
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
        Generate trading signals based on Benjamin Graham's Net‑Net Strategy.
        
        This function performs the following steps:
        
          1. Retrieves balance sheet data for the provided ticker(s) using the base class’s get_financials method.
             It expects to find at least the 'current_assets' and
             'total_liabilities_net_minority_interest' columns.
             
          2. Determines the date range from the balance sheet (i.e., the financial statement dates).
          
          3. Retrieves historical price data over that date range using get_historical_prices.
          
          4. Uses a merge_asof to align each financial statement date
             with the most recent available closing price (backward fill).
          
          5. Computes NCAV as:
          
                 NCAV = current_assets - total_liabilities_net_minority_interest
          
          6. Calculates the net‑net ratio as:
          
                 net_net_ratio = price / NCAV
                 
          7. Generates a binary signal:
             A signal of 1 is produced if price < (netnet_threshold * NCAV) (i.e. if the stock is trading
             below 66% of NCAV by default), otherwise 0.
         
          8. Optionally applies date filtering and selects only the most recent signal per ticker.
          
        Args:
            tickers (Union[str, List[str]]): Single ticker or list of tickers to analyze.
            start_date (str, optional): Start date (YYYY-MM-DD) to filter the results.
            end_date (str, optional): End date (YYYY-MM-DD) to filter the results.
            initial_position (int): (Not used in this strategy.)
            latest_only (bool): If True, returns only the most recent signal per ticker.
        
        Returns:
            pd.DataFrame: DataFrame containing the following columns:
              ['ticker', 'date', 'current_assets', 'total_liabilities', 'NCAV', 'price', 'net_net_ratio', 'signal']
              where 'net_net_ratio' is computed as price / NCAV.
        """
        try:
            # Step 1: Retrieve balance sheet data
            bs_df = self.get_financials(tickers, 'balance_sheet', lookback=None)
            if bs_df.empty:
                self.logger.warning("Balance sheet data is empty for tickers: %s", tickers)
                return pd.DataFrame()

            # Reset the index if the DataFrame has a MultiIndex (e.g., from ticker/date)
            if isinstance(bs_df.index, pd.MultiIndex):
                bs_df = bs_df.reset_index()
            else:
                # Ensure there is a 'ticker' column for consistency.
                if 'ticker' not in bs_df.columns:
                    bs_df['ticker'] = tickers if isinstance(tickers, str) else np.nan

            # Ensure the 'date' column is in datetime format.
            bs_df['date'] = pd.to_datetime(bs_df['date'])

            # Check that the required balance sheet columns are present.
            required_balance_cols = ['current_assets', 'total_liabilities_net_minority_interest']
            for col in required_balance_cols:
                if col not in bs_df.columns:
                    self.logger.error("Missing required balance sheet column: %s", col)
                    return pd.DataFrame()

            # Step 2: Determine date range from financial statements.
            min_date = bs_df['date'].min().strftime('%Y-%m-%d')
            max_date = bs_df['date'].max().strftime('%Y-%m-%d')

            # Step 3: Retrieve historical price data over that date range.
            price_df = self.get_historical_prices(tickers, from_date=min_date, to_date=max_date)
            if price_df.empty:
                self.logger.warning("Historical price data is empty for tickers: %s", tickers)
                return pd.DataFrame()

            # Reset the index for price data if necessary.
            if isinstance(price_df.index, pd.MultiIndex):
                price_df = price_df.reset_index()
            else:
                if 'ticker' not in price_df.columns:
                    price_df['ticker'] = tickers if isinstance(tickers, str) else tickers[0]

            # Ensure price 'date' column is in datetime format.
            price_df['date'] = pd.to_datetime(price_df['date'])

            # Step 4: Align financial statement dates with closing prices.
            bs_df.sort_values(by=['ticker', 'date'], inplace=True)
            price_df.sort_values(by=['ticker', 'date'], inplace=True)

            aligned_df = pd.merge_asof(
                bs_df,
                price_df[['ticker', 'date', 'close']],
                on='date',
                by='ticker',
                direction='backward'
            )

            # Step 5: Calculate NCAV.
            aligned_df['NCAV'] = aligned_df['current_assets'] - aligned_df['total_liabilities_net_minority_interest']

            # Remove any records where NCAV is not positive (to avoid division by zero or negative valuations).
            aligned_df = aligned_df[aligned_df['NCAV'] > 0].copy()

            # Step 6: Compute the net‑net ratio.
            # net_net_ratio = price / NCAV   where 'close' is the retrieved price.
            aligned_df['net_net_ratio'] = aligned_df['close'] / aligned_df['NCAV']

            # Step 7: Generate the trading signal.

            # Signal is 1 if price < (netnet_threshold * NCAV), else 0.
            threshold = self.params.get('netnet_threshold', 0.66)
            aligned_df['signal'] = (aligned_df['close'] < (threshold * aligned_df['NCAV'])).astype(int)

            # Optional: Date filtering.
            if start_date:
                aligned_df = aligned_df[aligned_df['date'] >= pd.to_datetime(start_date)]
            if end_date:
                aligned_df = aligned_df[aligned_df['date'] <= pd.to_datetime(end_date)]

            # Optional: Keep only the latest record per ticker.
            if latest_only:
                aligned_df = aligned_df.sort_values('date').groupby('ticker', group_keys=False).tail(1)

            # Step 8: Prepare and return the final result.
            result_df = aligned_df[
                ['ticker', 'date', 'current_assets', 'total_liabilities_net_minority_interest',
                 'NCAV', 'close', 'net_net_ratio', 'signal']
            ].copy()
            result_df.rename(columns={
                'close': 'price',
                'total_liabilities_net_minority_interest': 'total_liabilities'
            }, inplace=True)

            return result_df.reset_index(drop=True)

        except Exception as e:
            self.logger.error("Error generating signals for tickers %s: %s", tickers, str(e), exc_info=True)
            return pd.DataFrame()