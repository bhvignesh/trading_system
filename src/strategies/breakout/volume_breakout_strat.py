# trading_system/src/strategies/breakout/volume_breakout_strat.py

import numpy as np
import pandas as pd
from typing import Dict, Optional, Union, List

from src.strategies.base_strat import BaseStrategy
from src.database.config import DatabaseConfig
from src.strategies.risk_management import RiskManager


class VolumeBreakoutStrategy(BaseStrategy):
    """
    Volume Breakout Strategy with Integrated Risk Management.

    (Docstring truncated for brevity—same as before.)
    """

    def __init__(self, db_config: DatabaseConfig, params: Optional[Dict] = None):
        default_params = {
            'lookback_period': 20,
            'volume_threshold': 1.5,
            'price_threshold': 0.02,
            'volume_avg_period': 20,
            'consecutive_bars': 1,
            'use_atr_filter': False,
            'atr_period': 14,
            'atr_threshold': 1.0,
            'stop_loss_pct': 0.05,
            'take_profit_pct': 0.10,
            'trailing_stop_pct': 0.0,
            'slippage_pct': 0.001,
            'transaction_cost_pct': 0.001,
            'long_only': True,
        }
        current_params = default_params.copy()
        if params:
            current_params.update(params)

        super().__init__(db_config, current_params)

    def generate_signals(
        self,
        ticker: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_position: int = 0,
        latest_only: bool = False,
    ) -> pd.DataFrame:
        params = self.params
        lookback = int(params['lookback_period'])
        vol_avg = int(params['volume_avg_period'])
        atr_period = int(params['atr_period']) if params['use_atr_filter'] else 0

        extra_buffer = 50
        lookback_length = max(lookback, vol_avg, atr_period) + extra_buffer

        price_data = self.get_historical_prices(
            ticker, lookback=lookback_length, from_date=start_date, to_date=end_date
        )
        if price_data.empty:
            self.logger.warning(
                f"Insufficient data for ticker(s) {ticker} to generate Volume Breakout signals."
            )
            return pd.DataFrame()

        price_data = self._ensure_multiindex(price_data, ticker)
        indicators = self._compute_indicators(price_data, lookback, vol_avg, atr_period)

        result = price_data.join(indicators, how="left")

        result['price_change'] = result.groupby(level=0)['close'].transform(
            lambda s: s.pct_change(fill_method=None)
        )
        result['price_change'].fillna(0.0, inplace=True)

        resistance_shifted = result.groupby(level=0)['resistance'].shift(1)
        support_shifted = result.groupby(level=0)['support'].shift(1)

        above_mask = result['close'] > resistance_shifted
        below_mask = result['close'] < support_shifted

        result['above_resistance_count'] = self._consecutive_true_counts(above_mask)
        result['below_support_count'] = self._consecutive_true_counts(below_mask)

        if params['use_atr_filter']:
            result['price_volatility'] = result['atr'].div(result['close']).fillna(0.0)
        else:
            result['price_volatility'] = 0.0

        result['signal'] = 0
        consecutive_bars = int(params['consecutive_bars'])

        buy_conditions = (
            (result['above_resistance_count'] >= consecutive_bars)
            & (result['volume'] > result['avg_volume'] * params['volume_threshold'])
            & (result['price_change'].abs() > params['price_threshold'])
        )
        sell_conditions = (
            (result['below_support_count'] >= consecutive_bars)
            & (result['volume'] > result['avg_volume'] * params['volume_threshold'])
            & (result['price_change'].abs() > params['price_threshold'])
        )

        if params['use_atr_filter']:
            threshold = result['price_volatility'] * params['atr_threshold']
            buy_conditions &= result['price_change'].abs() > threshold
            sell_conditions &= result['price_change'].abs() > threshold

        result.loc[buy_conditions, 'signal'] = 1
        if not params['long_only']:
            result.loc[sell_conditions, 'signal'] = -1

        if params['use_atr_filter']:
            result['signal_strength'] = result['price_change'].abs().div(
                result['price_volatility'] + 1e-9
            )
        else:
            result['signal_strength'] = result['price_change'].abs()
        result['signal_strength'].replace([np.inf, -np.inf], 0.0, inplace=True)
        result['signal_strength'].fillna(0.0, inplace=True)

        essential_cols = ['resistance', 'support', 'avg_volume', 'price_change', 'close']
        if params['use_atr_filter']:
            essential_cols.extend(['atr', 'price_volatility'])
        result.dropna(subset=essential_cols, inplace=True)

        if result.empty:
            self.logger.warning(
                f"DataFrame became empty after dropping NaNs for ticker(s) {ticker}. No signals to process."
            )
            return pd.DataFrame()

        if latest_only:
            result = result.groupby(level=0).tail(1)

        rm_input = self._prepare_risk_manager_input(result)
        risk_manager = RiskManager(
            stop_loss_pct=params['stop_loss_pct'],
            take_profit_pct=params['take_profit_pct'],
            trailing_stop_pct=params['trailing_stop_pct'],
            slippage_pct=params['slippage_pct'],
            transaction_cost_pct=params['transaction_cost_pct'],
        )
        processed = risk_manager.apply(rm_input, initial_position=initial_position)
        processed = processed.rename(
            columns={
                'return': 'rm_strategy_return',
                'cumulative_return': 'rm_cumulative_return',
                'exit_type': 'rm_action',
            }
        )

        processed_multi = self._merge_risk_manager_output(processed, result.index)

        final_df = result.copy()
        cols_from_rm = ['position', 'rm_strategy_return', 'rm_cumulative_return', 'rm_action']
        for col in cols_from_rm:
            if col in processed_multi.columns:
                final_df[col] = processed_multi[col]
            else:
                final_df[col] = np.nan
                self.logger.warning(f"Column {col} missing from RiskManager output.")

        return final_df.sort_index()

    def _compute_indicators(
        self,
        price_data: pd.DataFrame,
        lookback: int,
        volume_avg_period: int,
        atr_period: int,
    ) -> pd.DataFrame:
        grouped = price_data.groupby(level=0, group_keys=False)

        indicators = pd.DataFrame(index=price_data.index)
        indicators['resistance'] = grouped['high'].transform(
            lambda s: s.rolling(window=lookback, min_periods=lookback).max()
        )
        indicators['support'] = grouped['low'].transform(
            lambda s: s.rolling(window=lookback, min_periods=lookback).min()
        )
        indicators['avg_volume'] = grouped['volume'].transform(
            lambda s: s.rolling(window=volume_avg_period, min_periods=volume_avg_period).mean()
        )

        if atr_period > 0:
            atr_values = []
            for ticker, group in price_data.groupby(level=0):
                group_df = group.droplevel(0)
                atr_series = self._calculate_atr(group_df, period=atr_period)
                atr_series.index = group.index
                atr_values.append(atr_series)
            indicators['atr'] = pd.concat(atr_values).sort_index()
        else:
            indicators['atr'] = 0.0

        return indicators

    def _prepare_risk_manager_input(self, df: pd.DataFrame) -> pd.DataFrame:
        rm_cols = ['open', 'high', 'low', 'close', 'signal', 'signal_strength', 'atr']
        available_cols = [col for col in rm_cols if col in df.columns]

        rm_df = df[available_cols].copy()

        rm_df = rm_df.reset_index(level=0).rename(columns={'level_0': 'ticker'})
        rm_df['ticker'] = rm_df['ticker'].astype(str)

        return rm_df

    def _merge_risk_manager_output(
        self, processed: pd.DataFrame, original_index: pd.MultiIndex
    ) -> pd.DataFrame:
        if 'ticker' not in processed.columns:
            self.logger.warning(
                "RiskManager output missing 'ticker' column; unable to map results back to MultiIndex."
            )
            processed['ticker'] = ''
        processed_multi = processed.set_index('ticker', append=True)
        processed_multi.index = processed_multi.index.swaplevel(0, 1)
        processed_multi = processed_multi.reindex(original_index)
        return processed_multi

    @staticmethod
    def _consecutive_true_counts(mask: pd.Series) -> pd.Series:
        if mask.empty:
            return pd.Series(index=mask.index, dtype='int64')

        mask_int = mask.astype(np.int64)

        if isinstance(mask.index, pd.MultiIndex):
            cumsum = mask_int.groupby(level=0).cumsum()
            reset = cumsum.where(~mask, np.nan).groupby(level=0).ffill().fillna(0)
        else:
            cumsum = mask_int.cumsum()
            reset = cumsum.where(~mask, np.nan).ffill().fillna(0)

        counts = (cumsum - reset).where(mask, 0)
        return counts.astype(np.int64)

    @staticmethod
    def _ensure_multiindex(price_data: pd.DataFrame, ticker: Union[str, List[str]]) -> pd.DataFrame:
        price_data = price_data.sort_index()
        if isinstance(price_data.index, pd.MultiIndex):
            return price_data

        if isinstance(ticker, list):
            if len(ticker) != 1:
                raise ValueError(
                    "Expected single-ticker data but received multiple tickers without MultiIndex."
                )
            ticker_value = ticker[0]
        else:
            ticker_value = ticker

        mi = pd.MultiIndex.from_product(
            [[str(ticker_value)], price_data.index],
            names=['ticker', price_data.index.name or 'date'],
        )
        price_data = price_data.copy()
        price_data.index = mi
        return price_data

    def _calculate_atr(self, price_data: pd.DataFrame, period: int = 14) -> pd.Series:
        if price_data.empty or len(price_data) < period:
            return pd.Series(np.nan, index=price_data.index)

        high = price_data['high']
        low = price_data['low']
        prev_close = price_data['close'].shift(1).reindex_like(high)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = true_range.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

        return atr