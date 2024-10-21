import pandas as pd
import logging
from .utils import validate_columns, process_trades_dataframe, merge_trade_conditions, merge_exchange_codes
from .config import ENABLE_CONCURRENCY
from .concurrency import concurrent_apply

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OHLCVCBuilder:
    """
    OHLCVC Builder with accurate condition handling for trades.
    This class takes raw trade data and builds OHLCVC (Open, High, Low, Close, Volume, and Count) bars.
    """

    def __init__(self, trades, format_columns, trade_conditions_df, exchange_codes_df=None):
        """
        Initialize the OHLCVCBuilder with raw trade data, format columns, and condition dataframes.

        :param trades: List of lists, where each list contains trade information
        :param format_columns: List of column names for the trade data
        :param trade_conditions_df: DataFrame containing trade conditions
        :param exchange_codes_df: DataFrame containing exchange codes (optional)
        """
        self.trades = trades
        self.format_columns = format_columns
        self.trade_conditions_df = trade_conditions_df
        self.exchange_codes_df = exchange_codes_df

        # Determine eligible conditions and volume_true_conditions
        self.eligible_conditions = self._determine_conditions('Cancel', False)
        self.volume_true_conditions = self._determine_conditions('Volume', True)

        # Create DataFrame from trades
        self.df_trades = self._create_dataframe()

    def _determine_conditions(self, column_name, value):
        """
        Determines condition codes based on a specified boolean column in trade_conditions_df.

        :param column_name: The column name to filter on
        :param value: The boolean value to filter for
        :return: Set of condition codes matching the criteria
        """
        condition_df = self.trade_conditions_df[self.trade_conditions_df[column_name] == value]
        condition_codes = set(condition_df['Code'].astype(int).unique())
        logging.info(f"Determined {len(condition_codes)} condition codes where {column_name} == {value}.")
        return condition_codes

    def _create_dataframe(self):
        """
        Converts the raw trade data into a pandas DataFrame with appropriate column names and timestamps.

        :return: Pandas DataFrame with trade data
        """
        df_trades = pd.DataFrame(self.trades, columns=self.format_columns)
        validate_columns(df_trades)
        df_trades = process_trades_dataframe(df_trades)
        df_trades = merge_trade_conditions(df_trades, self.trade_conditions_df)
        if self.exchange_codes_df is not None and 'exchange_code' in df_trades.columns:
            df_trades = merge_exchange_codes(df_trades, self.exchange_codes_df)
        return df_trades

    def apply_conditions(self):
        """
        Apply conditions to set flags for inclusion in open, high, low, close, volume calculations.
        """
        if ENABLE_CONCURRENCY:
            self.apply_conditions_concurrently()
        else:
            self.apply_conditions_sequentially()

    def apply_conditions_sequentially(self):
        """
        Sequentially apply conditions to set flags for inclusion in calculations.
        """
        self._apply_general_conditions()
        self._apply_close_conditions()
        self._apply_volume_conditions()

    def apply_conditions_concurrently(self):
        """
        Apply conditions concurrently to set flags for inclusion in calculations.
        """
        functions = [self._apply_general_conditions, self._apply_close_conditions, self._apply_volume_conditions]
        concurrent_apply(functions)

    def _apply_general_conditions(self):
        """
        Apply general conditions to set 'include_in_open', 'include_in_high', 'include_in_low' flags.
        """
        rth_start_time = '09:30:00'
        rth_end_time = '16:00:00'
        self.df_trades['RTH'] = self.df_trades['timestamp'].dt.time.between(
            pd.to_datetime(rth_start_time).time(),
            pd.to_datetime(rth_end_time).time(),
            inclusive='left'
        )
        self.df_trades['include_in_open'] = self.df_trades['condition'].isin(self.eligible_conditions)

        # High and Low inclusion conditions
        rth_mask = self.df_trades['RTH']
        eligible_conditions_mask = self.df_trades['condition'].isin(self.eligible_conditions)
        self.df_trades['include_in_high'] = False
        self.df_trades['include_in_low'] = False

        # For RTH trades
        self.df_trades.loc[rth_mask, 'include_in_high'] = eligible_conditions_mask & self.df_trades['High']
        self.df_trades.loc[rth_mask, 'include_in_low'] = eligible_conditions_mask & self.df_trades['Low']

        # For non-RTH trades
        self.df_trades.loc[~rth_mask, 'include_in_high'] = eligible_conditions_mask & ~rth_mask
        self.df_trades.loc[~rth_mask, 'include_in_low'] = eligible_conditions_mask & ~rth_mask

    def _apply_volume_conditions(self):
        """
        Apply conditions to set 'include_in_volume' flag.
        """
        self.df_trades['include_in_volume'] = self.df_trades['condition'].isin(self.volume_true_conditions)

    def _apply_close_conditions(self):
        """
        Applies conditional updates to 'include_in_close' flags based on trade conditions and their positions within intervals.
        """
        self.df_trades['include_in_close'] = False
        self.df_trades.loc[self.df_trades['Last'] == True, 'include_in_close'] = True

        # Define condition codes that require conditional handling
        conditional_condition_codes = {2, 5, 8, 13, 15, 96, 98}

        # Group trades by interval
        grouped = self.df_trades.groupby(pd.Grouper(key='timestamp', freq='1min'))
        for interval, group in grouped:
            if group.empty:
                continue

            # Identify trades with condition codes in conditional_condition_codes
            conditional_trades = group[group['condition'].isin(conditional_condition_codes)]
            if conditional_trades.empty:
                continue

            for idx, trade in conditional_trades.iterrows():
                code = trade['condition']

                if code in {2, 5, 8}:
                    # Update 'include_in_close' if it's the only relevant trade in the interval
                    relevant_trades_count = group[group['condition'].isin(conditional_condition_codes)].shape[0]
                    if relevant_trades_count == 1:
                        self.df_trades.at[idx, 'include_in_close'] = True

                elif code == 13:
                    # Update 'include_in_close' if no other qualifying 'include_in_close' exists
                    if not group[group['include_in_close'] == True].empty:
                        continue
                    else:
                        self.df_trades.at[idx, 'include_in_close'] = True

                elif code == 15:
                    # Update 'include_in_close' if it's the first trade in the interval
                    if idx == group.index[0]:
                        self.df_trades.at[idx, 'include_in_close'] = True

                elif code in {96, 98}:
                    # Update 'include_in_close' if it's the last trade in the interval
                    if idx == group.index[-1]:
                        self.df_trades.at[idx, 'include_in_close'] = True

    def calculate_ohlcvc(self, interval='1min'):
        """
        Calculate OHLCVC data based on the filtered trades and a specified interval.

        :param interval: Resampling interval for OHLCVC calculation
        :return: DataFrame containing the OHLCVC data
        """
        df = self.df_trades.set_index('timestamp')
        if not isinstance(df.index, pd.DatetimeIndex):
            logging.error("'timestamp' must be a DatetimeIndex for resampling.")
            raise TypeError("'timestamp' must be a DatetimeIndex for resampling.")

        open_ = df[df['include_in_open']]['price'].resample(interval).first()
        high = df[df['include_in_high']]['price'].resample(interval).max()
        low = df[df['include_in_low']]['price'].resample(interval).min()

        def get_close(g):
            close_trades = g[g['include_in_close']]
            if not close_trades.empty:
                return close_trades['price'].iloc[-1]
            elif not g['price'].empty:
                return g['price'].iloc[-1]
            else:
                return float('nan')

        close = df.groupby(pd.Grouper(freq=interval)).apply(get_close)
        ohlc = pd.DataFrame({'open': open_, 'high': high, 'low': low, 'close': close})

        volume = df[df['include_in_volume']]['size'].resample(interval).sum()
        ohlc['volume'] = volume.fillna(0).astype(int)
        count = df['price'].resample(interval).count()
        ohlc['count'] = count.fillna(0).astype(int)
        ohlc.fillna(method='ffill', inplace=True)
        logging.info(f"Calculated OHLCVC with interval '{interval}'. Total intervals: {len(ohlc)}")
        return ohlc

    def get_ohlcvc(self, interval='1min'):
        """
        Main function to build OHLCVC from raw trades and return the calculated values.

        :param interval: Resampling interval for OHLCVC calculation
        :return: DataFrame containing OHLCVC data
        """
        self.apply_conditions()
        ohlcvc = self.calculate_ohlcvc(interval=interval)
        return ohlcvc
