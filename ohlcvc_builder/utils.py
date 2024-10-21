import pandas as pd
import logging

def load_trade_conditions(trade_conditions_path):
    """
    Loads and processes the TradeConditions.csv file.

    :param trade_conditions_path: Path to the TradeConditions.csv file
    :return: DataFrame containing trade conditions
    """
    try:
        # Read the CSV without specifying usecols, then normalize column names
        trade_conditions_df = pd.read_csv(trade_conditions_path)
        # Normalize column names to lowercase
        trade_conditions_df.columns = [col.lower() for col in trade_conditions_df.columns]
        
        # Define the expected columns in lowercase
        expected_columns = ['code', 'name', 'cancel', 'latereport', 'autoexecuted', 
                            'openreport', 'volume', 'high', 'low', 'last']
        
        # Check if all expected columns are present
        missing_columns = set(expected_columns) - set(trade_conditions_df.columns)
        if missing_columns:
            logging.error(f"Missing columns in TradeConditions.csv: {missing_columns}")
            raise ValueError(f"Missing columns in TradeConditions.csv: {missing_columns}")
        
        # Keep only the expected columns
        trade_conditions_df = trade_conditions_df[expected_columns]
        
        bool_cols = ['cancel', 'latereport', 'autoexecuted', 'openreport', 'volume', 'high', 'low', 'last']
        for col in bool_cols:
            if col in trade_conditions_df.columns:
                trade_conditions_df[col] = trade_conditions_df[col].astype(str).str.lower().map({
                    'true': True, 
                    'false': False,
                    '*': 'conditional' if col == 'last' else True
                }).fillna(False)
            else:
                logging.warning(f"Column '{col}' not found in TradeConditions.csv.")
                trade_conditions_df[col] = False
        logging.info(f"Loaded TradeConditions.csv with {len(trade_conditions_df)} entries.")
        return trade_conditions_df
    except Exception as e:
        logging.error(f"Failed to load TradeConditions.csv: {e}")
        raise

def load_exchange_codes(exchange_codes_path):
    """
    Loads and processes the ExchangeCodes.csv file.

    :param exchange_codes_path: Path to the ExchangeCodes.csv file
    :return: DataFrame containing exchange codes
    """
    try:
        # Read the CSV without specifying usecols, then normalize column names
        exchange_codes_df = pd.read_csv(exchange_codes_path)
        # Normalize column names to lowercase
        exchange_codes_df.columns = [col.lower() for col in exchange_codes_df.columns]
        
        logging.info(f"Loaded ExchangeCodes.csv with {len(exchange_codes_df)} entries.")
        return exchange_codes_df
    except Exception as e:
        logging.error(f"Failed to load ExchangeCodes.csv: {e}")
        raise

def load_trade_data(trade_data_path):
    """
    Loads and processes the trade data from a GZIP-compressed JSON file.

    :param trade_data_path: Path to the trade data file
    :return: Tuple of trades (list) and format_columns (list)
    """
    import gzip
    import json
    try:
        with gzip.open(trade_data_path, 'rt') as f:
            trade_data = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load trade data from {trade_data_path}: {e}")
        raise

    if 'header' not in trade_data or 'format' not in trade_data['header']:
        logging.error("Trade data is missing 'header' or 'format' information.")
        raise KeyError("Trade data is missing 'header' or 'format' information.")

    if 'response' not in trade_data:
        logging.error("Trade data is missing 'response' information.")
        raise KeyError("Trade data is missing 'response' information.")

    format_columns = trade_data['header']['format']
    # Normalize format columns to lowercase
    format_columns = [col.lower() for col in format_columns]
    trades = trade_data['response']
    logging.info(f"Loaded trade data with {len(trades)} trades.")
    return trades, format_columns

def validate_columns(df_trades):
    """
    Validates that essential columns are present in the trades DataFrame.

    :param df_trades: DataFrame with trade data
    :raises ValueError: If essential columns are missing
    """
    essential_columns = {'date', 'ms_of_day', 'price', 'size', 'condition'}
    missing_columns = essential_columns - set(df_trades.columns)
    if missing_columns:
        logging.error(f"Missing essential columns: {missing_columns}")
        raise ValueError(f"Missing essential columns: {missing_columns}")

def process_trades_dataframe(df_trades):
    """
    Process the trades DataFrame to set correct data types and timestamps.

    :param df_trades: DataFrame with raw trade data
    :return: Processed DataFrame
    """
    df_trades['date'] = pd.to_datetime(df_trades['date'], format='%Y%m%d', errors='coerce')
    df_trades['time_delta'] = pd.to_timedelta(df_trades['ms_of_day'], unit='ms', errors='coerce')
    df_trades['timestamp'] = df_trades['date'] + df_trades['time_delta']
    df_trades['price'] = pd.to_numeric(df_trades['price'], errors='coerce')
    df_trades['size'] = pd.to_numeric(df_trades['size'], errors='coerce')
    df_trades['condition'] = pd.to_numeric(df_trades['condition'], errors='coerce').astype('Int64')
    initial_count = len(df_trades)
    df_trades.dropna(subset=['timestamp', 'price', 'size', 'condition'], inplace=True)
    filtered_count = len(df_trades)
    logging.info(f"Dropped {initial_count - filtered_count} trades due to NaN values.")
    df_trades.sort_values(by='timestamp', inplace=True)
    df_trades.reset_index(drop=True, inplace=True)
    df_trades.drop(['time_delta'], axis=1, inplace=True)
    logging.info(f"Processed DataFrame with {len(df_trades)} trades.")
    return df_trades

def merge_trade_conditions(df_trades, trade_conditions_df):
    """
    Merge the trades DataFrame with trade conditions to include condition flags.

    :param df_trades: DataFrame with processed trade data
    :param trade_conditions_df: DataFrame with trade conditions
    :return: DataFrame merged with condition flags
    """
    df_trades = df_trades.merge(
        trade_conditions_df[['code', 'last', 'high', 'low', 'openreport', 'volume', 'name']],
        how='left',
        left_on='condition',
        right_on='code',
        suffixes=('', '_cond')
    )
    df_trades.drop(['code'], axis=1, inplace=True)
    missing_flags = df_trades['last'].isna().sum()
    if missing_flags > 0:
        logging.warning(f"{missing_flags} trades have unrecognized condition codes. Setting flags to False.")
        df_trades[['last', 'high', 'low', 'openreport', 'volume']] = df_trades[['last', 'high', 'low', 'openreport', 'volume']].fillna(False)
    else:
        logging.info("All condition codes successfully merged with trade data.")
    return df_trades

def merge_exchange_codes(df_trades, exchange_codes_df):
    """
    Merge the trades DataFrame with exchange codes to include exchange names.

    :param df_trades: DataFrame with trade data
    :param exchange_codes_df: DataFrame with exchange codes
    :return: DataFrame merged with exchange codes
    """
    df_trades = df_trades.merge(
        exchange_codes_df,
        how='left',
        left_on='exchange_code',
        right_on='code',
        suffixes=('', '_exchange')
    )
    missing_exchanges = df_trades['name'].isna().sum()
    if missing_exchanges > 0:
        logging.warning(f"{missing_exchanges} trades have unrecognized exchange codes.")
    else:
        logging.info("All exchange codes successfully mapped.")
    return df_trades
