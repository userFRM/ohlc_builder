from ohlcvc_builder import OHLCVCBuilder
from ohlcvc_builder.utils import load_trade_conditions, load_exchange_codes, load_trade_data

def main():
    """
    Main function to execute the OHLCVC building process.
    """
    # Define paths to data files
    trade_data_path = 'data/sample_trades.gz'
    trade_conditions_path = 'data/trade_conditions.csv'
    exchange_codes_path = 'data/exchange_codes.csv'  # Optional; set to None if not used

    # Load trade data
    trades, format_columns = load_trade_data(trade_data_path)

    # Load trade conditions
    trade_conditions_df = load_trade_conditions(trade_conditions_path)

    # Load exchange codes if available
    exchange_codes_df = load_exchange_codes(exchange_codes_path)

    # Initialize the OHLCVCBuilder with dataframes
    builder = OHLCVCBuilder(
        trades=trades,
        format_columns=format_columns,
        trade_conditions_df=trade_conditions_df,
        exchange_codes_df=exchange_codes_df
    )

    # Generate the OHLCVC data for 1-minute bars
    ohlcvc = builder.get_ohlcvc(interval='1min')

    # Save the results to a CSV file
    ohlcvc.to_csv('data/ohlcvc_output.csv')
    print('OHLCVC Data:')
    print(ohlcvc)

if __name__ == "__main__":
    main()
