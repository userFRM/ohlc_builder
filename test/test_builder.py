import unittest
from ohlcvc_builder import OHLCVCBuilder
from ohlcvc_builder.utils import load_trade_conditions, load_exchange_codes, load_trade_data

class TestOHLCVCBuilder(unittest.TestCase):
    def setUp(self):
        # Load data for testing
        self.trade_conditions_df = load_trade_conditions('data/trade_conditions.csv')
        self.exchange_codes_df = load_exchange_codes('data/exchange_codes.csv')
        self.trades, self.format_columns = load_trade_data('data/sample_trades.json.gz')

    def test_builder_initialization(self):
        builder = OHLCVCBuilder(
            trades=self.trades,
            format_columns=self.format_columns,
            trade_conditions_df=self.trade_conditions_df,
            exchange_codes_df=self.exchange_codes_df
        )
        self.assertIsNotNone(builder.df_trades)

    def test_get_ohlcvc(self):
        builder = OHLCVCBuilder(
            trades=self.trades,
            format_columns=self.format_columns,
            trade_conditions_df=self.trade_conditions_df,
            exchange_codes_df=self.exchange_codes_df
        )
        ohlcvc = builder.get_ohlcvc(interval='1min')
        self.assertIsNotNone(ohlcvc)
        self.assertFalse(ohlcvc.empty)

if __name__ == '__main__':
    unittest.main()
