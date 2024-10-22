# OHLC Builder

OHLC Builder is a Python package designed to construct OHLC (Open, High, Low, Close, Volume, Count) bars from raw trade data with accurate condition handling based on trade conditions and exchange codes.

## Features

- **Accurate Condition Handling**: Implements detailed logic for trade conditions to ensure high-quality OHLC data.
- **Modular Design**: Follows SOLID principles for maintainability and scalability.
- **Concurrency Support**: Optional concurrency module for performance optimization.
- **Data Validation**: Comprehensive validation and error handling.

## Installation

```bash
pip install -e .
```

## Project Structure

```
OHLC_builder/
├── OHLC_builder
│   ├── __init__.py
│   ├── builder.py
│   ├── concurrency.py
│   ├── utils.py
│   └── config.py
├── data
│   ├── exchange_codes.csv
│   ├── trade_conditions.csv
│   └── sample_trades.json.gz
├── tests
│   ├── __init__.py
│   └── test_builder.py
├── README.md
├── requirements.txt
└── setup.py
```

---

## `setup.py`

```python
from setuptools import setup, find_packages

setup(
    name='OHLC_builder',
    version='1.0.0',
    description='A package for building OHLC data from trade data with accurate condition handling.',
    author='Your Name',
    author_email='your.email@example.com',
    packages=find_packages(),
    install_requires=[
        'pandas>=1.1.0',
        'numpy>=1.18.0',
    ],
    python_requires='>=3.7',
)
```

---

## `requirements.txt`

```
pandas>=1.1.0
numpy>=1.18.0
```

## Usage

```python
from OHLC_builder import OHLCBuilder
from OHLC_builder.utils import load_trade_conditions, load_exchange_codes, load_trade_data

# Load data
trade_conditions_df = load_trade_conditions('data/trade_conditions.csv')
exchange_codes_df = load_exchange_codes('data/exchange_codes.csv')
trades, format_columns = load_trade_data('data/sample_trades.json.gz')

# Initialize builder
builder = OHLCBuilder(
    trades=trades,
    format_columns=format_columns,
    trade_conditions_df=trade_conditions_df,
    exchange_codes_df=exchange_codes_df
)

# Generate OHLC data
OHLC = builder.get_OHLC(interval='1min')

# Save or process OHLC DataFrame as needed
OHLC.to_csv('data/OHLC_output.csv')
```

## Concurrency

To enable concurrency, import the `concurrency` module and adjust the settings in `config.py`.

## Testing

```bash
python -m unittest discover tests
```

## License

This project is licensed under the MIT License.


**How to Run the Package**

1. **Install Dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

2. **Install the Package**:

   ```bash
   pip install -e .
   ```

3. **Run the Main Script**:

   ```bash
   python main.py
   ```

4. **Run Tests**:

   ```bash
   python -m unittest discover tests
   ```
