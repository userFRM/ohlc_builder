# OHLCVC Builder

OHLCVC Builder is a Python package designed to construct OHLCVC (Open, High, Low, Close, Volume, Count) bars from raw trade data with accurate condition handling based on trade conditions and exchange codes.

## Features

- **Accurate Condition Handling**: Implements detailed logic for trade conditions to ensure high-quality OHLCVC data.
- **Modular Design**: Follows SOLID principles for maintainability and scalability.
- **Concurrency Support**: Optional concurrency module for performance optimization.
- **Data Validation**: Comprehensive validation and error handling.

## Installation

```bash
pip install -e .


Certainly! Below is a refactored and comprehensive enterprise-grade Python package for building OHLCVC data from trade data. The package is structured to be modular, adheres to SOLID principles, includes detailed docstrings, and is optimized for performance. Concurrency is implemented in a separate module and can be easily adjusted or removed.

---

## Project Structure

```
ohlcvc_builder/
├── ohlcvc_builder
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
    name='ohlcvc_builder',
    version='1.0.0',
    description='A package for building OHLCVC data from trade data with accurate condition handling.',
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

---

## `README.md`

```markdown
# OHLCVC Builder

OHLCVC Builder is a Python package designed to construct OHLCVC (Open, High, Low, Close, Volume, Count) bars from raw trade data with accurate condition handling based on trade conditions and exchange codes.

## Features

- **Accurate Condition Handling**: Implements detailed logic for trade conditions to ensure high-quality OHLCVC data.
- **Modular Design**: Follows SOLID principles for maintainability and scalability.
- **Concurrency Support**: Optional concurrency module for performance optimization.
- **Data Validation**: Comprehensive validation and error handling.

## Installation

```bash
pip install -e .
```

## Usage

```python
from ohlcvc_builder import OHLCVCBuilder
from ohlcvc_builder.utils import load_trade_conditions, load_exchange_codes, load_trade_data

# Load data
trade_conditions_df = load_trade_conditions('data/trade_conditions.csv')
exchange_codes_df = load_exchange_codes('data/exchange_codes.csv')
trades, format_columns = load_trade_data('data/sample_trades.json.gz')

# Initialize builder
builder = OHLCVCBuilder(
    trades=trades,
    format_columns=format_columns,
    trade_conditions_df=trade_conditions_df,
    exchange_codes_df=exchange_codes_df
)

# Generate OHLCVC data
ohlcvc = builder.get_ohlcvc(interval='1min')

# Save or process ohlcvc DataFrame as needed
ohlcvc.to_csv('data/ohlcvc_output.csv')
```

## Concurrency

To enable concurrency, import the `concurrency` module and adjust the settings in `config.py`.

## Testing

```bash
python -m unittest discover tests
```

## License

This project is licensed under the MIT License.
```

---

## `ohlcvc_builder/__init__.py`

```python
from .builder import OHLCVCBuilder
```

---

**Notes:**

- **Concurrency Module**: The `concurrency.py` module allows for optional concurrency using Python's `concurrent.futures`. By adjusting `ENABLE_CONCURRENCY` and `MAX_WORKERS` in `config.py`, you can enable or disable concurrency and control the number of worker threads.
- **Modular Design**: The code is organized into modules (`builder.py`, `utils.py`, `concurrency.py`) to enhance maintainability and scalability.
- **Performance Optimization**: Critical sections of the code are optimized for speed, and concurrency is used where appropriate.
- **Quality Docstrings**: All classes and functions include detailed docstrings that explain their purpose, parameters, and return values.
- **Testing**: A basic unit test is provided in `tests/test_builder.py` to ensure that the builder initializes correctly and generates OHLCVC data.

---

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
