from setuptools import setup, find_packages

setup(
    name='ohlcvc_builder',
    version='1.0.0',
    description='A package for building OHLCVC data from trade data with accurate condition handling.',
    author='TalesofThales',
    author_email='',
    packages=find_packages(),
    install_requires=[
        'pandas>=1.1.0',
        'numpy>=1.18.0',
    ],
    python_requires='>=3.7',
)
