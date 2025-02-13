import numpy as np
import pandas as pd
from .utils.logging_handler import get_logger
from pathlib import Path

CLN_STORAGE_DIR = Path(__file__).parent.parent / "data" / "cleaned"
CLN_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

def clean_transactions(data: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the transaction data by removing duplicates and missing values.

    Args:
        data (pd.DataFrame): Raw transaction data.

    Returns:
        pd.DataFrame: Cleaned transaction data.

    Raises:
        ValueError: If the input DataFrame is empty after cleaning.
    """
    logger = get_logger('clean')

    try:
        # Remove duplicates
        data.drop_duplicates(inplace=True)
        logger.info('Removed duplicates from transaction data.')

        # Remove rows with missing ids
        data.dropna(subset=['order_id','user_id','quantity'], inplace=True)
        logger.info('Removed rows with missing ids from transaction data.')

        # Fill missing values
        data['product']=data['product'].fillna('Unknown')
        data['category']=data['category'].fillna('Unknown')
        data['price'] = data.apply(lambda row: fill_missing_price(row, data), axis=1)
        data['timestamp']=pd.to_datetime(data['timestamp'], errors='coerce').fillna(pd.Timestamp.now())
        logger.info('Filled missing values in transaction data.')

        # Remove rows with missing prices
        data.dropna(subset=['price'], inplace=True)
        logger.info('Removed rows with missing prices from transaction data.')

        # Convert columns to appropriate data types
        data['order_id']=data['order_id'].astype(int)
        data['user_id']=data['user_id'].astype(int)
        data['product']=data['product'].astype(str)
        data['category']=data['category'].astype(str)
        data['price']=data['price'].astype(float)
        data['quantity']=data['quantity'].astype(int)
        logger.info('Converted columns to appropriate data types in transaction data.')

        # Additional cleaning steps
        data['category']=data['category'].str.title()
        logger.info('Additional cleaning steps applied to transaction data.')

        # Check if data is empty after cleaning
        if data.empty:
            raise ValueError('No data left after cleaning.')
        
        # Save cleaned data to parquet
        data.to_parquet(CLN_STORAGE_DIR / "transactions.parquet", index=True)
        logger.info('Cleaned transaction data saved to parquet.')

        return data
    except ValueError as e:
        logger.error('Error cleaning data: %s', e)
        raise

def clean_profiles(data: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the profile data by removing duplicates and missing values.

    Args:
        data (pd.DataFrame): Raw profile data.

    Returns:
        pd.DataFrame: Cleaned profile data.

    Raises:
        ValueError: If the input DataFrame is empty after cleaning.
    """
    logger = get_logger('clean')

    try:
        # Remove duplicates
        data.drop_duplicates(inplace=True)
        logger.info('Removed duplicates from profile data.')

        # Remove rows with missing ids
        data.dropna(subset=['user_id'], inplace=True)
        logger.info('Removed rows with missing ids from transaction data.')

        # Fill missing values
        data['name']=data['name'].replace(r'^\s*$', 'John Doe', regex=True) # Replace empty names with 'John Doe'
        data['signup_date']=pd.to_datetime(data['signup_date'], errors='coerce').fillna(pd.Timestamp.now().normalize())
        data['location']=data['location'].replace(r'^\s*$','Unknown',regex=True) # Replace empty locations with 'Unknown'
        logger.info('Filled missing values in profile data.')

        # Convert columns to appropriate data types
        data['user_id']=data['user_id'].astype(int)
        data['name']=data['name'].astype(str)
        data['email']=data['email'].astype(str)
        data['location']=data['location'].astype(str)
        logger.info('Converted columns to appropriate data types in profile data.')

        # Additional cleaning steps
        data['location']=data['location'].str.split(',').str[0].str.title()
        logger.info('Additional cleaning steps applied to profile data.')

        # Check if data is empty after cleaning
        if data.empty:
            raise ValueError('No data left after cleaning.')
        
        # Save cleaned data to parquet
        data.to_parquet(CLN_STORAGE_DIR / "profiles.parquet", index=True)
        logger.info('Cleaned profile data saved to parquet.')

        return data
    except ValueError as e:
        logger.error('Error cleaning data: %s', e)
        raise

def fill_missing_price(row: pd.Series, data: pd.DataFrame) -> float:
    """
    Custom function to fill missing price based on product name.

    Args:
        row (pd.Series): The row of the DataFrame being processed.
        data (pd.DataFrame): The entire DataFrame.

    Returns:
        float: The filled price.
    """
    if pd.isna(row['price']):
        # Look up the price for the same product
        similar_product = data[data['product'] == row['product']]['price']
        if len(similar_product) > 1:
            return similar_product.iloc[0]
        else:
            return np.nan
    else:
        return row['price']
