from pathlib import Path
import pandas as pd
from .utils.logging_handler import get_logger

TRNS_STORAGE_DIR = Path(__file__).parent.parent / "data" / "transformed"
TRNS_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

def transform_transactions(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the transaction data by calculating the total price.

    Args:
        data (pd.DataFrame): Raw transaction data.

    Returns:
        pd.DataFrame: Transformed transaction data with total price.
    """
    logger=get_logger('transform')
    
    data['total_price'] = data['quantity'] * data['price']

    logger.info('Transformation performed on transaction data.')
    return data

def get_labels(data: pd.DataFrame, low_threshold: float = 1000, high_threshold: float = 5000) -> pd.Series:
    """
    Generates customer segmentation labels based on customer lifetime value.

    Args:
        data (pd.DataFrame): DataFrame containing the customer lifetime value.
        low_threshold (float): Threshold for low segment.
        high_threshold (float): Threshold for high segment.

    Returns:
        pd.Series: Series containing the segmentation labels.
    """
    labels = ['Low', 'Medium', 'High']
    return pd.Series(pd.cut(data['customer_lifetime_value'], bins=[-float('inf'), low_threshold, high_threshold, float('inf')], labels=labels))

def consolidate_data(transactions: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidates transaction and user data into a single DataFrame.

    Args:
        transactions (pd.DataFrame): DataFrame containing transaction data.
        users (pd.DataFrame): DataFrame containing user data.

    Returns:
        pd.DataFrame: Consolidated DataFrame with additional calculated fields.
    """
    logger=get_logger('transform')
    
    transactions = transform_transactions(transactions)
    
    # Merge the datasets to create a consolidated transaction dataset
    data = pd.merge(transactions, users, on='user_id')
    logger.info('Transaction and user data consolidated.')
    
    # Drop the columns that are not needed
    data.drop(columns=['signup_date', 'email', 'name'], inplace=True)
    logger.info('Dropped unnecessary columns.')

    # Reorder the columns
    data = data[['order_id', 'user_id', 'product', 'category', 'price', 'quantity', 'total_price', 'timestamp', 'location']]
    logger.info('Reordered columns.')

    # Calculate the customer lifetime value
    data['customer_lifetime_value'] = data.groupby('user_id')['total_price'].transform('sum')
    logger.info('Calculated customer lifetime value.')

    # Generate customer segmentation labels
    ## Apply segmentation to unique users
    unique_users = data[['user_id', 'customer_lifetime_value']].drop_duplicates()
    unique_users['segment'] = get_labels(unique_users)

    ## Merge back with transaction data
    data = pd.merge(data, unique_users[['user_id', 'segment']], on='user_id', how='left')
    logger.info('Generated customer segmentation labels.')

    # Set the order_id as the index
    data.set_index('order_id', inplace=True)
    
    # Save the consolidated data to parquet
    data.to_parquet(TRNS_STORAGE_DIR / "processed_transactions.parquet", index=True)
    logger.info('Consolidated data saved to parquet.')

    return data