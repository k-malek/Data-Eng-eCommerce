from pathlib import Path
import pandas as pd
import logging

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
LOG_DIR = Path(__file__).parent.parent / "logs"
TEMP_STORAGE_DIR = Path(__file__).parent.parent / "data" / "intermediate"
TEMP_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# Configure logging

def get_logger():
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        handler = logging.FileHandler(LOG_DIR / "extract.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def extract_transactions():
    """
    Extracts transaction data from a CSV file and sets 'order_id' as the index.

    Returns:
        pd.DataFrame: DataFrame containing the transaction data with 'order_id' as the index.

    Raises:
        FileNotFoundError: If the CSV file is not found.
        pd.errors.EmptyDataError: If the CSV file is empty.
        pd.errors.ParserError: If there is a parsing error while reading the CSV file.
        Exception: For any other unexpected errors.
    """
    logger = get_logger()

    file_path = DATA_DIR / 'transactions_mock.csv'
    
    try:
        # Load the transactions data
        data = pd.read_csv(DATA_DIR / 'transactions_mock.csv')
        data.set_index('order_id', inplace=True)
        logger.info('Transactions data loaded successfully.')

        data.to_parquet(TEMP_STORAGE_DIR / "transactions.parquet", index=True)
        logger.info('Transactions data saved to parquet.')
        return data
    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
        raise
    except pd.errors.EmptyDataError as e:
        logger.error('No data: %s', e)
        raise
    except pd.errors.ParserError as e:
        logger.error('Parsing error: %s', e)
        raise
    except Exception as e:
        logger.error('Unexpected error: %s', e)
        raise

def extract_profiles():
    """
    Extracts profile data from a JSON file and sets 'user_id' as the index.

    Returns:
        pd.DataFrame: DataFrame containing the profile data with 'user_id' as the index.

    Raises:
        FileNotFoundError: If the JSON file is not found.
        ValueError: If there is a value error while reading the JSON file.
        Exception: For any other unexpected errors.
    """
    logger = get_logger()

    file_path = DATA_DIR / 'profiles_mock.json'

    try:
        # Load the profiles data
        data = pd.read_json(DATA_DIR / 'profiles_mock.json')
        data.set_index('user_id', inplace=True)
        logger.info('Profiles data loaded successfully.')

        data.to_parquet(TEMP_STORAGE_DIR / "profiles.parquet", index=True)
        logger.info('Profiles data saved to parquet.')
        return data
    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
        raise
    except ValueError as e:
        logger.error('Value error: %s', e)
        raise
    except Exception as e:
        logger.error('Unexpected error: %s', e)
        raise
