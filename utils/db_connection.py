# utils/db_connection.py

import os
import sys
from urllib.parse import urlparse, quote_plus
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy.pool import QueuePool

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_setup import setup_logging

# Load environment variables from .env file
load_dotenv()

logger = setup_logging('db_connection')

def get_engine(db_name):
    """Establish and return a database engine with connection pooling."""
    logger.info(f"Setting up database engine for {db_name}")
    
    try:
        mysql_url = os.getenv("MYSQL_URL")
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        
        if not all([mysql_url, mysql_user, mysql_password]):
            logger.error("Missing one or more database credentials in .env file")
            raise ValueError("Missing one or more database credentials in .env file")
        
        url = urlparse(mysql_url)
        
        logger.info(f"Parsed URL - Hostname: {url.hostname}, Port: {url.port}, Path: {url.path}")
        
        # URL-encode the password to handle special characters
        encoded_password = quote_plus(mysql_password)
        
        # Construct the connection URL directly
        connection_url = f"mysql+pymysql://{mysql_user}:{encoded_password}@{url.hostname}:{url.port if url.port else 3306}/{db_name}"
        
        logger.info("Connection URL constructed successfully")
        
        # Create and return the engine with connection pooling
        engine = sqlalchemy.create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        
        # Test the connection by connecting to the database
        engine.connect()
        
        logger.info(f"Database engine for {db_name} created successfully")
        print(f"Database connection to {db_name} was successful.")
        return engine
    
    except Exception as e:
        logger.error(f"Error setting up database engine for {db_name}: {e}")
        print(f"Database connection to {db_name} failed: {e}")
        raise

if __name__ == '__main__':
    # Test the database connection
    try:
        get_engine(os.getenv('PLAID_DB'))
        get_engine(os.getenv('MBNA_DB'))
    except Exception as e:
        print(f"Error: {e}")
