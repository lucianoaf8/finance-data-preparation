# utils/logging_setup.py

import logging
import os
from datetime import datetime

def setup_logging(script_name):
    """Setup logging configuration."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(log_dir, f'{script_name}_{timestamp}.log')
    
    # Create a custom logger
    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)
    
    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_filename)
    
    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    return logger
