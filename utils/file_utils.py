"""
Utility functions for file operations, including recursive file listing and loading tickers from YAML files.
"""

import os
from typing import Optional

import yaml


def get_all_files(directory):
    """
    Recursively collect all file paths in the given directory.

    Args:
        directory (str): The root directory to search for files.

    Returns:
        list[str]: A list of full file paths found in the directory.
    """
    all_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            all_files.append(os.path.join(root, file))
    return all_files

def load_tickers_from_yaml(file_path: str, max_tickers: Optional[int] = None) -> list[str]:
    """
    Load a list of tickers from a YAML file.

    The YAML file can contain either a plain list of tickers or
    a dictionary with a 'tickers' key containing the list.
    Optionally limits the number of tickers returned.

    Args:
        file_path (str): Path to the YAML file.
        max_tickers (Optional[int]): Maximum number of tickers to return.

    Returns:
        list[str]: List of ticker symbols.

    Raises:
        ValueError: If the YAML format is invalid or no tickers are loaded.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    ticker_list = []

    # Accept either a plain list, or a mapping with a 'tickers' key
    if isinstance(data, list):
        ticker_list = data
    elif isinstance(data, dict) and isinstance(data.get("tickers"), list):
        ticker_list = data["tickers"]
    else:
        raise ValueError("YAML must be a list of tickers or a mapping with a 'tickers' list.")

    if max_tickers and len(ticker_list) > max_tickers:
        ticker_list = ticker_list[:max_tickers]

    if not ticker_list:
        raise ValueError("No tickers loaded.")

    return ticker_list
