# utils.py
import sqlite3

import pandas as pd


def query_to_dataframe(query, db_path):
    """
    Executes a SQL query on the specified SQLite database and returns the result as a pandas DataFrame.

    Parameters:
        query (str): The SQL query to execute.
        db_path (str): Path to the SQLite database file.

    Returns:
        pd.DataFrame: DataFrame containing the query results.
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn)
    return df
