# trading_system/src/database/remove_dupliactes.py

import logging
import os
import sys

from sqlalchemy import text

print(os.getcwd())
# Determine the absolute path to the root directory (assuming the script is one level down)
root_path = os.path.abspath(os.getcwd())
print(root_path)

# Add the root directory to sys.path if it’s not already there
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from src.database.config import DatabaseConfig
from src.database.engine import create_db_engine


def clean_duplicate_dates(db_config: DatabaseConfig):
    """
    SQLite-compatible duplicate cleaning with proper connection handling
    """
    engine = create_db_engine(db_config)
    logger = logging.getLogger(__name__)
    conn = None  # Explicit connection reference
    
    try:
        # Get raw DBAPI connection
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # SQLite-specific deletion using rowid
        delete_query = """
            DELETE FROM daily_prices
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM daily_prices
                GROUP BY ticker, date
            );
        """
        
        cursor.execute(delete_query)
        deleted_rows = conn.total_changes
        conn.commit()  # Explicit commit
        
        logger.info(f"Removed {deleted_rows} duplicate records")
        
    except Exception as e:
        logger.error(f"Cleaning failed: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Proper cleanup
        if conn:
            conn.close()
        if engine:
            engine.dispose()

if __name__ == "__main__":
    import logging

    from src.database.config import DatabaseConfig
    
    logging.basicConfig(level=logging.INFO)
    db_config = DatabaseConfig.default()
    clean_duplicate_dates(db_config)