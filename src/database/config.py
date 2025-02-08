# trading_system/src/database/config.py

from pathlib import Path
from dataclasses import dataclass, field

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    url: str
    max_retries: int = field(default=3, metadata={"validate": lambda x: x > 0})
    pool_size: int = field(default=5, metadata={"validate": lambda x: x >= 1})
    max_overflow: int = field(default=10, metadata={"validate": lambda x: x >= 0})

    @staticmethod
    def default():
        """Generate a default configuration with the database in the 'data' folder."""
        base_path = Path(__file__).resolve().parent.parent.parent / "data"
        base_path.mkdir(exist_ok=True)  # Create 'data' directory if it doesn't exist
        # Use a .duckdb extension for a DuckDB database file.
        database_path = base_path / "trading_system.duckdb"
        return DatabaseConfig(url=f"duckdb:///{database_path}")
