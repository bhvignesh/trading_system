# trading_system/src/database/config.py

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class DatabaseConfig:
    """
    Database configuration with enhanced settings for trading system.
    
    Supports both SQLite and PostgreSQL with optimized connection parameters
    for high-frequency data operations and concurrent access patterns.
    """
    url: str
    max_retries: int = field(default=3)
    pool_size: int = field(default=10)
    max_overflow: int = field(default=20)
    pool_timeout: int = field(default=30)
    pool_recycle: int = field(default=3600)
    echo: bool = field(default=False)
    
    @staticmethod
    def default(db_type: str = "sqlite") -> "DatabaseConfig":
        """
        Create an optimized instance of DatabaseConfig based on the specified database type.

        This method configures a DatabaseConfig instance for either SQLite or PostgreSQL.
        For "sqlite", it constructs a path to the database file within the "data" directory (creating
        the directory if it does not exist) and sets connection pooling parameters optimized for SQLite.
        For "postgresql", it returns a configuration with standard connection pooling parameters.
        
        Args:
            db_type (str): The type of database to configure. Accepted values are "sqlite" or "postgresql".
        
        Returns:
            DatabaseConfig: A configured instance of DatabaseConfig tailored for the chosen database type.
        
        Raises:
            ValueError: If an unsupported database type is specified.
        """
        base_path = Path(__file__).resolve().parent.parent.parent / "data"
        base_path.mkdir(exist_ok=True)
        
        if db_type.lower() == "sqlite":
            database_path = base_path / "trading_system.db"
            return DatabaseConfig(
                url=f"sqlite:///{database_path}",
                pool_size=1,  # SQLite doesn't benefit from connection pooling
                max_overflow=0
            )
        elif db_type.lower() == "postgresql":
            # Example PostgreSQL configuration - adjust as needed
            return DatabaseConfig(
                url="postgresql://user:password@localhost:5432/trading_system",
                pool_size=10,
                max_overflow=20
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")