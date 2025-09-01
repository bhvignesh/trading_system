# trading_system/tests/database/test_config.py

import pytest
from pathlib import Path
from src.database.config import DatabaseConfig

def test_database_config_initialization():
    """Test basic initialization of DatabaseConfig."""
    config = DatabaseConfig(url="sqlite:///test.db")
    assert config.url == "sqlite:///test.db"
    assert config.max_retries == 3
    assert config.pool_size == 5
    assert config.max_overflow == 10

def test_database_config_validation():
    """Test validation of DatabaseConfig parameters."""
    with pytest.raises(ValueError):
        DatabaseConfig(url="sqlite:///test.db", max_retries=0)
    
    with pytest.raises(ValueError):
        DatabaseConfig(url="sqlite:///test.db", pool_size=0)
    
    with pytest.raises(ValueError):
        DatabaseConfig(url="sqlite:///test.db", max_overflow=-1)

def test_default_config():
    """Test the default configuration generation."""
    config = DatabaseConfig.default()
    expected_path = Path(__file__).resolve().parent.parent.parent / "data" / "trading_system.db"
    assert config.url == f"sqlite:///{expected_path}"
    assert config.max_retries == 3
    assert config.pool_size == 5
    assert config.max_overflow == 10

# trading_system/tests/database/test_engine.py

import pytest
from sqlalchemy import create_engine, exc
from src.database.config import DatabaseConfig
from src.database.engine import create_db_engine
from unittest.mock import Mock, patch

@pytest.fixture
def mock_config():
    return DatabaseConfig(
        url="sqlite:///test.db",
        max_retries=3,
        pool_size=5,
        max_overflow=10
    )

def test_create_db_engine_success(mock_config):
    """Test successful database engine creation."""
    engine = create_db_engine(mock_config)
    assert engine is not None
    # Test connection
    with engine.connect() as conn:
        result = conn.execute("SELECT 1").scalar()
        assert result == 1

def test_create_db_engine_failure():
    """Test database engine creation failure."""
    invalid_config = DatabaseConfig(
        url="sqlite:///nonexistent/path/db.db",
        max_retries=1
    )
    with pytest.raises(exc.SQLAlchemyError):
        create_db_engine(invalid_config)

@patch('sqlalchemy.create_engine')
def test_engine_connection_validation(mock_create_engine, mock_config):
    """Test that connection validation is properly set up."""
    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    
    create_db_engine(mock_config)
    
    # Verify that create_engine was called with correct parameters
    mock_create_engine.assert_called_once_with(
        mock_config.url,
        pool_size=mock_config.pool_size,
        max_overflow=mock_config.max_overflow,
        pool_pre_ping=True
    )

# trading_system/tests/collectors/test_base_collector.py

import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from src.collectors.base_collector import BaseCollector

class TestCollector(BaseCollector):
    """Test implementation of BaseCollector for testing."""
    def refresh_data(self, ticker: str, **kwargs) -> None:
        pass

@pytest.fixture
def test_engine():
    """Create a test database engine."""
    return create_engine('sqlite:///:memory:')

@pytest.fixture
def collector(test_engine):
    """Create a test collector instance."""
    return TestCollector(test_engine)

def test_validate_dataframe(collector):
    """Test DataFrame validation."""
    # Valid DataFrame
    df = pd.DataFrame({
        'col1': [1, 2],
        'col2': ['a', 'b']
    })
    assert collector._validate_dataframe(df, ['col1', 'col2']) is True
    
    # Missing columns
    assert collector._validate_dataframe(df, ['col1', 'col3']) is False
    
    # Empty DataFrame
    assert collector._validate_dataframe(pd.DataFrame(), ['col1']) is False

def test_delete_existing_data(collector):
    """Test deletion of existing data."""
    # Setup test data
    df = pd.DataFrame({
        'ticker': ['AAPL', 'AAPL'],
        'value': [1, 2]
    })
    df.to_sql('test_table', collector.engine, index=False)
    
    collector._delete_existing_data('test_table', 'AAPL')
    
    # Verify deletion
    result = pd.read_sql('SELECT * FROM test_table WHERE ticker = "AAPL"',
                        collector.engine)
    assert len(result) == 0

def test_get_latest_date(collector):
    """Test retrieval of latest date."""
    # Setup test data
    dates = ['2023-01-01', '2023-01-02']
    df = pd.DataFrame({
        'ticker': ['AAPL', 'AAPL'],
        'date': pd.to_datetime(dates)
    })
    df.to_sql('test_table', collector.engine, index=False)
    
    latest_date = collector._get_latest_date('test_table', 'AAPL')
    assert latest_date == pd.to_datetime('2023-01-02')

def test_save_to_database(collector):
    """Test saving data to database."""
    df = pd.DataFrame({
        'ticker': ['AAPL'],
        'value': [100],
        'date': [datetime.now()]
    })
    
    collector._save_to_database(
        df, 'test_table', ['ticker', 'value', 'date']
    )
    
    # Verify save
    result = pd.read_sql('SELECT * FROM test_table', collector.engine)
    assert len(result) == 1
    assert result['ticker'].iloc[0] == 'AAPL'

# trading_system/tests/collectors/test_statements_collector.py

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
from src.collectors.statements_collector import StatementsCollector

@pytest.fixture
def mock_engine():
    return Mock()

@pytest.fixture
def collector(mock_engine):
    return StatementsCollector(mock_engine)

@patch('yfinance.Ticker')
def test_fetch_financial_statement(mock_ticker, collector):
    """Test fetching financial statements."""
    # Setup mock data
    mock_stock = Mock()
    mock_ticker.return_value = mock_stock
    
    mock_data = pd.DataFrame({
        'Total Assets': [100000, 120000],
        'Total Liabilities': [50000, 60000]
    }, index=['2022-12-31', '2023-12-31'])
    
    mock_stock.balance_sheet = mock_data
    
    # Test the fetch
    collector.fetch_financial_statement(
        'AAPL',
        'balance_sheet',
        lambda x: x.balance_sheet
    )
    
    # Verify the data was processed correctly
    mock_ticker.assert_called_once_with('AAPL')

@patch('yfinance.Ticker')
def test_fetch_financial_statement_no_data(mock_ticker, collector):
    """Test handling of no data returned."""
    mock_stock = Mock()
    mock_ticker.return_value = mock_stock
    mock_stock.balance_sheet = None
    
    collector.fetch_financial_statement(
        'AAPL',
        'balance_sheet',
        lambda x: x.balance_sheet
    )
    
    # Verify no database calls were made
    assert not collector.engine.execute.called

# trading_system/tests/collectors/test_price_collector.py

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
from src.collectors.price_collector import PriceCollector

@pytest.fixture
def mock_engine():
    return Mock()

@pytest.fixture
def collector(mock_engine):
    return PriceCollector(mock_engine)

@patch('yfinance.Ticker')
def test_fetch_and_save(mock_ticker, collector):
    """Test fetching and saving price data."""
    # Setup mock data
    mock_stock = Mock()
    mock_ticker.return_value = mock_stock
    
    mock_data = pd.DataFrame({
        'Open': [150.0, 151.0],
        'High': [152.0, 153.0],
        'Low': [149.0, 150.0],
        'Close': [151.5, 152.5],
        'Volume': [1000000, 1100000]
    }, index=pd.date_range(start='2023-01-01', periods=2))
    
    mock_stock.history.return_value = mock_data
    
    # Test the fetch
    collector.fetch_and_save('AAPL')
    
    # Verify the data was processed
    mock_ticker.assert_called_once_with('AAPL')
    mock_stock.history.assert_called_once()

@patch('yfinance.Ticker')
def test_refresh_data(mock_ticker, collector):
    """Test refreshing price data."""
    mock_stock = Mock()
    mock_ticker.return_value = mock_stock
    
    mock_data = pd.DataFrame({
        'Open': [150.0],
        'High': [152.0],
        'Low': [149.0],
        'Close': [151.5],
        'Volume': [1000000]
    }, index=pd.date_range(start='2023-01-01', periods=1))
    
    mock_stock.history.return_value = mock_data
    
    collector.refresh_data('AAPL')
    
    mock_ticker.assert_called_once_with('AAPL')
    mock_stock.history.assert_called_once_with(period='max')

# trading_system/tests/collectors/test_info_collector.py

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
from src.collectors.info_collector import InfoCollector

@pytest.fixture
def mock_engine():
    return Mock()

@pytest.fixture
def collector(mock_engine):
    return InfoCollector(mock_engine)

def test_flatten_nested_dict(collector):
    """Test dictionary flattening."""
    nested_dict = {
        'level1': {
            'level2': {
                'level3': 'value'
            }
        },
        'simple': 'value'
    }
    
    flat_dict = collector._flatten_nested_dict(nested_dict)
    
    assert flat_dict['level1_level2_level3'] == 'value'
    assert flat_dict['simple'] == 'value'

@patch('yfinance.Ticker')
def test_fetch_company_info(mock_ticker, collector):
    """Test fetching company information."""
    mock_stock = Mock()
    mock_ticker.return_value = mock_stock
    
    mock_info = {
        'shortName': 'Apple Inc.',
        'sector': 'Technology',
        'industry': 'Consumer Electronics'
    }
    
    mock_stock.info = mock_info
    
    collector.fetch_company_info('AAPL')
    
    mock_ticker.assert_called_once_with('AAPL')

# trading_system/tests/collectors/test_data_collector.py

import pytest
from unittest.mock import Mock, patch
from src.collectors.data_collector import (
    select_tickers_for_refresh,
    refresh_data_for_ticker,
    main
)

def test_select_tickers_for_refresh():
    """Test ticker selection for refresh."""
    all_tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'FB']
    day_of_month = 1
    
    selected = select_tickers_for_refresh(all_tickers, day_of_month)
    
    assert len(selected) > 0
    assert all(ticker in all_tickers for ticker in selected)
    
    # Test consistency
    selected_again = select_tickers_for_refresh(all_tickers, day_of_month)
    assert selected == selected_again

@patch('src.collectors.price_collector.PriceCollector')
@patch('src.collectors.info_collector.InfoCollector')
def test_refresh_data_for_ticker(mock_info_collector, mock_price_collector):
    """Test refreshing data for a single ticker."""
    collectors = {
        'price': mock_price_collector,
        'info': mock_info_collector
    }
    
    refresh_data_for_ticker('AAPL', collectors)
    
    mock_price_collector.refresh_data.assert_called_once_with('AAPL')
    mock_info_collector.refresh_info.assert_called_once_with('AAPL')

@pytest.mark.integration
def test_main():
    """Integration test for main function."""
    with patch('builtins.open', 
               Mock(return_value=Mock(__enter__=Mock(
                   return_value=Mock(
                       readlines=Mock(return_value=['AAPL\n', 'GOOGL\n'])
                   ))))):
        main(batch_size=1)  # Use small batch size for testing