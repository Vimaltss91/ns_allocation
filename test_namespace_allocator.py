import pytest
import json
import yaml
import yaml
from unittest.mock import patch, MagicMock
from namespace_allocator import NamespaceAllocator
from db_connection import DatabaseConnection


@pytest.fixture
def mock_db_connection():
    """Fixture to create a mock database connection and cursor."""
    mock_connection = MagicMock()
    mock_connection.commit = MagicMock()
    mock_cursor = MagicMock()
    mock_db_connection = MagicMock()
    mock_db_connection.connection = mock_connection
    mock_db_connection.get_cursor.return_value.__enter__.return_value = mock_cursor
    return mock_db_connection, mock_cursor


@pytest.fixture
def test_data():
    """Fixture to load test data from JSON file."""
    with open('test_data.json', 'r') as f:
        return json.load(f)


def test_extract_args_yaml(mock_db_connection):
    """Test the extract_args method with YAML source."""
    db_connection, _ = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    # Load the YAML data from a file
    with open('test_data.yaml', 'r') as yaml_file:
        yaml_data = yaml.safe_load(yaml_file)

    namespace_value = yaml_data['Testbsf']['variables']['NAMESPACE']
    mock_parse_vars_data = {'namespace': namespace_value}

    with patch('namespace_allocator._extract_from_yaml', return_value=mock_parse_vars_data) as mock_extract_yaml, \
            patch('namespace_allocator._parse_variables', return_value=mock_parse_vars_data) as mock_parse_vars:
        result = allocator.extract_args('yaml', 'test_data.yaml')

        assert result['namespace'] == 'o-devops-123'
        mock_extract_yaml.assert_called_once_with('test_data.yaml')
        mock_parse_vars.assert_called_once_with(mock_parse_vars_data)


def test_insert_or_update_status_hardcoded_namespace(mock_db_connection, test_data):
    """Test insert_or_update_status method with a hardcoded namespace."""
    db_connection, mock_cursor = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    kwargs = test_data['insert_or_update_status']
    expected_kwargs = {**kwargs, 'status': 'ASSIGNED'}

    with patch('namespace_allocator.get_existing_namespace', return_value=True) as mock_get_existing_namespace, \
            patch('namespace_allocator.NamespaceAllocator._update_namespace_status') as mock_update_ns_status, \
            patch('namespace_allocator.NamespaceAllocator._insert_new_status') as mock_insert_new_status, \
            patch('namespace_allocator.NamespaceAllocator._update_namespace_pool_status') as mock_update_ns_pool:
        allocator.insert_or_update_status(**kwargs)

        mock_get_existing_namespace.assert_called_once_with(mock_cursor, 'o-devops-123')
        mock_update_ns_status.assert_called_once_with(mock_cursor, expected_kwargs)
        mock_update_ns_pool.assert_called_once_with(mock_cursor, 'o-devops-123')
        mock_insert_new_status.assert_not_called()


def test_allocate_namespace_no_available_namespace(mock_db_connection, test_data):
    """Test allocate_namespace when no namespace is available."""
    db_connection, mock_cursor = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    kwargs = test_data['allocate_namespace']

    with patch('namespace_allocator.get_assigned_status', return_value=None) as mock_get_assigned_status, \
            patch('namespace_allocator.find_and_lock_available_namespace', return_value=None) as mock_find_and_lock, \
            patch('namespace_allocator.fetch_total_cpu_requests_with_validation', return_value=1000) as mock_fetch_cpu:

        result = allocator.allocate_namespace(**kwargs)

        mock_get_assigned_status.assert_called_once_with(mock_cursor, *list(kwargs.values()))
        mock_find_and_lock.assert_called_once_with(mock_cursor, 'test')
        if mock_find_and_lock.return_value is None:
            assert mock_fetch_cpu.call_count == 0
        else:
            mock_fetch_cpu.assert_called_once()
        assert result is None


def test_delete_namespace_success(mock_db_connection):
    """Test delete_namespace method for successful deletion."""
    db_connection, mock_cursor = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    namespace_name = 'o-devops-bsf10'

    with patch('namespace_allocator.delete_namespace_from_status') as mock_delete_namespace, \
            patch('namespace_allocator.update_namespace_status') as mock_update_ns_status:
        allocator.delete_namespace(namespace_name)

        mock_delete_namespace.assert_called_once_with(mock_cursor, namespace_name)
        mock_update_ns_status.assert_called_once_with(mock_cursor, namespace_name)
        db_connection.connection.commit.assert_called_once()


def test_allocate_namespace_exception_handling(mock_db_connection, test_data):
    """Test allocate_namespace handling exceptions during database operations."""
    db_connection, mock_cursor = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    kwargs = test_data['allocate_namespace']

    # Simulate an exception during the call to get_assigned_status
    with patch('namespace_allocator.get_assigned_status', side_effect=Exception('Database error')) as mock_get_assigned_status, \
            patch('namespace_allocator.find_and_lock_available_namespace', return_value=None) as mock_find_and_lock, \
            patch('namespace_allocator.fetch_total_cpu_requests_with_validation', return_value=1000) as mock_fetch_cpu:

        # Wrap the call in a try-except block to catch the exception
        try:
            allocator.allocate_namespace(**kwargs)
            # If no exception is raised, the test should fail
            assert False, "Expected an exception to be raised"
        except Exception as e:
            # Verify that the exception is the one we expected
            assert str(e) == 'Database error'

        # Ensure that other methods were not called
        mock_find_and_lock.assert_not_called()
        mock_fetch_cpu.assert_not_called()


def test_allocate_namespace_with_existing_namespace(mock_db_connection, test_data):
    """Test allocate_namespace when an existing namespace is assigned."""
    db_connection, mock_cursor = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    kwargs = test_data['allocate_namespace']

    # Simulate `assigned_status` as a tuple with index 1 as 'ASSIGNED'
    with patch('namespace_allocator.get_assigned_status', return_value=('namespace', 'ASSIGNED', 'namespace_name', 'pipeline_url')) as mock_get_assigned_status, \
            patch('namespace_allocator.find_and_lock_available_namespace', return_value=None) as mock_find_and_lock, \
            patch('namespace_allocator.fetch_total_cpu_requests_with_validation', return_value=1000) as mock_fetch_cpu:
        result = allocator.allocate_namespace(**kwargs)

        mock_get_assigned_status.assert_called_once_with(mock_cursor, *list(kwargs.values()))
        mock_find_and_lock.assert_not_called()  # This should not be called as namespace is assigned
        mock_fetch_cpu.assert_not_called()  # This should not be called as namespace is assigned
        assert result == 'pipeline_url'  # Ensure the correct return value

def test_insert_or_update_status_empty_input(mock_db_connection, test_data):
    """Test insert_or_update_status with empty input data."""
    db_connection, mock_cursor = mock_db_connection
    allocator = NamespaceAllocator(db_connection)

    kwargs = test_data['insert_or_update_status_empty']
    expected_kwargs = {**kwargs, 'status': 'ASSIGNED'}

    with patch('namespace_allocator.get_existing_namespace', return_value=None) as mock_get_existing_namespace, \
            patch('namespace_allocator.NamespaceAllocator._update_namespace_status') as mock_update_ns_status, \
            patch('namespace_allocator.NamespaceAllocator._insert_new_status') as mock_insert_new_status, \
            patch('namespace_allocator.NamespaceAllocator._update_namespace_pool_status') as mock_update_ns_pool:
        allocator.insert_or_update_status(**kwargs)

        mock_get_existing_namespace.assert_called_once_with(mock_cursor, kwargs['namespace'])
        mock_update_ns_status.assert_not_called()
        mock_insert_new_status.assert_called_once_with(mock_cursor, expected_kwargs)
        mock_update_ns_pool.assert_called_once_with(mock_cursor, kwargs['namespace'])


