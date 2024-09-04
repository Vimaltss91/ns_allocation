import pytest
import json
import yaml
from unittest.mock import patch, MagicMock
from namespace_allocator import NamespaceAllocator
from db_connection import DatabaseConnection

# Common Fixtures
# @pytest.fixture
# def mock_db_connection():
#     """Fixture to create a mock database connection and cursor."""
#     mock_connection = MagicMock()
#     mock_cursor = MagicMock()
#     mock_db_connection = MagicMock()
#     mock_db_connection.connection = mock_connection
#     mock_db_connection.get_cursor.return_value.__enter__.return_value = mock_cursor
#     return mock_db_connection, mock_cursor

@pytest.fixture
def mock_db_connection():
    """Fixture to create a mock database connection and cursor."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_db_connection = MagicMock()
    mock_db_connection.connection = mock_connection
    mock_db_connection.get_cursor.return_value.__enter__.return_value = mock_cursor
    return mock_db_connection, mock_cursor

@pytest.fixture
def test_data():
    """Fixture to load test data from JSON file."""
    with open('test/test_data.json', 'r') as f:
        return json.load(f)

@pytest.fixture
def namespace_allocator(mock_db_connection):
    """Fixture to create an instance of NamespaceAllocator."""
    db_connection, _ = mock_db_connection
    return NamespaceAllocator(db_connection)

# Improved Test Functions

def test_extract_args_yaml(namespace_allocator):
    """Test the extract_args method with YAML source."""
    with open('test/test_data.yaml', 'r') as yaml_file:
        yaml_data = yaml.safe_load(yaml_file)

    # Extract variables from YAML data
    variables = yaml_data['Testbsf']['variables']

    # Create a mock response that matches the variables
    mock_parse_vars_data = variables

    with patch('namespace_allocator._extract_from_yaml', return_value=mock_parse_vars_data) as mock_extract_yaml, \
            patch('namespace_allocator._parse_variables', return_value=mock_parse_vars_data) as mock_parse_vars:
        result = namespace_allocator.extract_args('yaml', 'test_data.yaml')

        # Assert each variable value
        assert result['BUILD_NF'] == 'bsf', "Expected BUILD_NF to be 'bsf'"
        assert result['CSAR_DEPLOYMENT'] == 'true', "Expected CSAR_DEPLOYMENT to be 'true'"
        assert result['BSF_RELEASE_TAG'] == '23.4.1-ocngf-40008', "Expected BSF_RELEASE_TAG to be '23.4.1-ocngf-40008'"
        assert result['INCLUDE_INTERNAL_ATS_FEATURES'] == 'false', "Expected INCLUDE_INTERNAL_ATS_FEATURES to be 'false'"
        assert result['REPORT'] == 'false', "Expected REPORT to be 'false'"
        assert result['ATS_RELEASE_TAG'] == '23.4.1-ocngf-40008', "Expected ATS_RELEASE_TAG to be '23.4.1-ocngf-40008'"
        assert result['CUSTOM_NOTIFICATION_MESSAGE'] == '@devops testing 3', "Expected CUSTOM_NOTIFICATION_MESSAGE to be '@devops testing 3'"
        assert result['NAMESPACE'] == 'o-devops-123', "Expected NAMESPACE to be 'o-devops-123'"

        # Check that the mocks were called correctly
        mock_extract_yaml.assert_called_once_with('test_data.yaml')
        mock_parse_vars.assert_called_once_with(mock_parse_vars_data)

@pytest.mark.parametrize(
    "mock_get_existing_namespace_return, expected_update_call, expected_insert_call",
    [
        (True, 1, 0),  # Case when namespace exists
        (None, 0, 1)   # Case when namespace does not exist
    ]
)


def test_insert_or_update_status(namespace_allocator, test_data, mock_get_existing_namespace_return, expected_update_call, expected_insert_call):
    """Test insert_or_update_status with different scenarios."""
    kwargs = test_data['insert_or_update_status']
    #expected_kwargs = {**kwargs, 'status': 'ASSIGNED'}

    with patch('namespace_allocator.get_existing_namespace', return_value=mock_get_existing_namespace_return) as mock_get_existing_namespace, \
            patch('namespace_allocator.NamespaceAllocator._update_namespace_status') as mock_update_ns_status, \
            patch('namespace_allocator.NamespaceAllocator._insert_new_status') as mock_insert_new_status, \
            patch('namespace_allocator.NamespaceAllocator._update_namespace_pool_status') as mock_update_ns_pool:
        namespace_allocator.insert_or_update_status(**kwargs)

        mock_get_existing_namespace.assert_called_once()
        assert mock_update_ns_status.call_count == expected_update_call
        assert mock_insert_new_status.call_count == expected_insert_call
        mock_update_ns_pool.assert_called_once()


def test_allocate_namespace_with_existing_namespace(namespace_allocator, test_data):
    """Test allocate_namespace when an existing namespace is assigned."""
    # Extract mock cursor from the fixture
    mock_cursor = namespace_allocator.db_connection.get_cursor.return_value.__enter__.return_value

    kwargs = test_data['allocate_namespace']

    with patch('namespace_allocator.get_assigned_status', return_value=('namespace', 'ASSIGNED', 'namespace_name', 'pipeline_url')) as mock_get_assigned_status, \
            patch('namespace_allocator.find_and_lock_available_namespace', return_value=None) as mock_find_and_lock, \
            patch('namespace_allocator.fetch_total_cpu_requests_with_validation', return_value=1000) as mock_fetch_cpu:
        result = namespace_allocator.allocate_namespace(**kwargs)

        # Ensure get_assigned_status is called with the correct arguments
        mock_get_assigned_status.assert_called_once_with(
            mock_cursor, kwargs['nf_type'], kwargs['release_tag'], kwargs['ats_release_tag'], kwargs['is_csar'],
            kwargs['is_asm'], kwargs['is_tgz'], kwargs['is_internal_ats'], kwargs['is_occ'], kwargs['is_pcf'],
            kwargs['is_converged'], kwargs['upg_rollback'], kwargs['official_build'], kwargs['custom_message']
        )
        mock_find_and_lock.assert_not_called()  # Should not be called since namespace is assigned
        mock_fetch_cpu.assert_not_called()  # Should not be called since namespace is assigned

        # Ensure the correct priority is returned
        assert result == 'pipeline_url', "Expected 'pipeline_url' to be returned when namespace is assigned."



def test_allocate_namespace_no_available_namespace(namespace_allocator, test_data):
    """Test allocate_namespace when no namespace is available."""
    kwargs = test_data['allocate_namespace']

    with patch('namespace_allocator.get_assigned_status', return_value=None) as mock_get_assigned_status, \
            patch('namespace_allocator.find_and_lock_available_namespace', return_value=None) as mock_find_and_lock, \
            patch('namespace_allocator.fetch_total_cpu_requests_with_validation', return_value=1000) as mock_fetch_cpu:
        result = namespace_allocator.allocate_namespace(**kwargs)

        mock_get_assigned_status.assert_called_once()
        mock_find_and_lock.assert_called_once()
        mock_fetch_cpu.assert_not_called()  # Should not be called since no namespace was locked
        assert result is None, "Expected result to be None when no namespace is available."

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

def test_allocate_namespace_exception_handling(namespace_allocator, test_data):
    """Test allocate_namespace handling exceptions during database operations."""
    kwargs = test_data['allocate_namespace']

    with patch('namespace_allocator.get_assigned_status', side_effect=Exception('Database error')) as mock_get_assigned_status:
        with pytest.raises(Exception, match='Database error'):
            namespace_allocator.allocate_namespace(**kwargs)


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
