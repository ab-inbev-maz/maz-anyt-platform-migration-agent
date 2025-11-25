"""
Unit tests for Read_Manifest_and_Check_API tool.
"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from brewbridge.core.state import MigrationGraphState
# Import using importlib due to numeric module name
import importlib.util
from pathlib import Path

_extractor_path = Path(__file__).parent.parent.parent / "src" / "brewbridge" / "domain" / "tools" / "extractor" / "3.0" / "read_manifest_and_check_api.py"
spec = importlib.util.spec_from_file_location("read_manifest_and_check_api", _extractor_path)
_read_manifest_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_read_manifest_module)

read_manifest_and_check_api = _read_manifest_module.read_manifest_and_check_api
_merge_credentials = _read_manifest_module._merge_credentials
_collect_env_credentials = _read_manifest_module._collect_env_credentials
_ping_github = _read_manifest_module._ping_github
_ping_adf = _read_manifest_module._ping_adf
_ping_llm_apis = _read_manifest_module._ping_llm_apis


class TestMergeCredentials:
    """Test credential merging logic."""
    
    def test_env_vars_override_manifest(self):
        env_creds = {"GITHUB_TOKEN": "env_token"}
        manifest_creds = {"GITHUB_TOKEN": "manifest_token", "ADF_TENANT_ID": "tenant"}
        
        result = _merge_credentials(env_creds, manifest_creds)
        
        assert result["GITHUB_TOKEN"] == "env_token"  # Env overrides
        assert result["ADF_TENANT_ID"] == "tenant"  # Manifest kept


class TestReadManifestAndCheckAPI:
    """Test the main read_manifest_and_check_api tool."""
    
    @pytest.fixture
    def sample_manifest_path(self, tmp_path):
        """Create a sample manifest.yaml file."""
        manifest_file = tmp_path / "manifest.yaml"
        manifest_file.write_text("""
pipelines_to_migrate:
  - name: test_pipeline_1
    repo: BrewDat/test-repo
    path: pipelines/pipeline1.json

credentials:
  GITHUB_TOKEN: "test_token_from_manifest"

region: "us-east-1"
environment: "dev"
repo_name: "BrewDat/source-repo"
target_repo_name: "BrewDat/target-repo"
""")
        return str(manifest_file)
    
    @patch.object(_read_manifest_module, '_ping_github')
    @patch.object(_read_manifest_module, '_ping_adf')
    @patch.object(_read_manifest_module, '_ping_llm_apis')
    def test_read_manifest_success(
        self, 
        mock_ping_llm, 
        mock_ping_adf, 
        mock_ping_github,
        sample_manifest_path
    ):
        """Test successful manifest reading and API checks."""
        # Setup mocks
        mock_ping_github.return_value = True
        mock_ping_adf.return_value = True
        mock_ping_llm.return_value = True
        
        # Create initial state
        initial_state = MigrationGraphState(manifest_path=sample_manifest_path)
        
        # Run the tool
        result_state = read_manifest_and_check_api(initial_state)
        
        # Assertions
        assert result_state.manifest_path == sample_manifest_path
        assert len(result_state.pipelines_to_migrate) == 1
        assert result_state.pipelines_to_migrate[0]["name"] == "test_pipeline_1"
        assert result_state.credentials is not None
        assert result_state.api_connectivity_ok is True
        
        # Verify API pings were called
        mock_ping_github.assert_called_once()
        mock_ping_llm.assert_called_once()
    
    def test_missing_manifest_path(self):
        """Test error when manifest_path is missing."""
        initial_state = MigrationGraphState(manifest_path=None)
        
        with pytest.raises(Exception):  # Should raise ManifestNotFoundError
            read_manifest_and_check_api(initial_state)
    
    def test_invalid_manifest_path(self):
        """Test error when manifest file doesn't exist."""
        initial_state = MigrationGraphState(manifest_path="/nonexistent/manifest.yaml")
        
        with pytest.raises(Exception):  # Should raise ManifestNotFoundError
            read_manifest_and_check_api(initial_state)


class TestPingFunctions:
    """Test individual ping functions."""
    
    @patch.object(_read_manifest_module, 'GitHubClient')
    def test_ping_github_success(self, mock_github_client_class):
        """Test successful GitHub ping."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_github_client_class.return_value = mock_client
        
        credentials = {"GITHUB_TOKEN": "test_token"}
        result = _ping_github(credentials)
        
        assert result is True
        mock_github_client_class.assert_called_once_with(token="test_token")
    
    @patch.object(_read_manifest_module, 'GitHubClient')
    def test_ping_github_failure(self, mock_github_client_class):
        """Test GitHub ping failure."""
        mock_client = Mock()
        mock_client.ping.return_value = False
        mock_github_client_class.return_value = mock_client
        
        credentials = {"GITHUB_TOKEN": "test_token"}
        result = _ping_github(credentials)
        
        assert result is False
    
    def test_ping_github_no_token(self):
        """Test GitHub ping when token is missing."""
        credentials = {}
        result = _ping_github(credentials)
        
        assert result is False
    
    @patch.object(_read_manifest_module, 'ADFClient')
    def test_ping_adf_success(self, mock_adf_client_class):
        """Test successful ADF ping."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_adf_client_class.return_value = mock_client
        
        credentials = {
            "ADF_TENANT_ID": "tenant",
            "ADF_CLIENT_ID": "client",
            "ADF_CLIENT_SECRET": "secret"
        }
        result = _ping_adf(credentials)
        
        assert result is True
    
    def test_ping_adf_incomplete_credentials(self):
        """Test ADF ping with incomplete credentials."""
        credentials = {"ADF_TENANT_ID": "tenant"}  # Missing client_id and secret
        result = _ping_adf(credentials)
        
        assert result is False

