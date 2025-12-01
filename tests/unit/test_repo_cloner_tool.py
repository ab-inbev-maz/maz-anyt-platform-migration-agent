"""
Unit tests for Repo_Cloner_Tool.
"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from brewbridge.core.state import MigrationGraphState
from brewbridge.domain.tools.repo_cloner_tool import (
    repo_cloner_tool,
    _detect_required_frameworks,
    _get_repo_url,
    _clone_or_pull_repo,
)
from brewbridge.utils.exceptions import RepositoryCloneError


class TestDetectRequiredFrameworks:
    """Test framework detection logic."""
    
    def test_detect_brewtiful_from_gld(self):
        """Test detection of brewtiful from 'gld' in pipeline name."""
        pipelines = [
            {"name": "gld_maz_sales_pipeline"},
            {"name": "test_pipeline_gld"},
        ]
        result = _detect_required_frameworks(pipelines)
        assert "brewtiful" in result
        assert "hopsflow" not in result
    
    def test_detect_hopsflow_from_brz(self):
        """Test detection of hopsflow from 'brz' in pipeline name."""
        pipelines = [
            {"name": "brz_maz_logistics_pipeline"},
            {"name": "test_brz_pipeline"},
        ]
        result = _detect_required_frameworks(pipelines)
        assert "hopsflow" in result
        assert "brewtiful" not in result
    
    def test_detect_hopsflow_from_slv(self):
        """Test detection of hopsflow from 'slv' in pipeline name."""
        pipelines = [
            {"name": "slv_maz_sales_pipeline"},
            {"name": "test_slv_pipeline"},
        ]
        result = _detect_required_frameworks(pipelines)
        assert "hopsflow" in result
        assert "brewtiful" not in result
    
    def test_detect_both_frameworks(self):
        """Test detection of both frameworks when both patterns exist."""
        pipelines = [
            {"name": "gld_maz_sales_pipeline"},
            {"name": "brz_maz_logistics_pipeline"},
        ]
        result = _detect_required_frameworks(pipelines)
        assert "brewtiful" in result
        assert "hopsflow" in result
    
    def test_case_insensitive_detection(self):
        """Test that detection is case-insensitive."""
        pipelines = [
            {"name": "GLD_MAZ_PIPELINE"},
            {"name": "BRZ_MAZ_PIPELINE"},
        ]
        result = _detect_required_frameworks(pipelines)
        assert "brewtiful" in result
        assert "hopsflow" in result
    
    def test_no_frameworks_detected(self):
        """Test when no framework patterns are found."""
        pipelines = [
            {"name": "test_pipeline"},
            {"name": "another_pipeline"},
        ]
        result = _detect_required_frameworks(pipelines)
        assert len(result) == 0
    
    def test_empty_pipelines_list(self):
        """Test with empty pipelines list."""
        result = _detect_required_frameworks([])
        assert len(result) == 0
    
    def test_pipeline_without_name(self):
        """Test handling of pipeline without name field."""
        pipelines = [{"repo": "test/repo"}]
        result = _detect_required_frameworks(pipelines)
        assert len(result) == 0


class TestGetRepoUrl:
    """Test repository URL retrieval."""
    
    def test_get_brewtiful_url(self):
        """Test getting brewtiful repository URL."""
        url = _get_repo_url("brewtiful")
        assert url == "https://github.com/BrewDat/brewtiful.git"
    
    def test_get_hopsflow_url(self):
        """Test getting hopsflow repository URL."""
        url = _get_repo_url("hopsflow")
        assert url == "https://github.com/BrewDat/brewdat-pltfrm-ghq-tech-hopsflow.git"
    
    def test_unknown_repo_raises_error(self):
        """Test that unknown repository raises ValueError."""
        with pytest.raises(ValueError, match="Unknown framework repository"):
            _get_repo_url("unknown_repo")


class TestCloneOrPullRepo:
    """Test clone/pull repository logic."""
    
    @patch('brewbridge.domain.tools.repo_cloner_tool.Repo')
    def test_clone_new_repository(self, mock_repo_class, tmp_path):
        """Test cloning a new repository."""
        destination = tmp_path / "test_repo"
        github_token = "test_token_123"
        
        # Mock Repo.clone_from
        mock_repo_class.clone_from = Mock()
        
        _clone_or_pull_repo("brewtiful", destination, github_token)
        
        # Verify clone_from was called with authenticated URL
        mock_repo_class.clone_from.assert_called_once()
        call_args = mock_repo_class.clone_from.call_args
        assert "test_token_123@github.com" in call_args[0][0]  # Authenticated URL
        assert str(destination) == call_args[0][1]  # Destination path
    
    @patch('brewbridge.domain.tools.repo_cloner_tool.Repo')
    def test_pull_existing_repository(self, mock_repo_class, tmp_path):
        """Test pulling updates for existing repository."""
        destination = tmp_path / "test_repo"
        destination.mkdir()
        (destination / ".git").mkdir()  # Simulate existing git repo
        
        github_token = "test_token_123"
        
        # Mock Repo instance and remotes
        mock_repo = Mock()
        mock_origin = Mock()
        mock_repo.remotes.origin = mock_origin
        mock_repo_class.return_value = mock_repo
        
        _clone_or_pull_repo("brewtiful", destination, github_token)
        
        # Verify Repo was initialized with existing path
        mock_repo_class.assert_called_once_with(destination)
        # Verify fetch and pull were called
        mock_origin.fetch.assert_called_once()
        mock_origin.pull.assert_called_once()
    
    @patch('brewbridge.domain.tools.repo_cloner_tool.Repo')
    def test_clone_raises_git_command_error(self, mock_repo_class, tmp_path):
        """Test handling of GitCommandError during clone."""
        destination = tmp_path / "test_repo"
        github_token = "test_token_123"
        
        from git.exc import GitCommandError
        
        # Mock clone_from to raise GitCommandError
        mock_repo_class.clone_from = Mock(side_effect=GitCommandError("clone", "error"))
        
        with pytest.raises(RepositoryCloneError):
            _clone_or_pull_repo("brewtiful", destination, github_token)
    
    @patch('brewbridge.domain.tools.repo_cloner_tool.Repo')
    def test_pull_raises_git_error(self, mock_repo_class, tmp_path):
        """Test handling of GitError during pull."""
        destination = tmp_path / "test_repo"
        destination.mkdir()
        (destination / ".git").mkdir()
        
        github_token = "test_token_123"
        
        from git.exc import GitError
        
        # Mock Repo to raise GitError
        mock_repo_class.side_effect = GitError("pull error")
        
        with pytest.raises(RepositoryCloneError):
            _clone_or_pull_repo("brewtiful", destination, github_token)


class TestRepoClonerTool:
    """Test the main repo_cloner_tool function."""
    
    @pytest.fixture
    def sample_state(self):
        """Create a sample state with pipelines and credentials."""
        return MigrationGraphState(
            pipelines_to_migrate=[
                {"name": "gld_maz_sales_pipeline"},
                {"name": "brz_maz_logistics_pipeline"},
            ],
            credentials={"GITHUB_TOKEN": "test_token_123"}
        )
    
    @patch('brewbridge.domain.tools.repo_cloner_tool._clone_or_pull_repo')
    def test_clone_both_frameworks(self, mock_clone_or_pull, sample_state, tmp_path):
        """Test cloning both frameworks when both are detected."""
        # Mock Path to return tmp_path for cache directory
        with patch('brewbridge.domain.tools.repo_cloner_tool.Path') as mock_path:
            mock_cache_path = tmp_path / "cache"
            mock_cache_path.mkdir()
            
            # Mock Path constructor to return our test paths
            def path_side_effect(path_str):
                if path_str.startswith("cache/"):
                    return mock_cache_path / path_str.replace("cache/", "")
                return Path(path_str)
            
            mock_path.side_effect = path_side_effect
            
            result_state = repo_cloner_tool(sample_state)
            
            # Verify both repositories were cloned
            assert mock_clone_or_pull.call_count == 2
            assert result_state.repos_cloned == ["brewtiful", "hopsflow"]
    
    @patch('brewbridge.domain.tools.repo_cloner_tool._clone_or_pull_repo')
    def test_clone_only_brewtiful(self, mock_clone_or_pull, tmp_path):
        """Test cloning only brewtiful when only gld pipelines exist."""
        state = MigrationGraphState(
            pipelines_to_migrate=[
                {"name": "gld_maz_sales_pipeline"},
            ],
            credentials={"GITHUB_TOKEN": "test_token_123"}
        )
        
        with patch('brewbridge.domain.tools.repo_cloner_tool.Path') as mock_path:
            mock_cache_path = tmp_path / "cache"
            mock_cache_path.mkdir()
            
            def path_side_effect(path_str):
                if path_str.startswith("cache/"):
                    return mock_cache_path / path_str.replace("cache/", "")
                return Path(path_str)
            
            mock_path.side_effect = path_side_effect
            
            result_state = repo_cloner_tool(state)
            
            # Verify only brewtiful was cloned
            assert mock_clone_or_pull.call_count == 1
            assert result_state.repos_cloned == ["brewtiful"]
    
    @patch('brewbridge.domain.tools.repo_cloner_tool._clone_or_pull_repo')
    def test_clone_only_hopsflow(self, mock_clone_or_pull, tmp_path):
        """Test cloning only hopsflow when only brz/slv pipelines exist."""
        state = MigrationGraphState(
            pipelines_to_migrate=[
                {"name": "brz_maz_logistics_pipeline"},
            ],
            credentials={"GITHUB_TOKEN": "test_token_123"}
        )
        
        with patch('brewbridge.domain.tools.repo_cloner_tool.Path') as mock_path:
            mock_cache_path = tmp_path / "cache"
            mock_cache_path.mkdir()
            
            def path_side_effect(path_str):
                if path_str.startswith("cache/"):
                    return mock_cache_path / path_str.replace("cache/", "")
                return Path(path_str)
            
            mock_path.side_effect = path_side_effect
            
            result_state = repo_cloner_tool(state)
            
            # Verify only hopsflow was cloned
            assert mock_clone_or_pull.call_count == 1
            assert result_state.repos_cloned == ["hopsflow"]
    
    def test_missing_github_token(self):
        """Test error when GITHUB_TOKEN is missing."""
        state = MigrationGraphState(
            pipelines_to_migrate=[{"name": "gld_test_pipeline"}],
            credentials={}  # No token
        )
        
        with pytest.raises(RepositoryCloneError, match="GITHUB_TOKEN not found"):
            repo_cloner_tool(state)
    
    def test_no_pipelines(self):
        """Test behavior when no pipelines are provided."""
        state = MigrationGraphState(
            pipelines_to_migrate=[],
            credentials={"GITHUB_TOKEN": "test_token_123"}
        )
        
        result_state = repo_cloner_tool(state)
        
        assert result_state.repos_cloned == []
    
    def test_no_frameworks_detected(self):
        """Test behavior when no frameworks are detected from pipeline names."""
        state = MigrationGraphState(
            pipelines_to_migrate=[
                {"name": "test_pipeline_without_framework"},
            ],
            credentials={"GITHUB_TOKEN": "test_token_123"}
        )
        
        result_state = repo_cloner_tool(state)
        
        assert result_state.repos_cloned == []
    
    @patch('brewbridge.domain.tools.repo_cloner_tool._clone_or_pull_repo')
    def test_clone_error_propagates(self, mock_clone_or_pull, tmp_path):
        """Test that clone errors are properly propagated."""
        state = MigrationGraphState(
            pipelines_to_migrate=[{"name": "gld_test_pipeline"}],
            credentials={"GITHUB_TOKEN": "test_token_123"}
        )
        
        # Mock clone to raise RepositoryCloneError
        mock_clone_or_pull.side_effect = RepositoryCloneError("Clone failed")
        
        with patch('brewbridge.domain.tools.repo_cloner_tool.Path') as mock_path:
            mock_cache_path = tmp_path / "cache"
            mock_cache_path.mkdir()
            
            def path_side_effect(path_str):
                if path_str.startswith("cache/"):
                    return mock_cache_path / path_str.replace("cache/", "")
                return Path(path_str)
            
            mock_path.side_effect = path_side_effect
            
            with pytest.raises(RepositoryCloneError, match="Clone failed"):
                repo_cloner_tool(state)


