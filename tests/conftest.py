import pytest
from unittest.mock import patch
from src.server import create_mcp_server, register_tools, get_server_config
from src.tools import ALL_TOOL_CATEGORIES


# pytest fixture to mock the client token validation
@pytest.fixture(scope="session", autouse=True)
def mock_client_token():
    with patch('src.auth.auth_service.AuthService.is_client_token_valid', return_value=True):
        yield


@pytest.fixture
def mcp_server():
    # Get the server configuration
    config = get_server_config()

    server = create_mcp_server(config)
    register_tools(server, ALL_TOOL_CATEGORIES)
    return server
