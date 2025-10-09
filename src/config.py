# --------------------------------------------------------------------------
# Copyright Commvault Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------

"""
Configuration module for Commvault MCP Server.

This module handles all configuration-related functionality including
environment variable management and server settings.
"""

import sys
from dataclasses import dataclass
from typing import List, Optional

from src.logger import logger
from src.utils import get_env_var


@dataclass
class ServerConfig:
    transport_mode: str
    host: Optional[str] = None
    port: Optional[int] = None
    path: Optional[str] = None
    use_oauth: bool = False
    oauth_authorization_endpoint: Optional[str] = None
    oauth_token_endpoint: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_jwks_uri: Optional[str] = None
    oauth_required_scopes: Optional[List[str]] = None
    oauth_base_url: Optional[str] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.transport_mode not in ["stdio", "streamable-http", "sse"]:
            raise ValueError(f"Invalid transport mode: {self.transport_mode}")
        
        if self.transport_mode != "stdio":
            if not all([self.host, self.port, self.path]):
                raise ValueError("Host, port, and path are required for non-stdio transport modes")

        if self.use_oauth:
            if not all([self.oauth_authorization_endpoint, self.oauth_token_endpoint, self.oauth_client_id, self.oauth_client_secret, self.oauth_jwks_uri]):
                raise ValueError("All OAuth-related fields must be set for custom OAuth provider")


class ConfigManager:
    @staticmethod
    def load_config() -> ServerConfig:
        """
        Load server configuration from environment variables.
        """
        try:
            transport_mode = get_env_var("MCP_TRANSPORT_MODE")
            
            # Only load network config for non-stdio modes
            if transport_mode != "stdio":
                host = get_env_var("MCP_HOST")
                port = int(get_env_var("MCP_PORT"))
                path = get_env_var("MCP_PATH")
                use_oauth = get_env_var("USE_OAUTH", "false").lower() == "true"
                if use_oauth:
                    config = ServerConfig(
                        transport_mode=transport_mode,
                        host=host,
                        port=port,
                        path=path,
                        use_oauth=True,
                        oauth_authorization_endpoint=get_env_var("OAUTH_AUTHORIZATION_ENDPOINT"),
                        oauth_token_endpoint=get_env_var("OAUTH_TOKEN_ENDPOINT"),
                        oauth_client_id=get_env_var("OAUTH_CLIENT_ID"),
                        oauth_client_secret=get_env_var("OAUTH_CLIENT_SECRET"),
                        oauth_jwks_uri=get_env_var("OAUTH_JWKS_URI"),
                        oauth_required_scopes=get_env_var("OAUTH_REQUIRED_SCOPES").split(","),
                        oauth_base_url=get_env_var("OAUTH_BASE_URL")
                    )
                else:
                    config = ServerConfig(
                        transport_mode=transport_mode,
                        host=host,
                        port=port,
                        path=path
                    )
            else:
                config = ServerConfig(transport_mode=transport_mode)

            logger.info(f"Configuration loaded successfully: transport={transport_mode} oauth={use_oauth}")
            return config
            
        except (ValueError, TypeError) as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            sys.exit(1)


# Server constants
SERVER_NAME = "Commvault MCP Server"
SERVER_INSTRUCTIONS = "You can use this server to interact with Commvault Product"
