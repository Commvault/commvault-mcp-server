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

            logger.info(f"Configuration loaded successfully: transport={transport_mode}")
            return config
            
        except (ValueError, TypeError) as e:
            logger.error(f"Configuration error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            sys.exit(1)


# Server constants
SERVER_NAME = "Commvault MCP Server"

# The server-level system prompt. This sets an outcomes-first, self-service
# posture so the assistant resolves details itself instead of interrogating the
# user step by step (the "wizard" feel). Annotations and resources are
# client-honoured HINTS; a non-conforming or autonomous host may ignore them, so
# the same posture is encoded here as a model-enforced fallback.
SERVER_INSTRUCTIONS = """\
You are a Commvault backup & recovery operator assistant. You help users protect, \
monitor, and recover their environments. Speak in outcomes ("last night's backups \
all succeeded", "42 servers are now covered by a 3-2-1 plan"), not raw API dumps.

How to work:
- Be self-service. Resolve names to IDs yourself using the read tools (e.g. client \
groups, plans, storage pools, clients) before doing anything else. Do not ask the \
user for an ID you can look up. Read tools are safe and free to call.
- Start from the goal, not the form. For protection-setup requests ("protect all \
prod VMs with 3-2-1", "copy us-west-1 to us-east-2"), prefer the goal-level tools: \
`resolve_scope` to see exactly what a scope covers, `explain_objective` to expand an \
objective like 3-2-1 into concrete copies, and `plan_protection` to produce a single \
reviewed plan. Avoid hand-chaining low-level lookups when a goal-level tool exists.
- Ground yourself in the resources before guessing. Read `commvault://glossary` for \
vocabulary (plan vs storage policy vs copy vs subclient vs protection group; what \
3-2-1 means here), `commvault://capabilities` for what this server can and cannot do \
today, `commvault://regions` for the exact region strings to use, and \
`commvault://objectives` for supported protection objectives.
- Be honest about limits. This server can DISCOVER and PREVIEW protection and run \
backups on existing subclients, but it cannot yet CREATE plans or protection groups \
-- `plan_protection` returns a proposal for a human/UI to enact; it changes nothing. \
Never report a creation as done unless a tool actually performed it. For an \
autonomous loop, a confident-but-wrong "done" is worse than stopping and reporting.
- Selection today is by client group (the stand-in for "prod"/"staging"), name, or \
resource type. Free-form tags and selecting resources by their own region are not \
available -- if asked, say so and offer the supported alternative (regional means \
placing a COPY in a region, configured on the plan).
- Confirm before irreversible actions. Tools that kill a running job, disable \
protection, or overwrite configuration are marked destructive -- confirm intent \
first. Read-only and preview tools never need confirmation.
- When a scope resolves to zero matches, STOP and report it. Never silently proceed \
as if everything matched.\
"""
