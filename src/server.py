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
Commvault MCP Server - Main server module for the Model Context Protocol server.

This module sets up and runs the Commvault MCP server with all available tools
for interacting with Commvault product.
"""

import ipaddress
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Callable
from fastmcp import FastMCP
from fastmcp.tools import Tool
from fastmcp.server.auth.oauth_proxy import OAuthProxy
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.auth.jwt_verifier import CustomJWTVerifier
from src.config import ConfigManager, SERVER_NAME, SERVER_INSTRUCTIONS
from src.tools import ALL_TOOL_CATEGORIES
from src.logger import logger
from src.utils import get_env_var


def _is_loopback_host(host: str) -> bool:
    """Return True if host resolves to a loopback address (127.x.x.x or ::1)."""
    if host.lower() in ("localhost", "127.0.0.1", "::1"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Starlette HTTP middleware that enforces token authentication at the MCP
    protocol layer, blocking unauthenticated clients before they can complete
    the MCP handshake, enumerate tools, or read prompts/resources.

    Applied only when MCP_TRANSPORT_MODE is sse/streamable-http AND USE_OAUTH=false.
    """

    def __init__(self, app, auth_service):
        super().__init__(app)
        self._auth_service = auth_service

    async def dispatch(self, request: Request, call_next):
        # Allow .well-known/ discovery endpoints through without auth.
        # MCP clients probe these to discover the auth mechanism; blocking them
        # prevents clients from learning how to authenticate in the first place.
        if request.url.path.startswith("/.well-known/"):
            return await call_next(request)

        is_valid, error_message = self._auth_service.is_client_token_valid(request=request)
        if not is_valid:
            return JSONResponse(
                {"error": error_message or "Unauthorized"},
                status_code=401,
            )
        return await call_next(request)


def create_mcp_server(config) -> FastMCP:
    auth = None
    if config.use_oauth:
        auth = OAuthProxy(
            upstream_authorization_endpoint=config.oauth_authorization_endpoint,
            upstream_token_endpoint=config.oauth_token_endpoint,
            upstream_client_id=config.oauth_client_id,
            upstream_client_secret=config.oauth_client_secret,
            base_url=config.oauth_base_url,
            forward_resource=False,  # Azure AD v2.0 uses scopes instead of RFC 8707 resource param
            token_verifier=CustomJWTVerifier(
                jwks_uri=config.oauth_jwks_uri,
                required_scopes=config.oauth_required_scopes
            )
        )
    return FastMCP(name=SERVER_NAME, instructions=SERVER_INSTRUCTIONS, auth=auth)


def register_tools(mcp_server: FastMCP, tool_categories: List[List[Callable]]) -> None:
    logger.info("Registering tools with MCP server...")
    
    total_tools = 0
    for tool_category in tool_categories:
        for tool_fn in tool_category:
            # only enable docusign tools if ENABLE_DOCUSIGN_TOOLS is true
            if get_env_var("ENABLE_DOCUSIGN_TOOLS", "false").lower() == "false" and "docusign" in tool_fn.__module__:
                continue
            mcp_server.add_tool(Tool.from_function(tool_fn, output_schema=None))
            total_tools += 1
    
    logger.info(f"Successfully registered {total_tools} tools across {len(tool_categories)} categories")


def get_server_config():
    return ConfigManager.load_config()


def run_server() -> None:
    try:
        config = get_server_config()
        
        mcp = create_mcp_server(config)
        register_tools(mcp, ALL_TOOL_CATEGORIES)
        
        logger.info(f"Starting MCP server in {config.transport_mode} mode...")
        
        if config.transport_mode == "stdio":
            mcp.run(transport=config.transport_mode)
        else:
            # Emit a startup warning when binding non-locally without protocol-layer auth
            if not _is_loopback_host(config.host) and not config.use_oauth:
                logger.warning(
                    f"MCP_HOST={config.host!r} is a non-loopback address and USE_OAUTH=false. "
                    "MCP protocol endpoints are protected only by the shared secret key. "
                    "Consider enabling OAuth (USE_OAUTH=true) for production deployments."
                )

            transport_kwargs = {
                "host": config.host,
                "port": config.port,
                "path": config.path,
            }

            # Inject token-auth middleware at the MCP protocol layer when OAuth is not used.
            # This gates every MCP request (initialize, tools/list, tools/call, SSE handshake,
            # prompts/list, resources/list) before any CommServe traffic is generated.
            if not config.use_oauth:
                from src.auth.auth_service import AuthService
                transport_kwargs["middleware"] = [
                    Middleware(TokenAuthMiddleware, auth_service=AuthService())
                ]

            mcp.run(transport=config.transport_mode, **transport_kwargs)
            
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        print(f"ERROR: Failed to start server. Check cv_mcp.log for details.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run_server()