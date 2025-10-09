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

import sys
from typing import List, Callable

from fastmcp import FastMCP
from fastmcp.tools import Tool
from fastmcp.server.auth.oauth_proxy import OAuthProxy

from src.auth.jwt_verifier import CustomJWTVerifier
from src.config import ConfigManager, SERVER_NAME, SERVER_INSTRUCTIONS
from src.tools import ALL_TOOL_CATEGORIES
from src.logger import logger


def create_mcp_server(config) -> FastMCP:
    auth = None
    if config.use_oauth:
        auth = OAuthProxy(
            upstream_authorization_endpoint=config.oauth_authorization_endpoint,
            upstream_token_endpoint=config.oauth_token_endpoint,
            upstream_client_id=config.oauth_client_id,
            upstream_client_secret=config.oauth_client_secret,
            base_url=config.oauth_base_url,
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
            mcp.run(
                transport=config.transport_mode,
                host=config.host,
                port=config.port,
                path=config.path
            )
            
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_server()