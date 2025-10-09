MCP Server Test Suite

Basic test suite for CommVault MCP server tools.

Prerequisites

Python 3.11+

uv package manager

CommVault credentials configured in system keyring

Running Tests
All Tests
uv run pytest tests

Individual Test File
uv run pytest tests/test_client_tools.py

Specific Test Function
uv run pytest tests/test_client_tools.py::test_get_client_list

Useful Options
-v   # verbose output
-s   # show print statements
-x   # stop on first failure

