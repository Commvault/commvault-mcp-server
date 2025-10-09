# Commvault MCP Server

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
[![License](https://img.shields.io/badge/License-Apache_2.0-red.svg)](https://opensource.org/licenses/Apache-2.0)

A [Model Context Protocol (MCP)](https://modelcontextprotocol.org/) server for seamless integration with **Commvault** environments. This server enables AI agents to securely access and manage job details, commcell metrics, client and storage information, user permissions, plan configurations, and backup schedules.


## Features

The Commvault MCP Server enables seamless integration with Commvault environments, offering the following:

| Category | Features |
|----------|----------|
| **Job Management** | • View job details and history<br>• Control jobs (suspend, resume, resubmit, kill)<br>• Monitor job status and performance |
| **Commcell Management** | • Retrieve SLA status and compliance<br>• View security posture and scores<br>• Access storage space utilization metrics<br>• Get commcell details and entity counts |
| **Client Management** | • Access client groups and client information<br>• Manage subclients and client properties<br>• View client associations |
| **Storage Management** | • View storage policies and configurations<br>• Access storage pool information<br>• Monitor storage resources |
| **User Management** | • List users and user groups<br>• Access security associations<br>• Manage user permissions |
| **Plan Management** | • View plan configurations and details<br>• Access plan components and settings |
| **Schedule Management** | • Access backup schedules<br>• View schedule configurations<br>• Monitor schedule performance |


## Prerequisites

Before running the Commvault MCP Server, ensure the following requirements are met:

### 1. Python Environment

* Python 3.11 or higher
* [`uv`](https://github.com/astral-sh/uv) package manager (used for dependency management and running the server)

### 2. Authentication & Security Configuration

The Commvault MCP Server supports two authentication methods:

<details>
<summary>Option 1: OAuth Authentication</summary>
<br/>

> **Note:** OAuth authentication is only supported for Commvault environments running **SP42 CU 27 and above**.
> OAuth must be properly configured in the CommServe before using this option.

When using OAuth authentication, you'll need:

* **Discovery Endpoint URL:** The OAuth discovery/metadata endpoint
* **Client ID:** Your OAuth application's client identifier
* **Client Secret:** Your OAuth application's client secret
* **Required Scopes:** Required OAuth scopes
* **Base URL:** Base URL of the MCP Server

> **Important:** The redirect URI must be set to `OAUTH_BASE_URL/auth/callback` in your OAuth provider's app/client configuration.
</details>

<details>
<summary>Option 2: Traditional Token-Based Authentication</summary>
<br/>

The following values will be collected during the setup process:

* **Commvault Access Credentials:**
  You need a valid `access_token` and `refresh_token` to authenticate with the Commvault API.
  Learn how to generate these tokens here: [Creating an Access Token – Commvault Docs](https://documentation.commvault.com/11.38/expert/creating_access_token.html)
  
* **Secret Key:**
  This secret must be included by the **MCP Client** in the `Authorization` header of all tool requests.
  It acts as a security layer for tool access in remote server. You can set your own.
</details>

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/chandansgowda-cv/commvault-mcp-server.git
cd commvault-mcp-server
```

### 2. Run the Setup Script

The setup script will guide you through configuration options including:
- Transport mode (stdio, streamable-http, or sse)
- Server connection details (for remote modes)
- Authentication method (traditional tokens or OAuth)
- OAuth configuration (if selected)
- Secure token storage

```bash
uv run setup.py
```

### 3. Start the MCP Server

```bash
uv run -m src.server
```

## Configuring Clients

> **Note:** `npx` is required while using Token-Based Authentication. You can get it by installing [Node.js](https://nodejs.org/) which includes `npx` by default.

<details>
<summary>While using OAuth</summary>
<br/>

Refer to your AI client’s documentation for integration steps. For example, Claude requires specifying a server name and the MCP server URL in its connector configuration.

</details>

<details>
<summary>Remote MCP Server (Streamable HTTP / SSE)</summary>

```json
{
  "mcpServers": {
    "Commvault": {
      "command": "npx",
      "args": ["mcp-remote", "HOST:PORT/mcp", "--header", "Authorization: <secret stored in server keyring>"]
    }
  }
}

```
</details>

<details>
<summary>Remote MCP Server (Client on Windows)</summary>

```json
{
  "mcpServers": {
    "Commvault": {
      "command": "cmd",
      "args": ["/c", "npx", "mcp-remote", "HOST:PORT/mcp", "--header", "Authorization: <secret stored in server keyring>"]
    }
  }
}

```
</details>

<details>
<summary>Remote MCP Server (HTTP)</summary>

```json
{
  "mcpServers": {
    "Commvault": {
      "command": "npx",
      "args": ["mcp-remote", "HOST:PORT/mcp", "--header", "Authorization: <secret stored in server keyring>", "--allow-http"]
    }
  }
}

```
</details>

<details>
<summary>Local MCP Server (STDIO)</summary>

```json
{
  "mcpServers": {
    "Commvault": {
      "command": "C:\\YOUR\\PATH\\TO\\commvault-mcp-server\\.venv\\bin\\python",
      "args": [
        "C:\\YOUR\\PATH\\TO\\commvault-mcp-server\\src\\server.py"
      ]
    }
  }
}


```
</details>

## Contributing

- We're continuing to add more functionality to this MCP server. If you'd like to leave feedback, file a bug or provide a feature request, please open an issue on this repository.
- Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the Apache License. See the [LICENSE](./LICENSE) file for details.
