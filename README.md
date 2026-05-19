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
| **User Management** | • List users and user groups<br>• Access security associations |
| **Plan Management** | • View plan configurations and details<br>• Access plan components and settings |
| **Schedule Management** | • Access backup schedules<br>• View schedule configurations<br>• Monitor schedule performance |
| **DocuSign Integration** | • Setup Docusign Vault & Workflow<br>• Backup envelopes to Commvault S3 vault<br>• List & restore DocuSign envelope backups |
| **Salesforce Integration** | • Resolve Salesforce org ID to Commvault client<br>• Browse backed-up Salesforce object records from latest snapshot<br>• Filter records with optional WHERE-clause queries<br>• Paginated access to large record sets |



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

> **Important:** When using traditional token-based authentication, the setup script requires a secure, OS-native keyring backend to store sensitive credentials securely. Only secure backends are allowed for security reasons.

> **Running multiple MCP servers on the same host?** The OS keyring is per-user, so by default every install writes to the same keyring slots and the most recent `setup.py` run overwrites the previous install's credentials. Set a unique `MCP_INSTANCE_ID` per install — see [Running Multiple Instances on the Same Host](#running-multiple-instances-on-the-same-host) under *Additional Configuration*.

#### Supported Secure Backends by Platform

| Platform | Supported Backends | Description |
|----------|-------------------|-------------|
| **Windows** | `WinVaultKeyring`, `WinCredentialStore` | Uses Windows Credential Manager (Windows Vault) for secure storage |
| **macOS** | `macOS Keyring` | Uses the native macOS Keychain for secure storage |
| **Linux** | `SecretService` (GNOME), `KWallet` (KDE) | Uses Freedesktop Secret Service API (GNOME) or KWallet (KDE) for secure storage |
</details>

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/Commvault/commvault-mcp-server.git
cd commvault-mcp-server
```

### 2. Run the Setup Script

The setup script will guide you through configuration options including:
- **Metallic or on-premise deployment** — for Metallic setups, the gateway URL is configured automatically
- Transport mode (stdio, streamable-http, or sse)
- Server connection details (for remote modes)
- Authentication method (traditional tokens or OAuth)
- OAuth configuration (if selected)
- Secure token storage

```bash
uv run setup.py
```

<details>
<summary>Metallic (Cloud) Setup</summary>
<br/>

When prompted **"Is this a Metallic setup?"**, select **yes**. The setup script will automatically configure:

- **Gateway URL:** `https://api.metallic.io`

No additional configuration is needed for the server URL; all other setup steps (transport mode, authentication, etc.) proceed as normal.

</details>

### 3. Start the MCP Server

```bash
uv run -m src.server
```
<details>
<summary>Secure Production Deployment</summary>
<br/>

For production deployments, it is recommended to use a reverse proxy with TLS/HTTPS and security headers. The MCP server should bind to `127.0.0.1` (localhost only) to prevent direct public access.

### Quick Setup

1. **Configure MCP Server for localhost**: Set `MCP_HOST=127.0.0.1` in your `.env` file
2. **Install reverse proxy**: Choose nginx or Caddy
3. **Configure TLS**: Use Let's Encrypt for automatic certificate management
4. **Add security headers**: Configure [OWASP-recommended](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html#security-headers) headers (HSTS, X-Frame-Options, CSP, etc.)
5. **Update client config**: Use `https://your-domain.com/mcp` instead of `http://HOST:PORT/mcp`

</details>

## Running with Docker

The Docker image runs the server in **OAuth mode only**.

> **Prerequisites:** Docker installed, and a Commvault environment running **SP42 CU 27 or above** with OAuth configured.

### 1. Generate a `.env` file

Run the interactive setup script on your host machine. When prompted for the authentication method, select **OAuth**.

```bash
uv run setup.py
```

This produces a `.env` file with all required variables, including the OAuth endpoints and credentials.

### 2. Build the image

```bash
docker build -t commvault-mcp-server .
```

### 3. Run the container

Pass the generated `.env` file to the container at runtime:

```bash
docker run --rm -p 9090:9090 --env-file .env commvault-mcp-server
```

---

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
<summary>Local MCP Server (STDIO) - Unix</summary>

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

<details>
<summary>Local MCP Server (STDIO) - Windows</summary>

```json
{
  "mcpServers": {
    "Commvault": {
      "command": "C:\\YOUR\\PATH\\TO\\commvault-mcp-server\\.venv\\Scripts\\python.exe",
      "args": [
        "C:\\YOUR\\PATH\\TO\\commvault-mcp-server\\src\\server.py"
      ]
    }
  }
}


```
</details>

## Additional Configuration

> **Note:** These are optional configurations that extend the MCP server's capabilities.

<details>
<summary>Running Multiple Instances on the Same Host</summary>
<br/>

The OS keyring used to store the server secret and Commvault API tokens is **per-user**. By default every install of the MCP server on a given host writes to the same keyring entries under the service name `commvault-mcp-server`. If you run two or more MCP server installs under the same OS user, each rerun of `setup.py` silently overwrites the other instances' credentials, which typically surfaces as a `401 Invalid token` after a restart or reboot of the unaffected instance.

To run multiple MCP server instances on the same host under the same OS user, set a unique `MCP_INSTANCE_ID` per install. The setup script prompts for it at the start; you can also set it directly in each install's `.env`:

```bash
# Instance A's .env
MCP_INSTANCE_ID=prod

# Instance B's .env
MCP_INSTANCE_ID=dr
```

With an instance ID set, the keyring service name becomes `commvault-mcp-server:<MCP_INSTANCE_ID>`, isolating each install's `server_secret`, `server_secret_expiry`, `access_token`, and `refresh_token`.

### Rules and notes

- Allowed characters: letters, digits, `.`, `-`, `_` (max 32 chars).
- Leaving `MCP_INSTANCE_ID` unset (or setting it to `default`) preserves the original behaviour and reuses the existing `commvault-mcp-server` keyring entries, so **existing single-instance installs do not need to do anything**.
- Each install should still live in its own directory with its own `.env` so transport mode, port, `CC_SERVER_URL`, etc. don't collide. The instance ID only namespaces the keyring.
- After changing `MCP_INSTANCE_ID` for an existing install, rerun `uv run setup.py` so the secret and tokens are written under the new namespaced service name.
- Alternative isolation strategies that also work: run each instance under a dedicated OS user, or in its own host/VM/container.

</details>

<details>
<summary>Trusted Proxy Configuration</summary>
<br/>

When deployed behind a reverse proxy or load balancer, configure `TRUSTED_PROXY_IPS` environment variable with comma-separated proxy IP addresses to enable per-client rate limiting using the `X-Forwarded-For` header. For example:

```bash
export TRUSTED_PROXY_IPS="10.0.0.1,10.0.0.2,192.168.1.100"
```
</details>

<details>
<summary>Salesforce Backup Integration</summary>
<br/>

The Salesforce integration enables browsing of backed-up Salesforce records stored in Commvault. It provides two tools:

| Tool | Description |
|------|-------------|
| `get_salesforce_client` | Resolves a Salesforce Organisation ID (15- or 18-character) to the corresponding Commvault `clientId` |
| `get_salesforce_records` | Fetches backed-up records for a Salesforce object (e.g. `Account`, `Contact`, `Opportunity`) from the latest backup snapshot |

### Prerequisites

1. **Environment Variable**: Set `ENABLE_SALESFORCE_TOOLS=true` in your environment
2. **Commvault Salesforce Backup**: At least one Salesforce organisation must be configured and backed up in Commvault

### Usage Example

To browse backed-up `Account` records for a Salesforce org:

```
Get all backed-up Account records for Salesforce org 00D2w000005mBCpEAM
```

The tool will automatically resolve the org ID to a Commvault client and return the latest backed-up records.

### Parameters for `get_salesforce_records`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `salesforce_org_id` | Yes | Salesforce Organisation ID (15 or 18 characters) |
| `object_name` | Yes | Salesforce API object name (e.g. `Account`, `Contact`) |
| `limit` | No | Max records to return (default `50`, max `1000`) |
| `offset` | No | Pagination offset (default `0`) |
| `free_query` | No | Optional WHERE-clause filter (e.g. `"Name = 'Acme'"`) |

</details>

<details>
<summary>DocuSign Backup Integration</summary>
<br/>

The DocuSign backup integration enables backup of completed DocuSign envelopes to a Commvault S3 vault. This integration provides comprehensive document management capabilities including backup, listing, and restore operations.

### Prerequisites

1. **Environment Variable**: Set `ENABLE_DOCUSIGN_TOOLS=true` in your environment
2. **Commvault S3 Vault**: Configure an S3 endpoint in Commvault
   - Learn more: [Getting Started with S3 Vault](https://documentation.commvault.com/11.42/software/getting_started_with_s3_vault.html)
3. **DocuSign API Access**: Valid DocuSign integration credentials

### Configuration Files

Create the following files in the `config/` directory:

#### 1. DocuSign Configuration (`docusign_config.json`)

Based on the template file `config/docusign_config_template.json`:

```json
{
  "docusign": {
    "integrationKey": "YOUR_INTEGRATION_KEY_HERE",
    "userId": "YOUR_USER_ID_HERE", 
    "authServer": "account-d.docusign.com",
    "scopes": "signature impersonation",
    "basePath": "https://demo.docusign.net/restapi"
  },
  "fromDate": "2024-07-01T00:00:00Z"
}
```

#### 2. DocuSign Private Key (`docusign_key.pem`)

Place your DocuSign private key file in the `config/` directory as `docusign_key.pem`.

</details>

## Note on LiteLLM

We do not recommend using [LiteLLM](https://github.com/BerriAI/litellm) as an MCP client. LiteLLM's per-turn session lifecycle triggers cosmetic `ClosedResourceError` tracebacks in the server terminal due to a race condition in the upstream `mcp` Python SDK — these are harmless but noisy.

## Contributing

- We're continuing to add more functionality to this MCP server. If you'd like to leave feedback, file a bug or provide a feature request, please open an issue on this repository.
- Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the Apache License. See the [LICENSE](./LICENSE) file for details.
