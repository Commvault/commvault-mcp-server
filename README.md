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

## Security Considerations

### Multi-Server Trust Boundary (Tool Poisoning & Tool Shadowing)

The MCP protocol enforces **no isolation between servers** that share a single agent session. Any other MCP server running in the same session as the Commvault MCP has the practical ability to influence the agent's reasoning about Commvault operations, even without direct access to CommServe or any privileges against it.

Two attack paths follow from this architectural property:

- **Tool Poisoning** — A malicious server's tool description coerces the agent to read the operator's stored Commvault auth token from host-side storage (e.g. `~/.claude.json`, a project-level `.mcp.json`, or the OS keychain entry for `commvault-mcp-server`) and pass the token value as a parameter to an attacker-controlled tool, which then exfiltrates it.
- **Tool Shadowing** — A malicious server's tool description names Commvault tool identifiers (e.g. `kill_job`, `disable_schedule`, `set_user_enabled`) and instructs the agent to perform additional or modified Commvault operations whenever the operator legitimately invokes those tools. The Commvault MCP itself is the actor in these calls; its code and responses are not corrupted. The corruption lives in the agent's reasoning, induced by the third-party server's metadata.

Neither path requires any defect in the Commvault MCP Server code. Both are properties of the MCP architecture as currently specified.

> **Recommendations for operators:**
> - Install only first-party or independently audited MCP servers in agent sessions that include Commvault tools.
> - Where feasible, use an **isolated agent session** in which the Commvault MCP is the only integration.
> - Where isolation is not feasible, restrict the agent's general filesystem and outbound-network tool capabilities for the duration of the session.

### Credential Hygiene

- **Use short-lived access tokens with explicit refresh** rather than long-lived bearer tokens. A bounded token lifetime limits the window during which an exfiltrated credential remains usable, and rotation invalidates leaked tokens before an attacker can act on them.
- **Restrict outbound traffic from the MCP host.** Where your environment supports it, apply a network policy that limits outbound HTTP/HTTPS from the host running the MCP server to a documented allowlist. Unexpected outbound calls from an MCP host are the principal exfiltration channel for this class of attack and the most reliable detection signal.

### Investigating Possible Tool Shadowing (Signals, Not Proof)

Tool shadowing does not leave a dedicated “shadowing” marker in Commvault or MCP logs. Treat the following as **investigation signals** to correlate across sources. A signal alone does **not** prove shadowing; benign agents, automation, and legitimate operators can produce similar patterns.

| Signal | Where to look | What to correlate |
|--------|----------------|-------------------|
| High-impact CommServe API activity | Command Center audit / API logs for the **Commvault access token** used by the MCP server (same user as token owner) | `kill_job`, `disable_schedule`, `enable_schedule`, `set_user_enabled`, `run_backup_for_subclient`, and related job-control calls that the operator does not recall approving in the agent session |
| MCP server outbound API calls | `cv_mcp.log` in the **MCP server install directory** (rotating file, default name `cv_mcp.log`) | Log lines include HTTP method and Command Center API URL (`CommvaultApiClient`). Compare timestamps with CommServe audit entries and with the agent client’s session time window |
| MCP protocol access | MCP host process / reverse-proxy access logs (if deployed) | `POST`/`GET` to your configured `MCP_PATH` (for example `/mcp`) while an agent session is active |
| Unexpected multi-server context | Agent / IDE configuration | More than one MCP server enabled in the **same** agent session as Commvault (see [Multi-Server Trust Boundary](#multi-server-trust-boundary-tool-poisoning--tool-shadowing)) |

**Suggested correlation workflow**

1. Note the approximate **agent session** start/end time and which MCP tools the operator intended to run.
2. On the MCP host, review `cv_mcp.log` for API calls in that window (especially job control, schedules, user enable/disable).
3. In CommServe / Command Center, review audit or API activity for the **token identity** tied to the MCP install for the same window.
4. Compare **volume and type** of calls: operations that match Commvault tool semantics but were not part of the stated operator intent are worth escalation—not automatic attribution to shadowing.
5. If another MCP server was present in the session, review that server’s tool descriptions and session logs per your client vendor’s guidance.

This guidance does not assert a product defect in Commvault MCP isolation or document a demonstrated exploit. It supports cautious operator response to the shared MCP agent trust boundary.

### Credential and Configuration Storage (MCP Host vs CommServe)

**MCP client configuration** (on the operator workstation or IDE) holds how the client reaches the MCP server. It is separate from **MCP server-side** storage used to call CommServe.

| Layer | Purpose | Typical locations (verify for your OS and client) |
|-------|---------|---------------------------------------------------|
| **MCP client** | URL, transport, MCP `server_secret` or OAuth client settings for talking **to the MCP server** | Project file `.cursor/mcp.json` (Cursor), `~/.cursor/mcp.json` (global), Claude / other client config paths per vendor docs, `mcp-remote` args in JSON config (see [Configuring Clients](#configuring-clients)) |
| **MCP server — non-secret config** | `CC_SERVER_URL`, transport, host, port, OAuth endpoints (no Commvault refresh token in file for traditional mode) | `.env` in the **MCP server install directory** (created by `uv run setup.py`) |
| **MCP server — secrets (traditional token auth only)** | Commvault `access_token`, `refresh_token`, MCP `server_secret`, `server_secret_expiry` | OS keyring service **`commvault-mcp-server`** or **`commvault-mcp-server:<MCP_INSTANCE_ID>`** with entries of the same names (see [Running Multiple Instances on the Same Host](#running-multiple-instances-on-the-same-host)). On Windows: Credential Manager; on macOS: Keychain; on Linux: Secret Service / KWallet per backend chosen by `keyring` |
| **MCP server — secrets (OAuth mode)** | OAuth client credentials and endpoints | `.env` only (`OAUTH_CLIENT_SECRET`, etc.); **no** Commvault refresh token in keyring for OAuth path in this project |
| **CommServe** | Commvault tokens are issued and audited on the server; MCP does not store CommServe-side credentials beyond the API tokens in keyring (traditional) or forwarded OAuth bearer (OAuth mode) | Command Center token management and CommCell audit (product documentation) |

**What to monitor for hygiene reviews**

- MCP client configs that duplicate the **MCP server secret** or Commvault tokens in plaintext JSON.
- Stale copies in `output.txt`, shell history, or committed files under the install directory.
- Keyring namespace: unintended overwrites when multiple installs share one OS user without `MCP_INSTANCE_ID`.

### Validated Reference Configurations

The following are **reference baselines** exercised in development/testing of this repository. Your environment may differ; clients not listed here may work but are **not** documented as validated in this README.

| Configuration | Validated in project testing | Notes |
|---------------|------------------------------|-------|
| Traditional token auth (`USE_OAUTH=false`), `streamable-http`, local MCP on `localhost` with `MCP_PATH=/mcp` | Yes | Windows host, secure Windows keyring backend, `.env` + keyring as above |
| MCP client: **Cursor** native HTTP MCP (`url` + `headers.Authorization` bearer **MCP server secret**) | Yes | Structure differs from `mcp-remote` examples in [Configuring Clients](#configuring-clients); follow Cursor MCP docs |
| MCP client: **`mcp-remote`** with `--header Authorization: …` | Documented | Referenced in README client sections; treat as supported pattern, not re-tested in every release here |
| Transport **`stdio`** (local process, no HTTP `server_secret` on wire) | Documented | No HTTP middleware `server_secret` gate; client spawns server process directly |
| Transport **`sse`** | Documented | Same token-auth middleware as `streamable-http` when OAuth is disabled; **not** separately validated in project test notes for every client |
| **OAuth** MCP auth (`USE_OAUTH=true`) with Docker image | Documented | Container uses OAuth only (no keyring); client OAuth flows vary by IDE—**not** validated end-to-end for every client in this README |
| **OAuth** on host install via `setup.py` | Documented | Requires CommServe OAuth (SP42 CU27+); operator client integration **not** listed as validated here |
| **Metallic** (`IS_METALLIC=true`, gateway URL) | Documented | Separate API base URL path in code; validate per Metallic deployment guide |
| Linux / macOS keyring backends | Documented | Allowed backends listed under [Prerequisites](#2-authentication--security-configuration); platform-specific UI paths for audit are operator responsibility |

When opening a support or security review ticket, state **auth mode**, **transport**, **MCP client product**, and whether **`MCP_INSTANCE_ID`** is set so responders can map your layout to this table.

## Contributing

- We're continuing to add more functionality to this MCP server. If you'd like to leave feedback, file a bug or provide a feature request, please open an issue on this repository.
- Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the Apache License. See the [LICENSE](./LICENSE) file for details.
