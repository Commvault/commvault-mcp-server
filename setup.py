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

import os
import secrets
from getpass import getpass
from urllib.parse import urljoin

import keyring
import requests
from rich.console import Console
from rich.prompt import Prompt
from pyfiglet import Figlet

from src.utils import get_env_var

console = Console()

ENV_FILE = '.env'

def print_title():
    f = Figlet(font='slant')
    ascii_art = f.renderText('Commvault \nMCP Server')
    console.print(f"[bold][red]{ascii_art}[/red][/bold]")

def load_env():
    env_vars = {}
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env_vars[k.strip()] = v.strip()
    return env_vars

def save_env(env_vars):
    with open(ENV_FILE, 'w') as f:
        for k, v in env_vars.items():
            f.write(f"{k}={v}\n")

def validate_https_url(url):
    if not url:
        return False, "URL cannot be empty."
    
    url_lower = url.lower().strip()
    if url_lower.startswith('http://'):
        return False, "HTTP URLs are not allowed for security reasons. Please use HTTPS."
    if not url_lower.startswith('https://'):
        return False, "URL must start with 'https://' for secure communication."
    
    return True, None

def validate_commvault_tokens(access_token, refresh_token, server_url):
    """
    Validate Commvault access and refresh tokens by making a test API call.
    """
    if not access_token or not refresh_token:
        return False, "Access token and refresh token are required."
    
    if not server_url:
        return False, "Commvault server URL is required for token validation."
    
    try:
        api_base_url = urljoin(server_url.rstrip('/') + '/', 'commandcenter/api/')
        test_endpoint = urljoin(api_base_url, 'v2/whoami')
        
        headers = {
            'Accept': 'application/json',
            'Authtoken': access_token
        }
        
        console.print("[dim]Validating Commvault tokens...[/dim]")
        response = requests.get(
            test_endpoint, 
            headers=headers, 
            timeout=10, 
            verify=get_env_var("SSL_VERIFY", default="true").lower() == "true")
        
        if response.status_code == 200:
            return True, None
        elif response.status_code == 401:
            return False, "Invalid access token. The token may be expired or incorrect. Please generate a new token."
        else:
            return False, f"Token validation failed with HTTP {response.status_code}. Please check your tokens and server URL."
            
    except requests.exceptions.SSLError as e:
        return False, f"SSL verification failed: {str(e)}. Please check your server certificate."
    except requests.exceptions.ConnectionError as e:
        return False, f"Connection failed: {str(e)}. Please check your server URL and network connectivity."
    except requests.exceptions.Timeout:
        return False, "Connection timeout. Please check your server URL and network connectivity."
    except Exception as e:
        return False, f"Token validation error: {str(e)}"

def prompt_update_env(env_vars):
    keys = ['CC_SERVER_URL', 'MCP_TRANSPORT_MODE', 'MCP_HOST', 'MCP_PORT', 'MCP_PATH']
    transport_modes = ['streamable-http', 'stdio', 'sse']
    console.print("\n[bold underline]Environment Variables[/bold underline]")
    console.print("Press Enter to keep the current value (shown in brackets).\n")

    for key in keys:
        current_val = env_vars.get(key, '')

        if key == 'CC_SERVER_URL':
            while True:
                val = Prompt.ask(key, default=current_val if current_val else '')
                if not val:
                    console.print("[red]CC_SERVER_URL is required. Please enter a valid HTTPS URL.[/red]")
                    continue
                
                is_valid, error_msg = validate_https_url(val)
                if is_valid:
                    env_vars[key] = val
                    break
                else:
                    console.print(f"[red]{error_msg}[/red]")
        elif key == 'MCP_TRANSPORT_MODE':
            console.print(f"{key} [dim](Current: {current_val if current_val else 'None'})[/dim]")
            for i, mode in enumerate(transport_modes, 1):
                console.print(f"  {i}. {mode}")
            while True:
                choice = Prompt.ask("Select transport mode [1-3]", default=str(
                    transport_modes.index(current_val) + 1) if current_val in transport_modes else "1")
                if choice in ['1', '2', '3']:
                    val = transport_modes[int(choice) - 1]
                    env_vars[key] = val
                    break
                else:
                    console.print("[red]Invalid choice. Please enter 1, 2, or 3.[/red]")
            if val == 'stdio':
                env_vars[key] = val
                break # other variables are not needed for stdio mode
        elif key == 'MCP_PATH':
            val = Prompt.ask(key, default=current_val if current_val else '/mcp')
        else:
            val = Prompt.ask(key, default=current_val)
        env_vars[key] = val

    # Ask about OAuth configuration only for non-stdio modes
    if env_vars.get('MCP_TRANSPORT_MODE') != 'stdio':
        console.print("\n[bold underline]Authentication Configuration[/bold underline]")
        current_oauth = env_vars.get('USE_OAUTH', 'false').lower()
        use_oauth = Prompt.ask("Use OAuth for authentication? (y/n)", 
                             default='y' if current_oauth == 'true' else 'n')
        
        if use_oauth.lower() in ['y', 'yes', 'true']:
            env_vars['USE_OAUTH'] = 'true'
            console.print("\n[bold]OAuth Configuration[/bold]")
            
            # First ask for discovery endpoint
            discovery_endpoint = Prompt.ask("OAuth Discovery Endpoint URL", 
                                         default=env_vars.get('OAUTH_DISCOVERY_ENDPOINT', ''))
            env_vars['OAUTH_DISCOVERY_ENDPOINT'] = discovery_endpoint
            
            # If discovery endpoint is provided, fetch and set the other endpoints
            if discovery_endpoint:
                try:
                    console.print("[dim]Fetching OAuth configuration from discovery endpoint...[/dim]")
                    response = requests.get(discovery_endpoint)
                    if response.status_code == 200:
                        discovery_data = response.json()
                        env_vars['OAUTH_AUTHORIZATION_ENDPOINT'] = discovery_data.get('authorization_endpoint', '')
                        env_vars['OAUTH_TOKEN_ENDPOINT'] = discovery_data.get('token_endpoint', '')
                        env_vars['OAUTH_JWKS_URI'] = discovery_data.get('jwks_uri', '')
                        console.print("[green]Successfully retrieved OAuth endpoints from discovery URL.[/green]")
                    else:
                        console.print(f"[red]Failed to fetch from discovery endpoint (HTTP {response.status_code}). Setup aborted.[/red]")
                        exit(1)
                except Exception as e:
                    console.print(f"[red]Error fetching from discovery endpoint: {str(e)} Setup aborted.[/red]")
                    exit(1)
            
            # Add the remaining OAuth configuration that can't be obtained from discovery
            oauth_keys = [
                ('OAUTH_CLIENT_ID', 'OAuth Client ID'),
                ('OAUTH_CLIENT_SECRET', 'OAuth Client Secret'),
                ('OAUTH_REQUIRED_SCOPES', 'OAuth Required Scopes (comma-separated)'),
                ('OAUTH_BASE_URL', 'OAuth Base URL')
            ]

            for key, description in oauth_keys:
                current_val = env_vars.get(key, '')
                if key == 'OAUTH_CLIENT_SECRET':
                    masked = '*' * len(current_val) if current_val else ''
                    val = getpass(f"{description} [{masked}]: ", stream=None)
                    if val:
                        console.print(f"[green]{description} updated.[/green]")
                    else:
                        console.print(f"[yellow]{description} unchanged.[/yellow]")
                    if not val:
                        val = current_val
                else:
                    val = Prompt.ask(f"{description}", default=current_val)
                env_vars[key] = val

        else:
            env_vars['USE_OAUTH'] = 'false'
            # Remove OAuth-related vars if user chooses not to use OAuth
            oauth_keys_to_remove = [
                'OAUTH_AUTHORIZATION_ENDPOINT', 'OAUTH_TOKEN_ENDPOINT', 
                'OAUTH_CLIENT_ID', 'OAUTH_JWKS_URI', 'OAUTH_REQUIRED_SCOPES', 
                'OAUTH_BASE_URL', 'OAUTH_CLIENT_SECRET', 'OAUTH_DISCOVERY_ENDPOINT'
            ]
            for key in oauth_keys_to_remove:
                env_vars.pop(key, None)

    return env_vars

def prompt_and_save_keyring(service_name, env_vars):
    # Only ask for keyring secrets if NOT using OAuth
    if env_vars.get('USE_OAUTH', 'false').lower() != 'true':
        console.print(f"\n[bold underline]Secure Tokens (stored in OS keyring)[/bold underline]")
        console.print("[bold yellow]Warning: Ensure you're entering sensitive tokens in a secure terminal environment.[/bold yellow]\n")
        
        # Auto-generate server_secret
        console.print("[bold]Server Secret (Auto-generated)[/bold]")
        server_secret = secrets.token_urlsafe(32)  # 32 bytes = 43 characters URL-safe
        keyring.set_password(service_name, 'server_secret', server_secret)
        console.print("[green]✓ Server secret generated and stored securely.[/green]")
        console.print("\n[bold yellow]IMPORTANT: Copy this server secret for your LLM configuration:[/bold yellow]")
        console.print(f"[bold cyan]{server_secret}[/bold cyan]")
        console.print("[dim]This secret must be included in the Authorization header when connecting to the MCP server.[/dim]\n")
        
        # Prompt for access_token and refresh_token with validation
        console.print("[bold]Commvault API Tokens[/bold]")
        console.print("Leave blank to keep the existing token.\n")
        
        server_url = env_vars.get('CC_SERVER_URL', '')
        
        # Handle access_token
        while True:
            current_access = keyring.get_password(service_name, 'access_token')
            display_val = "<hidden>" if current_access else "none"
            access_token = getpass(f"Enter access_token [{display_val}]: ")
            
            if not access_token:
                # User wants to keep existing token
                if current_access:
                    console.print("[yellow]Access token unchanged.[/yellow]")
                    access_token = current_access
                    break
                else:
                    console.print("[red]Access token is required. Please enter a valid token.[/red]")
                    continue
            
            # Handle refresh_token
            current_refresh = keyring.get_password(service_name, 'refresh_token')
            display_val = "<hidden>" if current_refresh else "none"
            refresh_token = getpass(f"Enter refresh_token [{display_val}]: ")
            
            if not refresh_token:
                # User wants to keep existing refresh token
                if current_refresh:
                    refresh_token = current_refresh
                else:
                    console.print("[red]Refresh token is required when updating access token.[/red]")
                    continue
            
            # Validate tokens if server URL is available
            if server_url:
                is_valid, error_msg = validate_commvault_tokens(access_token, refresh_token, server_url)
                if is_valid:
                    # Store validated tokens
                    keyring.set_password(service_name, 'access_token', access_token)
                    keyring.set_password(service_name, 'refresh_token', refresh_token)
                    console.print("[green]Tokens validated and stored successfully.[/green]")
                    break
                else:
                    console.print(f"[red]✗ {error_msg}[/red]")
                    retry = Prompt.ask("Would you like to try again? (y/n)", default='y')
                    if retry.lower() not in ['y', 'yes']:
                        console.print("[yellow]Skipping token update. Existing tokens (if any) will be used.[/yellow]")
                        break
            else:
                # No server URL available, store without validation
                console.print("[yellow]⚠ Warning: Server URL not configured. Storing tokens without validation.[/yellow]")
                keyring.set_password(service_name, 'access_token', access_token)
                keyring.set_password(service_name, 'refresh_token', refresh_token)
                console.print("[green]✓ Tokens stored (not validated).[/green]")
                break
    else:
        console.print(f"\n[bold green]OAuth authentication enabled - skipping keyring token setup.[/bold green]")
        console.print("[dim]OAuth will handle authentication using the configured endpoints and client credentials.[/dim]")

def main():
    console.clear()
    print_title()

    env_vars = load_env()
    env_vars = prompt_update_env(env_vars)
    save_env(env_vars)
    console.print(f"\n[green]Updated {ENV_FILE} file.[/green]")

    service_name = 'commvault-mcp-server'
    prompt_and_save_keyring(service_name, env_vars)

    console.print("\n[bold green]Setup complete! You can now run the MCP server (uv run -m src.server)[/bold green]")

if __name__ == '__main__':
    main()
