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
from getpass import getpass

import keyring
from rich.console import Console
from rich.prompt import Prompt
from pyfiglet import Figlet

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

def prompt_update_env(env_vars):
    keys = ['CC_SERVER_URL', 'MCP_TRANSPORT_MODE', 'MCP_HOST', 'MCP_PORT', 'MCP_PATH']
    transport_modes = ['streamable-http', 'stdio', 'sse']
    console.print("\n[bold underline]Environment Variables[/bold underline]")
    console.print("Press Enter to keep the current value (shown in brackets).\n")

    for key in keys:
        current_val = env_vars.get(key, '')

        if key == 'MCP_TRANSPORT_MODE':
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
                    import requests
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
        console.print("Leave blank to keep the existing secret.\n")
        console.print("[bold yellow]Warning: Ensure you're entering sensitive tokens in a secure terminal environment.[/bold yellow]")
        
        basic_keys = ['access_token', 'refresh_token', 'server_secret']
        for key in basic_keys:
            current = keyring.get_password(service_name, key)
            display_val = "<hidden>" if current else "none"
            prompt_text = f"Enter {key} [{display_val}]"
            val = getpass(prompt_text + ": ")
            if val:
                keyring.set_password(service_name, key, val)
                console.print(f"[green]{key} updated.[/green]")
            else:
                console.print(f"[yellow]{key} unchanged.[/yellow]")
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
