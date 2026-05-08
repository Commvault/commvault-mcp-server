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

import json
import threading
import time
from typing import Dict, Any, Optional, Union
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

from src.auth.oauth_service import OAuthService
from src.auth.auth_service import AuthService
from src.logger import logger
from src.utils import get_env_var, sanitize_endpoint_path

load_dotenv()

class CommvaultApiClient:
    
    def __init__(self, use_oauth=False):
        server_url = get_env_var("CC_SERVER_URL")
        is_metallic = get_env_var("IS_METALLIC", default="false").lower() == "true"
        if is_metallic:
            self.base_url = server_url.rstrip('/') + '/'
        else:
            self.base_url = server_url + "/commandcenter/api/"
        self.ssl_verify = get_env_var("SSL_VERIFY", default="true").lower() == "true"  # This is to disable SSL verification for development purposes
        self.use_oauth = use_oauth
        self.auth_service = OAuthService() if use_oauth else AuthService()
        self._use_bearer_auth = not use_oauth
        # Serializes concurrent _refresh_access_token() calls so we never POST
        # the same refresh_token to /V4/AccessToken/Renew twice in parallel
        # (the second POST would race the first and get a 500 from CommServe).
        self._refresh_lock = threading.Lock()

    def _get_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        auth_token, _ = self.auth_service.get_tokens()
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'commvault-mcp-server/0.1.0'
        }
        if self.use_oauth:
            headers['Authorization'] = auth_token # this will have Bearer prefix
        elif self._use_bearer_auth:
            headers['Authorization'] = f'Bearer {auth_token}'
        else:
            headers['Authtoken'] = auth_token

        if additional_headers:
            headers.update(additional_headers)
            
        return headers
    
    def _build_url(self, endpoint: str) -> str:
        # Sanitize endpoint to prevent path traversal attacks
        try:
            sanitized_endpoint = sanitize_endpoint_path(endpoint)
            if sanitized_endpoint != endpoint:
                logger.warning(f"Endpoint was sanitized: '{endpoint}' -> '{sanitized_endpoint}'")
        except ValueError as e:
            logger.error(f"Invalid endpoint: {e}")
            raise Exception("Invalid endpoint")
        
        return urljoin(self.base_url, sanitized_endpoint)

    def _refresh_access_token(self) -> bool:
        """
        Refresh the access/refresh token pair via /V4/AccessToken/Renew.

        Thread-safety:
            Refresh tokens are single-use on the CommServe. If two threads call
            this concurrently with the same refresh token, the first wins and
            the second gets HTTP 500 from /V4/AccessToken/Renew, which then
            cascades into "Failed to refresh token" errors for every in-flight
            request.

            To prevent that, we serialize callers on `_refresh_lock` and
            double-check the in-memory access token after acquiring the lock.
            If another thread already rotated the token while we were waiting,
            we skip the network call and reuse the freshly stored token.
        """
        # Snapshot the access token BEFORE acquiring the lock. This is our
        # reference point for "did someone else rotate the token while I was
        # blocked on the lock?".
        snapshot_access_token, _ = self.auth_service.get_tokens()

        with self._refresh_lock:
            current_access_token, current_refresh_token = self.auth_service.get_tokens()

            if current_access_token != snapshot_access_token:
                logger.info(
                    "Access token already refreshed by a concurrent request; "
                    "reusing the rotated token instead of calling /V4/AccessToken/Renew again."
                )
                return True

            try:
                refresh_url = self._build_url("V4/AccessToken/Renew")
                payload = {
                    "accessToken": current_access_token,
                    "refreshToken": current_refresh_token
                }

                headers = {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'User-Agent': 'commvault-mcp-server/0.1.0'
                }

                response = requests.post(
                    url=refresh_url,
                    headers=headers,
                    data=json.dumps(payload),
                    verify=self.ssl_verify
                )

                response.raise_for_status()

                new_access_token = response.json().get("accessToken")
                new_refresh_token = response.json().get("refreshToken")
                if not new_access_token or not new_refresh_token:
                    raise Exception("No new tokens received")
                self.auth_service.set_tokens(new_access_token, new_refresh_token)

                logger.info("Access token refreshed successfully")
                return True

            except Exception as e:
                logger.error(f"Failed to refresh access token: {str(e)}")
                return False
    
    def request(self, 
                method: str, 
                endpoint: str, 
                params: Optional[Dict[str, Any]] = None, 
                data: Optional[Union[Dict[str, Any], str]] = None, 
                headers: Optional[Dict[str, str]] = None,
                max_retries: int = 2,
                retry_delay: float = 1.0) -> requests.Response:

        # Check if the secret key is passed in the header for sse and streamable-http modes when OAuth is not used
        if get_env_var('USE_OAUTH', 'false')!="true" and get_env_var('MCP_TRANSPORT_MODE')!="stdio":
            is_valid, error_message = self.auth_service.is_client_token_valid()
            if not is_valid:
                raise Exception(f"{error_message}")
        
        url = self._build_url(endpoint)
        request_headers = self._get_headers(headers)

        logger.info(f"{method} request to: {url}")
        
        request_data = data
        if isinstance(data, dict):
            request_data = json.dumps(data)
            if 'Content-Type' not in request_headers:
                request_headers['Content-Type'] = 'application/json'

        retries = 0
        while retries <= max_retries:
            try:
                response = requests.request(
                    method=method.upper(),
                    url=url,
                    headers=request_headers,
                    params=params,
                    data=request_data,
                    verify= self.ssl_verify
                )
                logger.info(f"Response status code: {response.status_code}")
                
                # Handle 401 Unauthorized error (expired token)
                if response.status_code == 401 and not self.use_oauth:
                    if self._use_bearer_auth:
                        logger.info("Bearer auth returned 401. Falling back to Authtoken header...")
                        self._use_bearer_auth = False
                        request_headers = self._get_headers(headers)
                        continue

                    logger.info("Received 401 Unauthorized response. Attempting to refresh token...")
                    success = self._refresh_access_token()
                    
                    if success:
                        request_headers = self._get_headers(headers)
                        logger.info(f"Retrying {method} request with new token")
                        continue
                
                # Catch other HTTP errors
                response.raise_for_status()

                try:
                    response_json = response.json()
                    logger.debug(f"Response content: {response_json}")
                    return response_json
                except ValueError:
                    logger.error(f"Invalid JSON response: {response.text[:100]}...")
                    raise Exception("Invalid response format from server")
                
            except requests.exceptions.HTTPError as e:
                # Let the 401 handling above take care of auth errors
                if e.response.status_code != 401:
                    retries += 1
                    if retries > max_retries:
                        raise

                    backoff_time = retry_delay * (2 ** (retries - 1))
                    logger.warning(f"Request failed with {e}. Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                else:
                    # If we get here, it means we got a 401 and the token refresh failed
                    if not self.use_oauth:
                        raise Exception("Failed to refresh token. please create a new token update the keyring")
                    raise e
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > max_retries:
                    raise Exception("Some issue occured. Please try again later.")
                
                backoff_time = retry_delay * (2 ** (retries - 1))
                logger.warning(f"Request failed with {e}. Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)

            except Exception as e:
                logger.error(f"An unexpected error occurred: {str(e)}")
                raise
    
    # Convenience methods for common HTTP methods
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make a GET request to the API."""
        return self.request("GET", endpoint, params=params, headers=headers)
    
    def post(self, endpoint: str, data: Optional[Union[Dict[str, Any], str]] = None, 
             params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make a POST request to the API."""
        return self.request("POST", endpoint, params=params, data=data, headers=headers)
    
    def put(self, endpoint: str, data: Optional[Union[Dict[str, Any], str]] = None, 
             params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make a PUT request to the API."""
        return self.request("PUT", endpoint, params=params, data=data, headers=headers)

commvault_api_client = CommvaultApiClient(use_oauth=(get_env_var('USE_OAUTH', 'false').lower() == 'true'))
