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
import re
from typing import Optional
from urllib.parse import unquote
from dotenv import load_dotenv


load_dotenv()

def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    value = os.getenv(var_name, default)
    if value is None:
        raise ValueError(f"Please check if you have set all the environment variables {var_name}. You can run the setup script to set them.")
    return value


def sanitize_endpoint_path(endpoint: str) -> str:
    """
    Sanitizes an API endpoint path to prevent path traversal attacks.
    
    This function validates and sanitizes endpoint paths by:
    - Removing path traversal sequences (.., ../, etc.)
    - Removing dangerous characters from path segments (/, \, ?, #, and encoded equivalents)
    - Validating each path segment contains only safe characters
    - Preserving query strings and fragments that appear at the end of the path
    
    Args:
        endpoint: The endpoint path string to sanitize
        
    Returns:
        A sanitized endpoint path string
        
    Raises:
        ValueError: If the endpoint contains path traversal or other dangerous patterns
    """
    if not endpoint or not isinstance(endpoint, str):
        raise ValueError("Endpoint must be a non-empty string")
    
    # Separate path from query string and fragment
    # Query strings and fragments are allowed at the end, but not in path segments
    query_fragment = ''
    path_part = endpoint
    
    # Find the first ? or # that's not part of a path segment
    # (i.e., appears after the last /)
    query_pos = endpoint.find('?')
    fragment_pos = endpoint.find('#')
    
    if query_pos != -1 or fragment_pos != -1:
        # Find the position of the first query/fragment marker
        first_marker = min(
            query_pos if query_pos != -1 else len(endpoint),
            fragment_pos if fragment_pos != -1 else len(endpoint)
        )
        # Only treat as query/fragment if it's after the last path separator
        last_slash = endpoint.rfind('/', 0, first_marker)
        if last_slash < first_marker:
            path_part = endpoint[:first_marker]
            query_fragment = endpoint[first_marker:]
    
    # URL decode to catch encoded path traversal attempts
    try:
        decoded_path = unquote(path_part)
    except Exception:
        decoded_path = path_part
    
    # Check for path traversal patterns (before and after decoding)
    dangerous_patterns = [
        r'\.\.',           # .. (path traversal)
        r'\.\./',          # ../
        r'\.\.\\',         # ..\
        r'%2e%2e',         # URL encoded ..
        r'%2E%2E',         # URL encoded .. (uppercase)
        r'\.\.%2f',        # Mixed encoding
        r'\.\.%2F',        # Mixed encoding (uppercase)
    ]
    
    for pattern in dangerous_patterns:
        if re.search(pattern, path_part, re.IGNORECASE) or re.search(pattern, decoded_path, re.IGNORECASE):
            raise ValueError(f"Path traversal detected in endpoint: {endpoint}")
    
    # Split path into segments
    # Handle both absolute paths (starting with /) and relative paths
    is_absolute = path_part.startswith('/')
    segments = [seg for seg in path_part.split('/') if seg]  # Remove empty segments
    
    # Validate each segment
    sanitized_segments = []
    for segment in segments:
        if not segment:
            continue
            
        # Check for dangerous characters in segment
        # Allow: alphanumeric, hyphens, underscores, dots (for version numbers like v4, V2)
        # Disallow: path separators, backslashes, query markers, and other special chars
        if re.search(r'[\/\\\?\#\<\>\"\|\*\:]', segment):
            raise ValueError(f"Invalid characters detected in endpoint segment: {segment}")
        
        # Additional check: ensure no encoded dangerous chars
        try:
            decoded_segment = unquote(segment)
            if re.search(r'[\/\\\?\#\<\>\"\|\*\:]', decoded_segment):
                raise ValueError(f"Invalid encoded characters detected in endpoint segment: {segment}")
        except Exception:
            pass  # If decoding fails, the original check above will catch it
        
        sanitized_segments.append(segment)
    
    # Reconstruct the path
    sanitized = '/'.join(sanitized_segments)
    if is_absolute:
        sanitized = '/' + sanitized
    
    # Reattach query string and fragment if they were present
    return sanitized + query_fragment
