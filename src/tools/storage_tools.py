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

from fastmcp.exceptions import ToolError
from typing import Annotated
from pydantic import Field

from src.cv_api_client import commvault_api_client
from src.logger import logger
from wrappers.wrappers import filter_storage_pool_response


def get_storage_policy_list() -> dict:
    """
    Gets storage policy list.
    """
    try:
        return commvault_api_client.get("V2/StoragePolicy")
    except Exception as e:
        logger.error(f"Error getting storage policy list: {e}")
        return ToolError({"error": str(e)})

def get_storage_policy_properties(storage_policy_id: Annotated[str, Field(description="The storage policy id to retrieve properties for.")]) -> dict:
    """
    Gets storage policy properties for a given storage policy id.
    """
    try:
        return commvault_api_client.get(f"V2/StoragePolicy/{storage_policy_id}?propertyLevel=10")
    except Exception as e:
        logger.error(f"Error getting storage policy properties: {e}")
        return ToolError({"error": str(e)})

def get_storage_policy_copy_details(storage_policy_id: Annotated[str, Field(description="The storage policy id to retrieve copy details for.")], copy_id: Annotated[str, Field(description="The copy id to retrieve details for.")]) -> dict:
    """
    Gets storage policy copy details for a given storage policy id and copy id.
    """
    try:
        return commvault_api_client.get(f"V2/StoragePolicy/{storage_policy_id}/Copy/{copy_id}")
    except Exception as e:
        logger.error(f"Error getting storage policy copy details: {e}")
        return ToolError({"error": str(e)})

def get_storage_policy_copy_size(storage_policy_id: Annotated[str, Field(description="The storage policy id to retrieve copy size for.")], copy_id: Annotated[str, Field(description="The copy id to retrieve size for.")]) -> dict:
    """
    Gets storage policy copy size for a given storage policy id and copy id.
    """
    try:
        return commvault_api_client.get(f"V2/StoragePolicy/{storage_policy_id}/Copy/{copy_id}/Size")
    except Exception as e:
        logger.error(f"Error getting storage policy copy size: {e}")
        return ToolError({"error": str(e)})

def get_library_list() -> dict:
    """
    Gets the list of libraries.
    """
    try:
        return commvault_api_client.get("V2/Library?propertyLevel=10")
    except Exception as e:
        logger.error(f"Error retrieving library list: {e}")
        return ToolError({"error": str(e)})

def get_library_properties(library_id: Annotated[str, Field(description="The library id to retrieve properties for.")]) -> dict:
    """
    Gets properties for a given library id.
    """
    try:
        return commvault_api_client.get(f"V2/Library/{library_id}")
    except Exception as e:
        logger.error(f"Error retrieving library properties: {e}")
        return ToolError({"error": str(e)})

def get_storage_pool_list() -> dict:
    """
    Gets the list of storage pools, filtered for LLM-friendly output.
    """
    try:
        response = commvault_api_client.get("StoragePool")
        return filter_storage_pool_response(response)
    except Exception as e:
        logger.error(f"Error retrieving storage pool list: {e}")
        return ToolError({"error": str(e)})
    
def get_mediaagent_list() -> dict:
    """
    Gets the list of media agents.
    """
    try:
        return commvault_api_client.get("MediaAgent")
    except Exception as e:
        logger.error(f"Error retrieving media agent list: {e}")
        return ToolError({"error": str(e)})
    
STORAGE_MANAGEMENT_TOOLS = [
    get_storage_policy_list,
    get_storage_policy_properties,
    get_storage_policy_copy_details,
    get_storage_policy_copy_size,
    get_library_list,
    get_library_properties,
    get_storage_pool_list,
    get_mediaagent_list
]
