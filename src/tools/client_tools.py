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
from wrappers.wrappers import filter_client_list_response, filter_subclient_response, get_basic_client_group_details


def get_client_group_list() -> dict:
    """
    Gets the list of client groups.
    """
    try:
        response = commvault_api_client.get("ClientGroup")
        return get_basic_client_group_details(response)
    except Exception as e:
        logger.error(f"Error retrieving client group list: {e}")
        return ToolError({"error": str(e)})
    
def get_client_list() -> dict:
    """
    Gets the list of clients.
    """
    try:
        response = commvault_api_client.get("Client")
        return filter_client_list_response(response)
    except Exception as e:
        logger.error(f"Error retrieving client list: {e}")
        return ToolError({"error": str(e)})

def get_client_group_properties(client_group_id: Annotated[str, Field(description="The client group id to retrieve properties for.")]) -> dict:
    """
    Gets properties for a given client group id.
    """
    try:
        return commvault_api_client.get(f"ClientGroup/{client_group_id}")
    except Exception as e:
        logger.error(f"Error retrieving client group properties: {e}")
        return ToolError({"error": str(e)})

def get_clientid_from_clientname(client_name: Annotated[str, Field(description="The client name to retrieve client id for.")]) -> dict:
    """
    Gets client id for a given client name.
    """
    try:
        return commvault_api_client.get("getid", params={"clientname": client_name})
    except Exception as e:
        logger.error(f"Error retrieving client id: {e}")
        return ToolError({"error": str(e)})
    
def get_subclient_list(client_identifier: Annotated[str, Field(description="The client name or ID to retrieve subclients for.")], identifier_type: Annotated[str, Field(description="Specify 'name' to use client name or 'id' to use client ID.")]) -> dict:
    """
    Gets subclient list for a given client name or client id.
    Args:
        client_identifier: The client name or ID.
        identifier_type: 'name' or 'id'.
    Returns:
        Subclient list for the specified client, filtered for LLM-friendly output.
    """
    try:
        if identifier_type == 'name':
            params = {"clientName": client_identifier}
        elif identifier_type == 'id':
            params = {"clientId": client_identifier}
        else:
            raise ValueError("identifier_type must be 'name' or 'id'")
        response = commvault_api_client.get("subclient", params=params)
        return filter_subclient_response(response)
    except Exception as e:
        logger.error(f"Error getting subclient list for client {identifier_type}: {e}")
        return ToolError({"error": str(e)})

def get_subclient_properties(subclient_id: Annotated[str, Field(description="The subclient id to retrieve properties for.")]) -> dict:
    """
    Gets subclient properties for a given subclient id.
    """
    try:
        return commvault_api_client.get(f"subclient/{subclient_id}")
    except Exception as e:
        logger.error(f"Error getting subclient properties: {e}")
        return ToolError({"error": str(e)})

def run_backup_for_subclient(
    subclient_id: Annotated[str, Field(description="The subclient id to run backup for.")],
    backup_type: Annotated[str, Field(description="The type of backup to run. Valid values are 'Full', 'INCREMENTAL', 'SYNTHETIC_FULL'")],
) -> dict:
    """
    Runs a backup job for the specified subclient with the given backup type.
    """
    try:
        params = {
            "backupLevel": backup_type
        }
        return commvault_api_client.post(
            f"subclient/{subclient_id}/action/backup",
            params=params
        )
    except Exception as e:
        logger.error(f"Error running backup for subclient {subclient_id}: {e}")
        return ToolError({"error": str(e)})
        
CLIENT_MANAGEMENT_TOOLS = [
    get_client_group_list,
    get_client_list,
    get_client_group_properties,
    get_clientid_from_clientname,
    get_subclient_list,
    get_subclient_properties,
    run_backup_for_subclient
]
