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
from src.wrappers import filter_security_associations_response, filter_user_groups_response, filter_users_response


def get_users_list() -> dict:
    """
    Gets the list of users in the CommCell.
    Returns:
        A dictionary containing the list of users.
    """
    try:
        response = commvault_api_client.get("v4/user")
        return filter_users_response(response)
    except Exception as e:
        logger.error(f"Error retrieving user list: {e}")
        return ToolError({"error": str(e)})
    
def get_user_properties(user_id: Annotated[str, Field(description="The user id to retrieve properties for.")]) -> dict:
    """
    Gets properties for a given user id.
    """
    try:
        return commvault_api_client.get(f"v4/user/{user_id}")
    except Exception as e:
        logger.error(f"Error retrieving user properties: {e}")
        return ToolError({"error": str(e)})

def set_user_enabled(user_id: Annotated[str, Field(description="The user id to enable or disable.")], enabled: Annotated[bool, Field(description="Set to True to enable the user, False to disable.")]) -> dict:
    """
    Enables or disables a user with the given user id based on the 'enabled' flag.
    """
    try:
        action = "enable" if enabled else "disable"
        response = commvault_api_client.put(f"user/{user_id}/{action}")
        if response["response"][0].get("errorCode", -1) == 0:
            return {"message": f"User {action}d successfully."}
        else:
            error_message = response["response"][0].get("errorMessage", "Unknown error occurred.")
            raise Exception(f"Failed to {action} user: {error_message}")
    except Exception as e:
        logger.error(f"Error {'enabling' if enabled else 'disabling'} user: {e}")
        return ToolError({"error": str(e)})

def get_user_groups_list() -> dict:
    """
    Gets the list of user groups in the CommCell.
    Returns:
        A dictionary containing the list of user groups.
    """
    try:
        response = commvault_api_client.get("v4/usergroup")
        return filter_user_groups_response(response)
    except Exception as e:
        logger.error(f"Error retrieving user group list: {e}")
        return ToolError({"error": str(e)})

def get_user_group_properties(user_group_id: Annotated[str, Field(description="The user group id to retrieve properties for.")]) -> dict:
    """
    Gets properties for a given user group id.
    """
    try:
        return commvault_api_client.get(f"v4/usergroup/{user_group_id}")
    except Exception as e:
        logger.error(f"Error retrieving user group properties: {e}")
        return ToolError({"error": str(e)})
    
def get_associated_entities_for_user_or_group(id: Annotated[str, Field(description="The user or user group id to retrieve associated entities for.")], type: Annotated[str, Field(description="Specify 'user' for user id or 'usergroup' for user group id.")]) -> dict:
    """
    Gets the associated entities (roles and permissions for each entity) for a given user or user group id.
    """
    try:
        response = commvault_api_client.get(f"{type.lower()}/{id}/security")
        return filter_security_associations_response(response)
    except Exception as e:
        logger.error(f"Error retrieving associated entities for user {id}: {e}")
        return ToolError({"error": str(e)})
    
def view_entity_permissions(
    entity_type: Annotated[str, Field(description="The type of entity to view permissions for. Valid values are: COMMCELL_ENTITY, CLIENT_ENTITY, INSTANCE_ENTITY, BACKUPSET_ENTITY, SUBCLIENT_ENTITY, CLIENT_GROUP_ENTITY, USER_ENTITY, USERGROUP_ENTITY, LIBRARY_ENTITY, STORAGE_POLICY_ENTITY, STORAGE_POLICY_COPY_ENTITY, SUBCLIENT_POLICY_ENTITY.")],
    entity_id: Annotated[str, Field(description="The ID of the entity to view permissions for.")]
) -> dict:
    """
    Retrieves permissions the user has for a specific entity type and ID.
    """
    try:
        response = commvault_api_client.get(f"Security/{entity_type}/{entity_id}/Permissions")
        return response
    except Exception as e:
        logger.error(f"Error retrieving permissions for entity {entity_type} with ID {entity_id}: {e}")
        return ToolError({"error": str(e)})
    
def get_roles_list() -> dict:
    """
    Gets the list of roles in the CommCell.
    """
    try:
        response = commvault_api_client.get("v4/role")
        return response
    except Exception as e:
        logger.error(f"Error retrieving roles list: {e}")
        return ToolError({"error": str(e)})
    
def get_my_user_info() -> dict:
    """
    Gets the information about the current user.
    """
    try:
        response = commvault_api_client.get("v2/whoami")
        return response
    except Exception as e:
        logger.error(f"Error retrieving my user info: {e}")
        return ToolError({"error": str(e)})

USER_MANAGEMENT_TOOLS = [
    get_users_list,
    get_user_properties,
    set_user_enabled,
    get_user_groups_list,
    get_user_group_properties,
    get_associated_entities_for_user_or_group,
    view_entity_permissions,
    get_roles_list,
    get_my_user_info
]
