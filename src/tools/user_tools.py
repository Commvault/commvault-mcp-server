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

def set_user_group_assignment(
    user_id: Annotated[str, Field(description="The user id to assign to the user group.")], 
    user_group_id: Annotated[str, Field(description="The user group id to assign the user to.")], 
    assign: Annotated[bool, Field(description="Set to True to assign the user to the group, False to remove the user from the group.")]=True
    ) -> dict:
    """
    Assigns or removes a user from a user group based on the 'assign' flag.
    Set assign=True to add the user to the group, or assign=False to remove the user from the group.
    """
    try:
        operation = "ADD" if assign else "DELETE"
        payload = {
            "userGroupOperation": operation,
            "userGroups": [
                {
                    "id": user_group_id
                }
            ],
        }
        return commvault_api_client.put(f"v4/user/{user_id}", data=payload)
    except Exception as e:
        logger.error(f"Error assigning user {user_id} to user group {user_group_id}: {e}")
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

def grant_or_revoke_access_to_entity(
    entity_id: Annotated[str, Field(description="The ID of the entity to grant access to.")],
    entity_type: Annotated[str, Field(description="The type of the entity to grant access to. Valid values: 'client', 'client_group', 'agent', 'instance', 'backup_set', 'subclient', 'storage_policy', 'schedule_policy', 'alert', 'workflow', 'plan'.")],
    role_id: Annotated[int, Field(description="The role ID to assign. You can get the role ID using the get_roles_list tool or ask the user to provide it.")],
    user_id: Annotated[str, Field(description="The user ID to grant access to.")],
    assign: Annotated[bool, Field(description="Set to True to grant access, False to revoke access. Default is True.")]=True
) -> dict:
    """
    Grants or revokes access to an entity for a user with a specific role.
    """
    ENTITY_TYPE_MAP = {
        "client": 3,
        "client_group": 28,
        "agent": 4,
        "instance": 5,
        "backup_set": 6,
        "subclient": 7,
        "storage_policy": 17,
        "schedule_policy": 35,
        "alert": 64,
        "workflow": 83,
        "plan": 158,
    }
    try:
        entity_type_num = ENTITY_TYPE_MAP.get(entity_type.lower())
        if entity_type_num is None:
            raise ValueError(f"Invalid entity_type: {entity_type}")
        payload = {
            "entityAssociated": {
                "entity": [
                    {
                        "entityType": entity_type_num,
                        "_type_": entity_type_num if entity_type.lower() != "plan" else 150, # plan uses a different _type_
                        "entityId":entity_id
                    }
                ]
            },
            "securityAssociations": {
                "associationsOperationType": "ADD" if assign else "DELETE",
                "associations": [
                    {
                        "userOrGroup": [
                            {
                                "userId": user_id
                            }
                        ],
                        "properties": {
                            "role": {
                                "roleId": role_id
                            }
                        }
                    }
                ]
            }
        }
        return commvault_api_client.post("Security", data=payload)
    except Exception as e:
        logger.error(f"Error granting/revoking access to entity {entity_id}: {e}")
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
    
    
USER_MANAGEMENT_TOOLS = [
    get_users_list,
    get_user_properties,
    set_user_enabled,
    get_user_groups_list,
    get_user_group_properties,
    set_user_group_assignment,
    get_associated_entities_for_user_or_group,
    view_entity_permissions,
    grant_or_revoke_access_to_entity,
    get_roles_list
]
