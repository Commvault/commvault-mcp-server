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
from wrappers.wrappers import filter_schedules_response


def get_schedules_list() -> dict:
    """
    Gets the list of schedules (filtered for relevant information).
    """
    try:
        response = commvault_api_client.get("Schedules")
        return filter_schedules_response(response)
    except Exception as e:
        logger.error(f"Error retrieving schedule list: {e}")
        return ToolError({"error": str(e)})
    
def get_schedule_properties(schedule_id: Annotated[str, Field(description="The schedule id to retrieve properties for.")]) -> dict:
    """
    Gets properties for a given schedule id.
    """
    try:
        return commvault_api_client.get(f"Schedule/{schedule_id}")
    except Exception as e:
        logger.error(f"Error retrieving schedule properties: {e}")
        return ToolError({"error": str(e)})
    
def enable_schedule(schedule_id: Annotated[str, Field(description="The schedule id to enable.")]) -> dict:
    """
    Enables a schedule with the given schedule id.
    """
    try:
        return commvault_api_client.post(f"/Schedules/task/Action/Enable", data={"taskId": schedule_id})
    except Exception as e:
        logger.error(f"Error enabling schedule: {e}")
        return ToolError({"error": str(e)})
    
def disable_schedule(schedule_id: Annotated[str, Field(description="The schedule id to disable.")]) -> dict:
    """
    Disables a schedule with the given schedule id.
    """
    try:
        return commvault_api_client.post(f"/Schedules/task/Action/Disable", data={"taskId": schedule_id})
    except Exception as e:
        logger.error(f"Error disabling schedule: {e}")
        return ToolError({"error": str(e)})

SCHEDULE_MANAGEMENT_TOOLS = [
    get_schedules_list,
    get_schedule_properties,
    enable_schedule,
    disable_schedule
]
