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
from wrappers.wrappers import filter_client_list_response, filter_subclient_response, get_basic_client_group_details, filter_hypervisor_list_response
    
def get_hypervisor_list() -> dict:
    """
    Gets the list of hypervisors.
    """
    try:
        response = commvault_api_client.get("V4/Hypervisor")
        return filter_hypervisor_list_response(response)
    except Exception as e:
        logger.error(f"Error retrieving hypervisor list: {e}")
        return ToolError({"error": str(e)})

VSA_TOOLS = [
    get_hypervisor_list
]