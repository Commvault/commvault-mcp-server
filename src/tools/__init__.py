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

"""
Tools package for Commvault MCP Server.

This package contains all tools organized by functionality categories.
"""

from .client_tools import CLIENT_MANAGEMENT_TOOLS
from .commcell_tools import COMMCELL_MANAGEMENT_TOOLS
from .job_tools import JOB_MANAGEMENT_TOOLS
from .plan_tools import PLAN_MANAGEMENT_TOOLS
from .schedule_tools import SCHEDULE_MANAGEMENT_TOOLS
from .storage_tools import STORAGE_MANAGEMENT_TOOLS
from .user_tools import USER_MANAGEMENT_TOOLS
from .vsa_tools import VSA_TOOLS

# All available tool categories
ALL_TOOL_CATEGORIES = [
    JOB_MANAGEMENT_TOOLS,
    CLIENT_MANAGEMENT_TOOLS,
    SCHEDULE_MANAGEMENT_TOOLS,
    STORAGE_MANAGEMENT_TOOLS,
    PLAN_MANAGEMENT_TOOLS,
    COMMCELL_MANAGEMENT_TOOLS,
    USER_MANAGEMENT_TOOLS,
    VSA_TOOLS
]

__all__ = [
    'CLIENT_MANAGEMENT_TOOLS',
    'COMMCELL_MANAGEMENT_TOOLS', 
    'JOB_MANAGEMENT_TOOLS',
    'PLAN_MANAGEMENT_TOOLS',
    'SCHEDULE_MANAGEMENT_TOOLS',
    'STORAGE_MANAGEMENT_TOOLS',
    'USER_MANAGEMENT_TOOLS',
    'ALL_TOOL_CATEGORIES',
    'VSA_TOOLS'
]
