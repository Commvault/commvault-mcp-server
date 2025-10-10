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

from typing import Annotated
from fastmcp.exceptions import ToolError
from pydantic import Field

from src.cv_api_client import commvault_api_client
from src.logger import logger
from wrappers.wrappers import compute_security_score, format_report_dataset_response, transform_sla_data


def get_sla_status() -> dict:
    """
    Retrieves the SLA status from Commvault.
    Returns:
        A dictionary containing information about the SLA status.
    """
    try:
        sla_data = commvault_api_client.get("cr/reportsplusengine/datasets/getslacounts/data?cache=true&parameter.i_dashboardtype=commcell&datasource=2")
        sla_status = transform_sla_data(sla_data)
        return sla_status
    except Exception as e:
        logger.error(f"Error retrieving SLA status: {e}")
        return ToolError({"error": str(e)})
    
def get_security_posture():
    """
    Retrieves the security posture of the Commcell.
    Returns:
        A dictionary containing information about the security posture of the Commcell.
    """
    try:
        security_posture = commvault_api_client.get("Security/Dashboard")
        return security_posture
    except Exception as e:
        logger.error(f"Error retrieving security posture: {e}")
        return ToolError({"error": str(e)})

def get_security_score():
    """
    Retrieves the security score of the Commcell.
    Returns:
        A dictionary containing information about the security score of the Commcell.
    """
    try:
        security_posture = commvault_api_client.get("Security/Dashboard")
        security_score = compute_security_score(security_posture)
        response = {
            "security_score": security_score
        }
        return response
    except Exception as e:
        logger.error(f"Error retrieving security score: {e}")
        return ToolError({"error": str(e)})

def get_storage_space_utilization():
    """
    Retrieves storage space utilization or the amount of data that is in all disk or cloud libraries, and the percentage of storage space that was saved because of compression and deduplication.
    Returns:
        A dictionary containing information about the storage space utilization.
    """
    try:
        api_response = commvault_api_client.get("cr/reportsplusengine/datasets/2b366703-52e1-4775-8047-1f4cfa13d2db/data?cache=true&parameter.i_dashboardtype=commcell&orderby='date to be full'&datasource=2")
        return format_report_dataset_response(api_response)
    except Exception as e:
        logger.error(f"Error retrieving storage space utilization: {e}")
        return ToolError({"error": str(e)})

def get_commcell_details() -> dict:
    """
    Gets the details of the commcell, formatted for LLM consumption.
    """
    try:
        api_response = commvault_api_client.get("cr/reportsplusengine/datasets/a0f077a5-2dfe-4010-a957-57a24cae89a8/data")
        return format_report_dataset_response(api_response)
    except Exception as e:
        logger.error(f"Error retrieving commcell details: {e}")
        return ToolError({"error": str(e)})

def get_entity_counts() -> dict:
    """
    Gets the total count of entities, including servers, VMs, laptops, and users, within the CommCell environment.
    """
    try:
        api_response = commvault_api_client.get("/cr/reportsplusengine/datasets/d0a73c45-b06d-4358-8d7e-d55d428ba75c/data?cache=true&parameter.i_dashboardtype=commcell&datasource=2")
        return format_report_dataset_response(api_response)
    except Exception as e:
        logger.error(f"Error retrieving entity counts: {e}")
        return ToolError({"error": str(e)})
    
def create_send_logs_job_for_commcell(emailid: Annotated[str, Field(description="The email id to send logs to.")], commcell_name: Annotated[str, Field(description="The commcell name for which to send logs.")]) -> dict:
    """
    Triggers a new send logs job for the specified CommCell and sends logs to the provided email address.
    If successful, returns the job ID of the created send logs job.
    """
    try:
        data = {
            "taskInfo": {
                "task": {
                    "taskType": 1,
                    "initiatedFrom": 1,
                    "policyType": 0,
                    "taskFlags": {"disabled": False}
                },
                "subTasks": [
                    {
                        "subTask": {"subTaskType": 1, "operationType": 5010},
                        "options": {
                            "adminOpts": {
                                "sendLogFilesOption": {
                                    "actionLogsEndJobId": 0,
                                    "emailSelected": True,
                                    "jobid": 0,
                                    "tsDatabase": False,
                                    "galaxyLogs": True,
                                    "getLatestUpdates": False,
                                    "actionLogsStartJobId": 0,
                                    "computersSelected": True,
                                    "csDatabase": False,
                                    "otherDatabases": False,
                                    "crashDump": False,
                                    "isNetworkPath": False,
                                    "saveToFolderSelected": False,
                                    "notifyMe": True,
                                    "includeJobResults": False,
                                    "doNotIncludeLogs": True,
                                    "machineInformation": True,
                                    "scrubLogFiles": False,
                                    "emailSubject": f"Your CommCell Logs",
                                    "osLogs": True,
                                    "allUsersProfile": True,
                                    "splitFileSizeMB": 512,
                                    "actionLogs": False,
                                    "includeIndex": False,
                                    "databaseLogs": True,
                                    "includeDCDB": False,
                                    "collectHyperScale": False,
                                    "logFragments": False,
                                    "uploadLogsSelected": True,
                                    "useDefaultUploadOption": True,
                                    "enableChunking": True,
                                    "collectRFC": False,
                                    "collectUserAppLogs": False,
                                    "impersonateUser": {"useImpersonation": False},
                                    "clients": [
                                        {"clientId": 2, "clientName": commcell_name}
                                    ],
                                    "recipientCc": {
                                        "emailids": [emailid],
                                        "users": [],
                                        "userGroups": []
                                    },
                                    "sendLogsOnJobCompletion": False
                                }
                            }
                        }
                    }
                ]
            }
        }
        return commvault_api_client.post("createtask", data=data)
    except Exception as e:
        logger.error(f"Error creating send logs job for commcell: {e}")
        return ToolError({"error": str(e)})
    
COMMCELL_MANAGEMENT_TOOLS = [
    get_sla_status,
    get_security_posture,
    get_security_score,
    get_storage_space_utilization,
    create_send_logs_job_for_commcell,
    get_commcell_details,
    get_entity_counts
]