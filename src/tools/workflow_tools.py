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
from src.wrappers import format_report_dataset_response


TEST_WORKFLOW_XML = """<Workflow_WorkflowDefinition outputs="&lt;outputs/&gt;" inputs="&lt;inputs/&gt;" interactive="0" description="" manualPercentageComplete="0" apiMode="0" executeOnWeb="0" variables="&lt;variables/&gt;" revision="$Revision: $" modTime="1755581343" uniqueGuid="99f2e0bc-0d71-4a55-979e-6d4686336c45" name="mcp-test" config="&lt;configuration/&gt;"><schema><outputs className="" type="" name="outputs" /><variables className="" type="" name="variables" /><inputs className="" type="" name="inputs" /><config className="" type="" name="configuration" /></schema><Start displayName="Start" description="" continueOnFailure="0" namespaceUri="commvault.cte.workflow.activities" commented="0" height="40" created="1755580554903" breakpoint="0" uniqueName="Start_1" skipAttempt="0" name="Start" width="60" x="45" y="41" /><onStart /><onComplete /></Workflow_WorkflowDefinition>"""

def _check_workflow_exists(
    workflow_name: Annotated[str, Field(description="Name of the workflow to check for existence.")],
):
    workflow_list = commvault_api_client.get("Workflow")
    if isinstance(workflow_list, dict) and "container" in workflow_list:
        for workflow in workflow_list["container"]:
            if isinstance(workflow, dict) and "entity" in workflow:
                entity = workflow["entity"]
                if entity.get("workflowName") == workflow_name:
                    return {"workflowId": entity.get("workflowId")}
    return None

def _trigger_workflow(workflow_name: Annotated[str, Field(description="Name of the workflow to trigger.")]):
    response = commvault_api_client.post(f"wapi/{workflow_name}")
    if isinstance(response, dict) and "jobId" in response:
        return {"message": f"Backup triggered successfully. Job ID: {response['jobId']}"}
    else:
        return response

def _import_and_deploy_workflow():
    new_workflow = commvault_api_client.put("Workflow", data=TEST_WORKFLOW_XML)
    
    if not isinstance(new_workflow, dict) or "workflow" not in new_workflow:
        return {"error": "Workflow creation failed"}
    
    workflow_info = new_workflow["workflow"]
    required_fields = ["GUID", "workflowName", "workflowId"]
    
    if all(workflow_info.get(field) for field in required_fields):
        deployment_result = commvault_api_client.post(f"Workflow/{workflow_info['workflowId']}/Action/Deploy?clientId=2")
        if (isinstance(deployment_result, dict) and deployment_result.get("errorMessage") == "Success"):
            return workflow_info["workflowName"]
    
    return {"error": "Workflow creation failed"}


def trigger_docusign_backup_workflow(
    docusign_api_key: Annotated[str, Field(description="API key for Docusign integration.")],
) -> dict:
    """
    Triggers a workflow to backup Docusign documents.
    """
    try:
        workflow_name = "mcp-test"
        workflow_entity = _check_workflow_exists(workflow_name)
        
        if not workflow_entity:
            workflow_name = _import_and_deploy_workflow()
            if isinstance(workflow_name, dict) and "error" in workflow_name:
                raise Exception(workflow_name["error"])
        
        return _trigger_workflow(workflow_name)
    except Exception as e:
        logger.error(f"Error triggering workflow {workflow_name}: {e}")
        return ToolError({"error": str(e)})

def schedule_workflow(
    workflow_name: Annotated[str, Field(description="Name of the workflow to schedule.")],
    schedule_type: Annotated[str, Field(description="Type of schedule to apply. Valid values are 'daily' / 'weekly'.")],
):
    if schedule_type not in ("daily", "weekly"):
        return {"error": "Only 'daily' and 'weekly' schedule types are supported."}
    
    workflow_entity = _check_workflow_exists(workflow_name)
    if workflow_entity and "workflowId" in workflow_entity:
        workflow_id = workflow_entity["workflowId"]
    else:
        return {"error": "Workflow not found."}

    if schedule_type == "daily":
        freq_type = 4  # Daily
        freq_interval = 1
        days_to_run = None
    else:
        freq_type = 8  # Weekly
        freq_interval = 127  # All days
        days_to_run = {
            "Monday": True,
            "Tuesday": True,
            "Wednesday": True,
            "Thursday": True,
            "Friday": True,
            "Saturday": True,
            "Sunday": True
        }

    schedule_name = f"{workflow_name}-{schedule_type}-schedule"

    pattern = {
        "name": schedule_name,
        "freq_type": freq_type,
        "freq_interval": freq_interval,
        "freq_recurrence_factor": 1,
        "active_start_time": 75600,
        "timeZone": {
            "TimeZoneID": 1000
        },
        "description": f"{schedule_type} schedule for {workflow_name}"
    }
    if days_to_run:
        pattern["daysToRun"] = days_to_run

    payload = {
        "taskInfo": {
            "subTasks": [
                {
                    "subTask": {
                        "subTaskName": schedule_name,
                        "subTaskType": 1,
                        "operationType": 2001,
                        "flags": 0
                    },
                    "pattern": pattern,
                    "options": {
                        "workflowJobOptions": "<inputs></inputs>"
                    }
                }
            ],
            "task": {
                "taskType": 2,
                "initiatedFrom": 1,
                "policyType": 0
            },
            "associations": [
                {
                    "workflowId": workflow_id
                }
            ]
        }
    }
    response = commvault_api_client.post("CreateTask", data=payload)
    return response

def get_workflow_jobs(
    workflow_name: Annotated[str, Field(description="Name of the workflow to get jobs for.")],
    jobLookupWindow: Annotated[int, Field(description="The time window in seconds to look up for jobs jobs. For example, 86400 for the last 24 hours.")]=86400,
    limit: Annotated[int, Field(description="The maximum number of jobs to return. Default is 50.")] = 50,
    offset: Annotated[int, Field(description="The offset for pagination.")] = 0
):
    workflow_entity = _check_workflow_exists(workflow_name)
    if not workflow_entity:
        return {"error": "Workflow not found."}

    workflow_id = workflow_entity["workflowId"]
    endpoint = f"cr/reportsplusengine/datasets/e8ee6af4-58d8-4444-abae-3c096e5628a4/data?limit={limit}&offset={offset}&orderby=%5BjobEndTime%5DDESC&parameter.hideAdminJobs=0&parameter.jobCategory=3&parameter.showOnlyLaptopJobs=0&parameter.statusList%5B%5D=Completed%2C%22%22Completed+w%2F+one+or+more+errors%22%22%2C%22%22Completed+w%2F+one+or+more+warnings%22%22&parameter.completedJobLookupTime={jobLookupWindow}&parameter.workFlows={workflow_id}&parameter.jobTypes=90"
    response = commvault_api_client.get(endpoint)
    formatted_response = format_report_dataset_response(response)
    
    # Filter the response to keep only important fields for LLM
    if isinstance(formatted_response, dict) and "records" in formatted_response:
        important_fields = [
            "jobId",
            "status",
            "percentComplete",
            "jobStartTime",
            "jobEndTime", 
            "jobElapsedTime",
            "pendingReason"
        ]
        
        filtered_records = []
        for record in formatted_response["records"]:
            filtered_record = {field: record.get(field) for field in important_fields if record.get(field) is not None}
            filtered_records.append(filtered_record)
        
        formatted_response["records"] = filtered_records
    
    return formatted_response

WORKFLOW_TOOLS = [
    trigger_docusign_backup_workflow,
    schedule_workflow,
    get_workflow_jobs
]