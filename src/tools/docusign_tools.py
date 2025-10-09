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
from typing import Annotated, Optional, Dict, Any
from pydantic import Field
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.cv_api_client import commvault_api_client
from src.logger import logger
from src.wrappers import format_report_dataset_response
import json
from pathlib import Path


# Constants
DOCUSIGN_VAULT_NAME = "docusign-backup-vault"
DOCUSIGN_WORKFLOW_NAME = "Backup Docusign Utility"
CONFIG_DIR = Path("config")
DOCUSIGN_CONFIG_FILE = CONFIG_DIR / "docusign_config.json"
DOCUSIGN_KEY_FILE = CONFIG_DIR / "docusign_key.pem"
DOCUSIGN_WORKFLOW_XML = CONFIG_DIR / "docusign_workflow.xml"


# ============================================================================
# Helper Functions
# ============================================================================

def _load_json_config(config_path: Path) -> Dict[str, Any]:
    """
    Load and validate JSON configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Dictionary containing configuration data
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file contains invalid JSON or missing required keys
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"DocuSign configuration file not found at '{config_path}'.\n"
            f"Please create '{config_path}' with the required configuration.\n"
            f"Refer to the documentation for the proper format."
        )
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON in configuration file '{config_path}': {str(e)}\n"
            f"Please ensure the file contains valid JSON."
        )
    except Exception as e:
        raise IOError(f"Error reading configuration file '{config_path}': {str(e)}")
    
    if not isinstance(config, dict):
        raise ValueError(
            f"Configuration file '{config_path}' must contain a JSON object at the root level."
        )
    
    return config


def _read_file_safely(file_path: Path, file_description: str = "file") -> str:
    """
    Safely read a file with proper error handling.
    
    Args:
        file_path: Path to the file to read
        file_description: Human-readable description of the file for error messages
        
    Returns:
        Contents of the file as a string
        
    Raises:
        FileNotFoundError: If file doesn't exist
        IOError: If file cannot be read
    """
    if not file_path.exists():
        raise FileNotFoundError(
            f"{file_description.capitalize()} not found at '{file_path}'.\n"
            f"Please ensure the file exists and is in the correct location."
        )
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        if not content or not content.strip():
            raise ValueError(f"{file_description.capitalize()} at '{file_path}' is empty.")
        
        return content
    except Exception as e:
        if isinstance(e, (FileNotFoundError, ValueError)):
            raise
        raise IOError(f"Error reading {file_description} '{file_path}': {str(e)}")


def _validate_time_format(time_str: str) -> tuple[int, int]:
    """
    Validate and parse time string in HH:MM format.
    
    Args:
        time_str: Time string in 24-hour format (HH:MM)
        
    Returns:
        Tuple of (hour, minute)
        
    Raises:
        ValueError: If time format is invalid
    """
    try:
        parts = time_str.split(":")
        if len(parts) != 2:
            raise ValueError("Time must be in HH:MM format")
        
        hour, minute = int(parts[0]), int(parts[1])
        
        if not (0 <= hour <= 23):
            raise ValueError(f"Hour must be between 0 and 23, got {hour}")
        if not (0 <= minute <= 59):
            raise ValueError(f"Minute must be between 0 and 59, got {minute}")
        
        return hour, minute
    except (ValueError, AttributeError) as e:
        raise ValueError(
            f"Invalid time format '{time_str}'. Expected 24-hour format HH:MM (e.g., '18:00'). Error: {str(e)}"
        )


def _check_workflow_exists(
    workflow_name: Annotated[str, Field(description="Name of the workflow to check for existence.")]):
    """
    Check if a workflow exists in the Commvault environment.
    """
    try:
        workflow_list = commvault_api_client.get("Workflow")
        
        if not isinstance(workflow_list, dict):
            logger.error(f"Unexpected response type from Workflow API: {type(workflow_list)}")
            raise Exception("Failed to retrieve workflow list: Invalid API response")
        
        if "container" not in workflow_list:
            logger.warning("No workflows found in the system")
            return False
        
        for workflow in workflow_list["container"]:
            if isinstance(workflow, dict) and "entity" in workflow:
                entity = workflow["entity"]
                if entity.get("workflowName") == workflow_name:
                    workflow_id = entity.get("workflowId")
                    if workflow_id:
                        logger.info(f"Found workflow '{workflow_name}' with ID: {workflow_id}")
                        return {"workflowId": workflow_id}
        
        logger.info(f"Workflow '{workflow_name}' not found")
        return False
    except Exception as e:
        logger.error(f"Error checking workflow existence: {str(e)}")
        raise Exception(f"Failed to check if workflow '{workflow_name}' exists: {str(e)}")

def _trigger_workflow(
        workflow_name: Annotated[str, Field(description="Name of the workflow to trigger.")], 
        operation_type: Annotated[str, Field(description="Type of operation to perform (backup/restore).")],
        source_path: Optional[str] = None
    ) -> Dict[str, Any]:
    """
    Trigger a workflow with specified operation type.
    """
    if not workflow_name or not workflow_name.strip():
        raise ValueError("Workflow name cannot be empty")
    
    if not operation_type or not operation_type.strip():
        raise ValueError("Operation type cannot be empty")
    
    operation_type = operation_type.lower()
    if operation_type not in ("backup", "restore"):
        raise ValueError(f"Invalid operation type '{operation_type}'. Must be 'backup' or 'restore'")
    
    try:
        payload = {"operationType": operation_type}
        if source_path:
            payload["path"] = source_path
        
        logger.info(f"Triggering workflow '{workflow_name}' with operation '{operation_type}'")
        response = commvault_api_client.post(f"wapi/{workflow_name}", data=payload)
        
        if isinstance(response, dict) and "jobId" in response:
            job_id = response["jobId"]
            logger.info(f"Workflow triggered successfully. Job ID: {job_id}")
            return {
                "message": f"{operation_type.capitalize()} operation triggered successfully.",
                "jobId": job_id,
                "workflow": workflow_name
            }
        else:
            error_msg = response.get("errorMessage", "Unknown error") if isinstance(response, dict) else str(response)
            raise Exception(f"Workflow trigger failed: {error_msg}")
    except Exception as e:
        logger.error(f"Error triggering workflow '{workflow_name}': {str(e)}")
        raise Exception(f"Failed to trigger {operation_type} workflow: {str(e)}")

def _import_and_deploy_workflow(user_id: int, s3_endpoint: str) -> str:
    """
    Import workflow from XML file and deploy it.
    """
    try:
        # Read workflow XML file
        workflow_xml = _read_file_safely(
            DOCUSIGN_WORKFLOW_XML, 
            "DocuSign workflow XML file"
        )
        
        logger.info(f"Importing workflow from {DOCUSIGN_WORKFLOW_XML}")
        new_workflow = commvault_api_client.put("Workflow", data=workflow_xml)
        
        if not isinstance(new_workflow, dict):
            raise Exception(f"Invalid response from workflow creation API: {new_workflow}")
        
        if "workflow" not in new_workflow:
            error_msg = new_workflow.get("errorMessage", "Unknown error")
            raise Exception(f"Workflow creation failed: {error_msg}")
        
        workflow_info = new_workflow["workflow"]
        required_fields = ["GUID", "workflowName", "workflowId"]
        missing_fields = [f for f in required_fields if not workflow_info.get(f)]
        
        if missing_fields:
            raise Exception(
                f"Workflow creation response missing required fields: {', '.join(missing_fields)}"
            )
        
        workflow_id = workflow_info["workflowId"]
        workflow_name = workflow_info["workflowName"]
        
        logger.info(f"Workflow '{workflow_name}' created with ID: {workflow_id}. Deploying...")
        
        # Deploy the workflow
        deployment_result = commvault_api_client.post(
            f"Workflow/{workflow_id}/Action/Deploy?clientId=2"
        )
        
        if not isinstance(deployment_result, dict):
            raise Exception(f"Invalid deployment response: {deployment_result}")
        
        if deployment_result.get("errorMessage") != "Success":
            error_msg = deployment_result.get("errorMessage", "Unknown error")
            raise Exception(f"Workflow deployment failed: {error_msg}")
        
        logger.info(f"Workflow '{workflow_name}' deployed successfully. Updating configuration...")
        
        # Update workflow configuration
        if not _update_workflow_configuration(workflow_id, user_id, s3_endpoint):
            raise Exception("Failed to update workflow configuration")
        
        logger.info(f"Workflow '{workflow_name}' setup completed successfully")
        return workflow_name
        
    except Exception as e:
        logger.error(f"Error importing and deploying workflow: {str(e)}")
        raise Exception(f"Failed to import and deploy workflow: {str(e)}")

def _update_workflow_configuration(workflow_id: int, user_id: int, s3_endpoint: str) -> bool:
    """
    Update workflow configuration with DocuSign credentials.
    """
    try:
        # Validate workflow ID
        if not isinstance(workflow_id, int) or workflow_id <= 0:
            raise ValueError(f"Invalid workflow ID: {workflow_id}")
        
        config_json = _get_vault_config(user_id, s3_endpoint)
        
        private_key = _read_file_safely(
            DOCUSIGN_KEY_FILE,
            "DocuSign private key file"
        )
        
        # Create Config XML payload
        xml_payload = f"""<configuration>
    <configJson><![CDATA[{config_json}]]></configJson>
    <privateKey><![CDATA[{private_key}]]></privateKey>
</configuration>"""
        
        logger.info(f"Updating workflow configuration for workflow ID: {workflow_id}")
        response = commvault_api_client.post(
            f"cr/apps/configform/{workflow_id}", 
            data=xml_payload
        )
        
        if not isinstance(response, dict):
            raise Exception(f"Invalid response from configuration update: {response}")
        
        error_code = response.get("errorCode")
        if error_code == 0:
            logger.info(f"Workflow configuration updated successfully for workflow ID: {workflow_id}")
            return True
        else:
            error_msg = response.get("errorMessage", "Unknown error")
            raise Exception(
                f"Configuration update failed with error code {error_code}: {error_msg}"
            )
    
    except Exception as e:
        logger.error(f"Error updating workflow configuration: {str(e)}")
        if isinstance(e, (FileNotFoundError, ValueError)):
            raise
        raise Exception(f"Failed to update workflow configuration: {str(e)}")

def _check_docusign_backup_vault_exists() -> bool:
    """
    Check if the DocuSign backup vault exists.
    """
    try:
        response = commvault_api_client.get("V4/cvS3Stores?hardRefresh=true&start=0&limit=100&fl=overview")
        
        # Check if CVS3Stores exists in response
        if not response or "CVS3Stores" not in response:
            return False
        
        # Check if any vault has the name matching DOCUSIGN_VAULT_NAME
        for store in response["CVS3Stores"]:
            if store.get("name") == DOCUSIGN_VAULT_NAME:
                return True
        
        return False
    except Exception as e:
        logger.error(f"Error checking if DocuSign backup vault exists: {str(e)}")
        raise Exception(f"Failed to check if DocuSign backup vault exists: {str(e)}")

def _create_docusign_backup_vault(plan_id: int, plan_name: str, user_id: int, user_name: str) -> bool:
    """
    Create a backup vault for DocuSign.
    """
    payload = {
        "bucket": {
            "clientName": DOCUSIGN_VAULT_NAME
        },
        "plan": {
            "planId": plan_id,
            "planName": plan_name
        },
        "owners": [
            {
                "userId": user_id,
                "userName": user_name
            }
        ]
    }

    try:
        logger.info(f"Creating backup vault '{DOCUSIGN_VAULT_NAME}'")
        response = commvault_api_client.post("v4/cvs3Store", data=payload)
        
        if not isinstance(response, dict):
            raise Exception(f"Invalid response from vault creation API: {response}")
        
        error_code = response.get("response", {}).get("errorCode", 1)
        if error_code == 0:
            logger.info(f"Backup vault '{DOCUSIGN_VAULT_NAME}' created successfully")
            return True
        else:
            error_msg = response.get("response", {}).get("errorMessage", "Unknown error")
            raise Exception(f"Vault creation failed with error code {error_code}: {error_msg}")
    
    except Exception as e:
        logger.error(f"Error creating backup vault: {str(e)}")
        raise Exception(f"Failed to create backup vault '{DOCUSIGN_VAULT_NAME}': {str(e)}")
    
def _get_vault_config(user_id: int, s3_endpoint: str) -> Dict[str, Any]:
    """
    Get S3 access keys for the vault and update configuration file.
    """
    try:
        config = _load_json_config(DOCUSIGN_CONFIG_FILE)

        required_docusign_keys = ["integrationKey", "userId", "authServer", "scopes", "basePath"]
        if "docusign" not in config or not all(k in config["docusign"] for k in required_docusign_keys):
            raise Exception(
                f"Docusign configuration section not found or missing required keys in {DOCUSIGN_CONFIG_FILE}. "
                "Please refer to the documentation to configure the Docusign integration."
            )
        
        logger.info(f"Generating S3 access keys for user ID: {user_id}")
        response = commvault_api_client.put(f"v4/user/{user_id}/s3accesskey")
        
        if not isinstance(response, dict):
            raise Exception(f"Invalid response from S3 access key API: {response}")
        
        # Check for successful response
        error_code = response.get("response", {}).get("errorCode")
        if error_code != 200:
            error_msg = response.get("response", {}).get("errorMessage", "Unknown error")
            raise Exception(f"Failed to generate S3 access keys (error code {error_code}): {error_msg}")
        
        # Validate required fields
        access_key_id = response.get("accessKeyID")
        secret_access_key = response.get("secretAccessKey")
        
        if not access_key_id or not secret_access_key:
            raise Exception(
                "Response missing required fields: accessKeyID or secretAccessKey"
            )

        config["aws"] = {
            "endpoint": s3_endpoint,
            "bucket": DOCUSIGN_VAULT_NAME,
            "region": "us-east-1",
            "accessKeyId": access_key_id,
            "secretAccessKey": secret_access_key
        }

        config_json = json.dumps(config, indent=2)
        with open(DOCUSIGN_CONFIG_FILE, "w") as f:
            f.write(config_json)

        return config_json
        
    except Exception as e:
        logger.error(f"Error getting and vault config: {str(e)}")
        raise Exception(f"Failed to get and vault config: {str(e)}")

def setup_docusign_backup_vault(
    s3_endpoint: Annotated[str, Field(description="The endpoint of the Commvault S3 vault to use for the backup vault.")],
    plan_id: Annotated[int, Field(description="The ID of the plan to use for the backup vault.")], 
    plan_name: Annotated[str, Field(description="The name of the plan to use for the backup vault.")], 
    user_id: Annotated[int, Field(description="The ID of the user to use for the backup vault.")], 
    user_name: Annotated[str, Field(description="The name of the user to use for the backup vault.")]) -> Dict[str, Any]:
    """
    Use this tool to configure a backup vault and workflow for DocuSign documents.
    Fetch user_id and user_name using get_my_user_info, and plan_id and plan_name using get_plan_list. Ask the user to provide the S3 endpoint and choose the plan from the list of s3 vault compatible plans.

    IMPORTANT: Only run this setup if the user explicitly requests to initialize or configure the DocuSign backup vault.
    If not requested, assume the vault is already set up.
    Always confirm with the user before proceeding, as this may overwrite existing settings.
    """
    try:
        logger.info("Starting DocuSign backup vault setup")
        vault_exists, workflow_exists = _check_docusign_backup_vault_exists(), _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)

        if vault_exists and workflow_exists:
            logger.info("DocuSign backup vault and workflow are already set up")
            return {"message": f"Docusign backup is already set up. You can trigger or schedule backups."}

        if not vault_exists:
            _create_docusign_backup_vault(plan_id, plan_name, user_id, user_name)
        else:
            logger.info(f"DocuSign backup vault '{DOCUSIGN_VAULT_NAME}' already exists")
        
        # Ensure workflow exists
        logger.info(f"Checking if workflow '{DOCUSIGN_WORKFLOW_NAME}' exists")
        if not workflow_exists:
            logger.info(f"Workflow '{DOCUSIGN_WORKFLOW_NAME}' not found. Configuring...")
            workflow_name = _import_and_deploy_workflow(user_id, s3_endpoint)
            logger.info(f"Workflow '{workflow_name}' created and deployed successfully")
        else:
            logger.info(f"Workflow '{DOCUSIGN_WORKFLOW_NAME}' already exists")
        
        logger.info("DocuSign backup vault setup completed successfully")
        return {"message": f"Docusign backup is set up successfully. You can now trigger or schedule backups."}
    except Exception as e:
        logger.error(f"DocuSign backup vault setup failed: {str(e)}")
        raise Exception(f"Failed to setup DocuSign backup vault: {str(e)}")

def trigger_docusign_backup() -> Dict[str, Any]:
    """
    Triggers a workflow to backup Docusign documents. If the backup workflow is not configured, an error will be raised.
    """
    try:
        logger.info(f"Attempting to trigger DocuSign backup using workflow '{DOCUSIGN_WORKFLOW_NAME}'")
        vault_exists, workflow_exists = _check_docusign_backup_vault_exists(), _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
        if not vault_exists or not workflow_exists:
            raise Exception(
                f"DocuSign backup vault or workflow is not configured. To configure, please run setup_docusign_backup_vault."
            )

        result = _trigger_workflow(DOCUSIGN_WORKFLOW_NAME, operation_type="backup")
        logger.info(f"DocuSign backup triggered successfully: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error triggering DocuSign backup: {str(e)}")
        raise ToolError(f"Failed to trigger DocuSign backup: {str(e)}")

def schedule_docusign_backup(
    schedule_type: Annotated[str, Field(description="Type of schedule to create. Options are 'daily' or 'weekly'. Default is 'daily'.")] = "daily",
    time: Annotated[str, Field(description="Time to run the backup job. 24 Hour Format: HH:MM")] = "18:00",
    day_of_week: Annotated[str, Field(description="Day of the week to run the backup job if the schedule type is 'weekly'. Default is 'Sunday'. Title case is used for days.")] = "Sunday"
) -> Dict[str, Any]:
    """
    Schedules docusign backup workflow to run daily or weekly. Ask the user if they want to schedule it before running.
    """
    try:
        # Validate schedule type
        schedule_type = schedule_type.lower()
        if schedule_type not in ("daily", "weekly"):
            raise ValueError("Only 'daily' and 'weekly' schedule types are supported.")
        
        # Validate and parse time
        hour, minute = _validate_time_format(time)
        trigger_time = hour * 3600 + minute * 60
        
        # Validate day of week for weekly schedules
        valid_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        if schedule_type == "weekly" and day_of_week not in valid_days:
            raise ValueError(
                f"Invalid day_of_week '{day_of_week}'. Must be one of: {', '.join(valid_days)}"
            )
        
        logger.info(f"Creating {schedule_type} schedule for DocuSign backup at {time}")
        
        # Check if workflow exists
        workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
        if not workflow_entity or "workflowId" not in workflow_entity:
            raise Exception(
                f"DocuSign backup workflow '{DOCUSIGN_WORKFLOW_NAME}' not found. "
                "Please run setup_docusign_backup_vault first."
            )
        
        workflow_id = workflow_entity["workflowId"]
        
        # Configure schedule pattern
        if schedule_type == "daily":
            freq_type = 4  # Daily
            freq_interval = 1
            days_to_run = None
        else:
            freq_type = 8  # Weekly
            freq_interval = 1
            days_to_run = {d: d == day_of_week for d in valid_days}

        schedule_name = f"{DOCUSIGN_WORKFLOW_NAME}-{schedule_type}-schedule"

        pattern = {
            "name": schedule_name,
            "freq_type": freq_type,
            "freq_interval": freq_interval,
            "freq_recurrence_factor": 1,
            "active_start_time": trigger_time,
            "timeZone": {
                "TimeZoneID": 1000
            },
            "description": f"{schedule_type.capitalize()} schedule for {DOCUSIGN_WORKFLOW_NAME}"
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
                            "workflowJobOptions": "<inputs><operationType>Backup</operationType></inputs>"
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
        
        logger.info(f"Creating schedule task for workflow ID: {workflow_id}")
        response = commvault_api_client.post("CreateTask", data=payload)
        
        if not isinstance(response, dict):
            raise Exception(f"Invalid response from CreateTask API: {response}")
        
        if "taskId" in response:
            task_id = response["taskId"]
            logger.info(f"Schedule created successfully. Task ID: {task_id}")
            return {
                "status": "success",
                "message": f"{schedule_type.capitalize()} schedule created successfully at {time}.",
                "taskId": task_id,
                "scheduleName": schedule_name
            }
        else:
            error_msg = response.get("errorMessage", "Unknown error")
            raise Exception(f"Schedule creation failed: {error_msg}")
    
    except Exception as e:
        logger.error(f"Error scheduling DocuSign backup: {str(e)}")
        raise ToolError(f"Failed to schedule DocuSign backup: {str(e)}")

def get_docusign_jobs(
    jobLookupWindow: Annotated[int, Field(description="The time window in seconds to look up for jobs jobs. For example, 86400 for the last 24 hours.")]=86400,
    limit: Annotated[int, Field(description="The maximum number of jobs to return. Default is 50.")] = 50,
    offset: Annotated[int, Field(description="The offset for pagination.")] = 0
) -> Dict[str, Any]:
    """
    Retrieves the list of DocuSign backup/restore jobs.
    
    Args:
        jobLookupWindow: Time window in seconds to look back for jobs
        limit: Maximum number of jobs to return
        offset: Pagination offset
        
    Returns:
        Dictionary with job records
        
    Raises:
        ToolError: If job retrieval fails
    """
    try:
        # Validate parameters
        if jobLookupWindow < 0:
            raise ValueError(f"jobLookupWindow must be positive, got {jobLookupWindow}")
        if limit < 1 or limit > 1000:
            raise ValueError(f"limit must be between 1 and 1000, got {limit}")
        if offset < 0:
            raise ValueError(f"offset must be non-negative, got {offset}")
        
        logger.info(
            f"Retrieving DocuSign jobs (window: {jobLookupWindow}s, limit: {limit}, offset: {offset})"
        )
        
        workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
        if not workflow_entity or "workflowId" not in workflow_entity:
            raise Exception(
                f"DocuSign backup workflow '{DOCUSIGN_WORKFLOW_NAME}' not found. "
                "Please run setup_docusign_backup_vault first."
            )

        workflow_id = workflow_entity["workflowId"]
        
        # Build API endpoint
        endpoint = (
            f"cr/reportsplusengine/datasets/e8ee6af4-58d8-4444-abae-3c096e5628a4/data"
            f"?limit={limit}&offset={offset}&orderby=%5BjobEndTime%5DDESC"
            f"&parameter.hideAdminJobs=0&parameter.jobCategory=3"
            f"&parameter.showOnlyLaptopJobs=0"
            f"&parameter.completedJobLookupTime={jobLookupWindow}"
            f"&parameter.workFlows={workflow_id}&parameter.jobTypes=90"
        )
        
        response = commvault_api_client.get(endpoint)
        
        if not isinstance(response, dict):
            raise Exception(f"Invalid response from jobs API: {response}")
        
        formatted_response = format_report_dataset_response(response)
        
        # Filter the response to keep only important fields for LLM
        if isinstance(formatted_response, dict) and "records" in formatted_response:
            important_fields = [
                "jobId",
                "status",
                "jobStartTime",
                "jobEndTime", 
                "pendingReason"
            ]
            
            filtered_records = []
            for record in formatted_response["records"]:
                if not isinstance(record, dict):
                    logger.warning(f"Skipping invalid record: {record}")
                    continue
                
                # Extract important fields
                filtered_record = {
                    field: record.get(field) 
                    for field in important_fields 
                    if record.get(field) is not None
                }
                
                # Get operation type from job details
                job_id = record.get("jobId")
                if job_id:
                    try:
                        job_detail = commvault_api_client.post("JobDetails", data={"jobId": job_id})
                        if isinstance(job_detail, dict):
                            workflow_inputs_xml = (
                                job_detail.get("job", {})
                                .get("jobDetail", {})
                                .get("detailInfo", {})
                                .get("workflowInputsXml", "")
                            )
                            if workflow_inputs_xml:
                                workflow_inputs_lower = workflow_inputs_xml.lower()
                                if "restore" in workflow_inputs_lower:
                                    filtered_record["operationType"] = "Restore"
                                elif "backup" in workflow_inputs_lower:
                                    filtered_record["operationType"] = "Backup"
                    except Exception as e:
                        logger.warning(f"Failed to get operation type for job {job_id}: {str(e)}")
                
                filtered_records.append(filtered_record)
            
            formatted_response["records"] = filtered_records
            logger.info(f"Retrieved {len(filtered_records)} DocuSign jobs")
        else:
            logger.warning("No records found in jobs response")
        
        return formatted_response
    
    except Exception as e:
        logger.error(f"Error retrieving DocuSign jobs: {str(e)}")
        raise ToolError(f"Failed to retrieve DocuSign jobs: {str(e)}")

def list_backedup_docusign_envelopes(
    date: Annotated[str, Field(description="Date in YYYY-MM-DD format to list backups on specific date. Default is empty string which lists all backups.")] = ""
) -> Dict[str, Any]:
    """
    Lists all backed up DocuSign envelopes and files. This can be used to fetch the paths of all backed up envelopes and their files.
    """
    def _get_s3_client(config: Dict[str, Any]):
        """Create and return an S3 client from configuration."""
        try:
            # Validate required config keys
            required_keys = ['endpoint', 'region', 'accessKeyId', 'secretAccessKey']
            aws_config = config.get('aws', {})
            
            missing_keys = [key for key in required_keys if not aws_config.get(key)]
            if missing_keys:
                raise ValueError(
                    f"Missing required S3 configuration keys: {', '.join(missing_keys)}. "
                    f"Please check {DOCUSIGN_CONFIG_FILE}."
                )
            
            return boto3.client(
                's3',
                endpoint_url=aws_config['endpoint'],
                region_name=aws_config['region'],
                aws_access_key_id=aws_config['accessKeyId'],
                aws_secret_access_key=aws_config['secretAccessKey'],
                config=boto3.session.Config(s3={'addressing_style': 'path'})
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Configuration error: {str(e)}")
            raise
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to create S3 client: {str(e)}")
            raise Exception(f"Failed to create S3 client: {str(e)}")

    def _process_envelope(envelope_folder: str, paginator) -> Dict[str, Any]:
        """
        Process a single envelope and return envelope data.
        
        Args:
            envelope_folder: Path to envelope folder in S3
            paginator: S3 paginator instance
            
        Returns:
            Dictionary with envelope ID and documents
        """
        try:
            envelope_id = envelope_folder.split("/")[-1]
            logger.debug(f"Processing envelope: {envelope_id}")
            
            document_files = []
            envelope_params = {
                "Bucket": DOCUSIGN_VAULT_NAME,
                "Prefix": envelope_folder + "/"
            }
            
            for envelope_page in paginator.paginate(**envelope_params):
                if "Contents" in envelope_page:
                    for obj in envelope_page["Contents"]:
                        key = obj.get("Key", "")
                        if key and not key.endswith("/"):
                            # Extract filename from full path
                            try:
                                file_name = key.split(envelope_folder + "/", 1)[1]
                                # Skip metadata files
                                if file_name not in ("metadata.json", "Summary"):
                                    document_files.append(file_name)
                                logger.debug(f"   |___ {key}")
                            except IndexError:
                                logger.warning(f"Could not extract filename from key: {key}")
            
            envelope_entry = {"id": envelope_id}
            if document_files:
                # Limit the number of documents shown for large envelopes
                if len(document_files) <= 10:
                    envelope_entry["docs"] = document_files
                else:
                    envelope_entry["docs"] = f"{len(document_files)} documents"
            
            return envelope_entry
        except Exception as e:
            logger.error(f"Error processing envelope {envelope_folder}: {str(e)}")
            return {"id": envelope_folder.split("/")[-1], "error": str(e)}

    def _process_date_folder(folder: str, paginator) -> Optional[Dict[str, Any]]:
        """
        Process a date folder and return date entry with envelopes.
        
        Args:
            folder: Path to date folder in S3
            paginator: S3 paginator instance
            
        Returns:
            Dictionary with date and envelopes, or None if no envelopes found
        """
        try:
            folder_name = folder.split("/")[-1]
            logger.info(f"Processing date folder: {folder_name}")
            
            date_entry = {
                "date": folder_name,
                "envelopes": []
            }
            
            date_params = {
                "Bucket": DOCUSIGN_VAULT_NAME,
                "Prefix": folder + "/",
                "Delimiter": "/"
            }
            
            for date_page in paginator.paginate(**date_params):
                if "CommonPrefixes" in date_page:
                    for envelope_cp in date_page["CommonPrefixes"]:
                        envelope_folder = envelope_cp.get("Prefix", "").rstrip("/")
                        if envelope_folder:
                            envelope_entry = _process_envelope(envelope_folder, paginator)
                            date_entry["envelopes"].append(envelope_entry)
            
            return date_entry if date_entry["envelopes"] else None
        except Exception as e:
            logger.error(f"Error processing date folder {folder}: {str(e)}")
            return None
    
    try:
        logger.info("----- Starting List Backup -----")
        
        # Validate date format if provided
        if date:
            date = date.strip()
            if date:
                # Basic date format validation (YYYY-MM-DD)
                import re
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
                    raise ValueError(
                        f"Invalid date format '{date}'. Expected YYYY-MM-DD format (e.g., '2024-01-15')"
                    )
                logger.info(f"Filtering backups for date: {date}")

        # Load configuration
        config = _load_json_config(DOCUSIGN_CONFIG_FILE)
        
        # Validate AWS configuration exists
        if 'aws' not in config:
            raise ValueError(
                f"AWS configuration section not found in {DOCUSIGN_CONFIG_FILE}. "
                "Please run setup to configure AWS credentials."
            )
        
        logger.debug(f"Configuration loaded successfully from {DOCUSIGN_CONFIG_FILE}")
        
        # Create S3 client
        s3 = _get_s3_client(config)
        paginator = s3.get_paginator("list_objects_v2")

        backup_data = {"backups": []}

        if date:
            # When date is specified, process that specific date
            if not date.endswith("/"):
                date += "/"
            
            logger.info(f"Listing backup for specific date: [{date}], bucket: [{DOCUSIGN_VAULT_NAME}]")
            
            date_params = {
                "Bucket": DOCUSIGN_VAULT_NAME,
                "Prefix": date,
                "Delimiter": "/"
            }
            
            date_entry = {
                "date": date.rstrip("/"),
                "envelopes": []
            }
            
            try:
                for date_page in paginator.paginate(**date_params):
                    if "CommonPrefixes" in date_page:
                        for envelope_cp in date_page["CommonPrefixes"]:
                            envelope_folder = envelope_cp.get("Prefix", "").rstrip("/")
                            if envelope_folder:
                                envelope_entry = _process_envelope(envelope_folder, paginator)
                                date_entry["envelopes"].append(envelope_entry)
            except (BotoCoreError, ClientError) as e:
                logger.error(f"S3 error while listing date folder '{date}': {str(e)}")
                raise Exception(f"Failed to list backups for date '{date}': {str(e)}")
            
            if date_entry["envelopes"]:
                backup_data["backups"].append(date_entry)
        else:
            # When no date specified, list all dates
            operation_params = {
                "Bucket": DOCUSIGN_VAULT_NAME,
                "Prefix": "",
                "Delimiter": "/"
            }

            logger.info(f"Listing all backup folders, bucket: [{DOCUSIGN_VAULT_NAME}]")

            try:
                for page in paginator.paginate(**operation_params):
                    if "CommonPrefixes" in page:
                        for cp in page["CommonPrefixes"]:
                            folder = cp.get("Prefix", "").rstrip("/")
                            if not folder:
                                continue
                                
                            folder_name = folder.split("/")[-1]
                            
                            # Only consider date folders (skip docusign-backup folder)
                            if folder_name != "docusign-backup":
                                date_entry = _process_date_folder(folder, paginator)
                                if date_entry:
                                    backup_data["backups"].append(date_entry)
            except (BotoCoreError, ClientError) as e:
                logger.error(f"S3 error while listing backup folders: {str(e)}")
                raise Exception(f"Failed to list backup folders: {str(e)}")

        total_dates = len(backup_data["backups"])
        total_envelopes = sum(len(date_data["envelopes"]) for date_data in backup_data["backups"])
        backup_data["summary"] = f"{total_dates} dates, {total_envelopes} envelopes"

        if total_dates == 0:
            logger.info("No matching backups found.")
            backup_data["message"] = "No backups found"
        else:
            logger.info(f"Found {total_dates} backup dates with {total_envelopes} envelopes")

        logger.info("----- END List Backup -----")
        logger.debug(json.dumps(backup_data, indent=2))
        return backup_data
    
    except Exception as e:
        logger.error(f"Error listing backed up envelopes: {str(e)}")
        raise ToolError(f"Failed to list backed up DocuSign envelopes: {str(e)}")

def recover_docusign_envelope(
    backup_date: Annotated[str, Field(description="Date in YYYY-MM-DD format when the envelope was backed up.")],
    envelope_id: Annotated[str, Field(description="The ID of the envelope to recover.")]
) -> Dict[str, Any]:
    """
    Trigger the restoration of a DocuSign envelope from backup. 
    Restored files will be saved to <commvault_temp_directory_path>/docusign-restores/<backup_date>/<envelope_id>. 
    Please update the backup_date and envelope_id before responding. Temp directory path can be left as placeholder, assume user knows it.
    """
    try:
        # Validate parameters
        if not backup_date or not backup_date.strip():
            raise ValueError("backup_date cannot be empty")
        
        if not envelope_id or not envelope_id.strip():
            raise ValueError("envelope_id cannot be empty")
        
        # Validate date format
        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', backup_date):
            raise ValueError(
                f"Invalid backup_date format '{backup_date}'. Expected YYYY-MM-DD format (e.g., '2024-01-15')"
            )
        
        logger.info(f"Attempting to recover DocuSign envelope '{envelope_id}' from date '{backup_date}'")
        
        workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)

        if not workflow_entity:
            raise Exception(
                f"DocuSign backup workflow '{DOCUSIGN_WORKFLOW_NAME}' is not configured. "
                "Please run setup_docusign_backup_vault first to configure the backup."
            )

        source_path = f"{backup_date}/{envelope_id}"
        result = _trigger_workflow(
            DOCUSIGN_WORKFLOW_NAME, 
            operation_type="restore", 
            source_path=source_path
        )
        
        logger.info(f"DocuSign envelope restore triggered successfully for '{envelope_id}'")
        return result
        
    except Exception as e:
        logger.error(f"Error recovering DocuSign envelope: {str(e)}")
        raise ToolError(f"Failed to recover DocuSign envelope: {str(e)}")
    

DOCUSIGN_TOOLS = [
    setup_docusign_backup_vault,
    trigger_docusign_backup,
    schedule_docusign_backup,
    list_backedup_docusign_envelopes,
    recover_docusign_envelope,
    get_docusign_jobs
]
