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
import boto3

from src.cv_api_client import commvault_api_client
from src.logger import logger
from src.wrappers import format_report_dataset_response
import os
import json


DOCUSIGN_VAULT_NAME = "delete-chandan-test3"
DOCUSIGN_WORKFLOW_NAME = "Backup Docusign Utility"


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

def _trigger_workflow(
        workflow_name: Annotated[str, Field(description="Name of the workflow to trigger.")], 
        operation_type: Annotated[str, Field(description="Type of operation to perform (backup/restore).")],
        source_path: Annotated[str, Field(description="Source path for the restore operation.")]=None
    ):
    response = commvault_api_client.post(f"wapi/{workflow_name}", data={"operationType": operation_type, "path": source_path})
    if isinstance(response, dict) and "jobId" in response:
        return {"message": f"{operation_type} triggered successfully. Job ID: {response['jobId']}"}
    else:
        return response

def _import_and_deploy_workflow():
    workflow_xml_path = "config/docusign_workflow.xml"
    with open(workflow_xml_path, "r", encoding="utf-8") as xml_file:
        workflow_xml = xml_file.read()

    new_workflow = commvault_api_client.put("Workflow", data=workflow_xml)

    if not isinstance(new_workflow, dict) or "workflow" not in new_workflow:
        return {"error": "Workflow creation failed"}
    
    workflow_info = new_workflow["workflow"]
    required_fields = ["GUID", "workflowName", "workflowId"]
    
    if all(workflow_info.get(field) for field in required_fields):
        deployment_result = commvault_api_client.post(f"Workflow/{workflow_info['workflowId']}/Action/Deploy?clientId=2")
        if (isinstance(deployment_result, dict) and deployment_result.get("errorMessage") == "Success"):
            if _update_workflow_configuration(workflow_info["workflowId"]):
                return workflow_info["workflowName"]
    
    return {"error": "Workflow creation failed"}

def _update_workflow_configuration(workflow_id: Annotated[int, Field(description="ID of the workflow to update.")]):
    config_path = "config/docusign_config.json"
    private_key_path = "config/docusign_key.pem"
    if not os.path.isfile(config_path) or not os.path.isfile(private_key_path):
        raise Exception("Docusign Configuration file not found. Please add a docusign_config.json and docusign_key.pem file in config folder using the format specified in the documentation.")

    with open(config_path, "r", encoding="utf-8") as config_file:
        config_json = config_file.read()

    with open(private_key_path, "r", encoding="utf-8") as private_key_file:
        private_key = private_key_file.read()

    xml_payload = f"""
    <configuration>
        <configJson><![CDATA[{config_json}]]></configJson>
        <privateKey><![CDATA[{private_key}]]></privateKey>
    </configuration>
    """
    xml_payload = xml_payload.strip()

    response = commvault_api_client.post(f"cr/apps/configform/{workflow_id}", data=xml_payload)
    if isinstance(response, dict) and response.get("errorCode") == 0:
        return True
    else:
        return False
    
def _ensure_docusign_workflow_exists():
    workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
    
    if not workflow_entity:
        workflow_name = _import_and_deploy_workflow()
        if isinstance(workflow_name, dict) and "error" in workflow_name:
            raise Exception(workflow_name["error"])
    
    return True

def _create_docusign_backup_vault():
    payload = {
        "bucket": {
            "clientName": DOCUSIGN_VAULT_NAME
        },
        "plan": {
            "planId": 3,
            "planName": "aravind-sp"
        },
        "owners": [
            {
            "userId": 4,
            "userName": "admin"
            }
        ]
    }

    response = commvault_api_client.post(f"v4/cvs3Store", data=payload)

    if isinstance(response, dict) and response.get("response", {}).get("errorCode", 1) == 0:
        return True
    else:
        raise Exception(f"Failed to create backup vault: {response}")
    
def _get_and_set_vault_keys():
    response = commvault_api_client.put(f"v4/user/4/s3accesskey")
    if (
        isinstance(response, dict)
        and response.get("response", {}).get("errorCode") == 200
        and "accessKeyID" in response
        and "secretAccessKey" in response
    ):
        config_path = "config/docusign_config.json"
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            raise Exception("DocuSign configuration file not found. Please ensure config/docusign_config.json exists.")
        except json.JSONDecodeError:
            raise Exception("Invalid JSON in DocuSign configuration file.")
        
        if "aws" not in config:
            config["aws"] = {}
        
        config["aws"]["bucket"] = DOCUSIGN_VAULT_NAME
        config["aws"]["accessKeyId"] = response["accessKeyID"]
        config["aws"]["secretAccessKey"] = response["secretAccessKey"]
        
        try:
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=3)
            logger.info(f"Successfully updated AWS credentials in {config_path}")
            return {
                "status": "success",
                "message": f"AWS credentials updated in {config_path}",
                "accessKeyID": response["accessKeyID"],
                "secretAccessKey": response["secretAccessKey"]
            }
        except Exception as e:
            raise Exception(f"Failed to write updated config to {config_path}: {str(e)}")
    else:
        raise Exception(f"Failed to get vault keys: {response}")

def setup_docusign_backup_vault():
    """
    Use this tool to setup a vault to backup Docusign. Use this tool only if the user specifically requests to set it up. Or else assume that the user has already set it up.
    You must confirm from the user before executing this.
    """
    # _create_docusign_backup_vault()
    # _get_and_set_vault_keys()
    _ensure_docusign_workflow_exists()

    return {"message": f"Docusign backup is set up successfully. You can now trigger or schedule backups."}

def trigger_docusign_backup() -> dict:
    """
    Triggers a workflow to backup Docusign documents. Assume that the user has already set up the backup vault. If it does not exist, an error will be raised.
    """
    try:
        workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)

        if not workflow_entity:
            raise Exception("Docusign backup is not configured. Please setup a vault to run backups.")

        return _trigger_workflow(DOCUSIGN_WORKFLOW_NAME, operation_type="backup")
    except Exception as e:
        logger.error(f"Error triggering workflow {DOCUSIGN_WORKFLOW_NAME}: {e}")
        return ToolError({"error": str(e)})

def schedule_docusign_backup(
    schedule_type: Annotated[str, Field(description="Type of schedule to create. Options are 'daily' or 'weekly'. Default is 'daily'.")] = "daily",
    time: Annotated[str, Field(description="Time to run the backup job. 24 Hour Format: HH:MM")] = "18:00",
    day_of_week: Annotated[str, Field(description="Day of the week to run the backup job if the schedule type is 'weekly'. Default is 'Sunday'. Title case is used for days.")] = "Sunday"
):
    """
    Schedules docusign backup workflow to run daily. Ask the user if they want to schedule it before running.
    """
    schedule_type = 'daily'

    if schedule_type not in ("daily", "weekly"):
        return {"error": "Only 'daily' and 'weekly' schedule types are supported."}

    hour, minute = map(int, time.split(":"))
    trigger_time = hour*3600 + minute*60

    workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
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
        days_to_run = {d: d == day_of_week for d in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}

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
        "description": f"{schedule_type} schedule for {DOCUSIGN_WORKFLOW_NAME}"
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
    response = commvault_api_client.post("CreateTask", data=payload)
    if isinstance(response, dict) and "taskId" in response:
        return {"message": f"Schedule created successfully. Task ID: {response['taskId']}"}
    return response

def get_docusign_jobs(
    jobLookupWindow: Annotated[int, Field(description="The time window in seconds to look up for jobs jobs. For example, 86400 for the last 24 hours.")]=86400,
    limit: Annotated[int, Field(description="The maximum number of jobs to return. Default is 50.")] = 50,
    offset: Annotated[int, Field(description="The offset for pagination.")] = 0
):
    """
    Retrieves the list of Docusign backup/restore jobs.
    """
    workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)
    if not workflow_entity:
        return {"error": "Workflow not found."}

    workflow_id = workflow_entity["workflowId"]
    endpoint = f"cr/reportsplusengine/datasets/e8ee6af4-58d8-4444-abae-3c096e5628a4/data?limit={limit}&offset={offset}&orderby=%5BjobEndTime%5DDESC&parameter.hideAdminJobs=0&parameter.jobCategory=3&parameter.showOnlyLaptopJobs=0&parameter.completedJobLookupTime={jobLookupWindow}&parameter.workFlows={workflow_id}&parameter.jobTypes=90"
    response = commvault_api_client.get(endpoint)
    formatted_response = format_report_dataset_response(response)
    
    # Filter the response to keep only important fields for LLM
    if isinstance(formatted_response, dict) and "records" in formatted_response:
        important_fields = [
            "jobId",
            "status",
            # "percentComplete",
            "jobStartTime",
            "jobEndTime", 
            # "jobElapsedTime",
            "pendingReason"
        ]
        
        filtered_records = []
        for record in formatted_response["records"]:
            filtered_record = {field: record.get(field) for field in important_fields if record.get(field) is not None}
            job_detail = commvault_api_client.post("JobDetails", data={"jobId": record.get("jobId")})
            try:
                workflow_inputs_xml = (
                    job_detail.get("job", {})
                    .get("jobDetail", {})
                    .get("detailInfo", {})
                    .get("workflowInputsXml", "")
                )
                if workflow_inputs_xml:
                    if "restore" in workflow_inputs_xml.lower():
                        filtered_record["operationType"] = "Restore"
                    elif "backup" in workflow_inputs_xml.lower():
                        filtered_record["operationType"] = "Backup"
            except Exception:
                pass
            filtered_records.append(filtered_record)
        
        formatted_response["records"] = filtered_records
    
    return formatted_response

def list_backedup_docusign_envelopes(
    date: Annotated[str, Field(description="Date in YYYY-MM-DD format to list backups on specific date. Default is empty string which lists all backups.")] = ""
):
    """
    Lists all backed up docusign envelopes and files. This can be used to fetch the paths of all backed up envelopes and their files.
    """
    def _get_s3_client(config):
        try:
            return boto3.client(
                's3',
                endpoint_url=config['aws']['endpoint'],
                region_name=config['aws']['region'],
                aws_access_key_id=config['aws']['accessKeyId'],
                aws_secret_access_key=config['aws']['secretAccessKey'],
                config=boto3.session.Config(s3={'addressing_style': 'path'})
            )
        except KeyError as e:
            logger.exception(f"Missing S3 Vault config key: {e}. Please check your configuration file.")
            raise

    def _process_envelope(envelope_folder):
        """Process a single envelope and return envelope data"""
        envelope_id = envelope_folder.split("/")[-1]
        logger.info(f"Processing envelope: {envelope_id}")
        
        document_files = []
        envelope_params = {
            "Bucket": DOCUSIGN_VAULT_NAME,
            "Prefix": envelope_folder + "/"
        }
        
        for envelope_page in paginator.paginate(**envelope_params):
            if "Contents" in envelope_page:
                for obj in envelope_page["Contents"]:
                    if not obj["Key"].endswith("/"):
                        file_name = obj["Key"].split(envelope_folder + "/")[1]
                        if file_name not in ("metadata.json", "Summary"):
                            document_files.append(file_name)
                        logger.info(f"   |___ {obj['Key']}")
        
        envelope_entry = {"id": envelope_id}
        if document_files:
            if len(document_files) <= 10:
                envelope_entry["docs"] = document_files
            else:
                envelope_entry["docs"] = f"{len(document_files)} documents"
        
        return envelope_entry

    def _process_date_folder(folder):
        """Process a date folder and return date entry with envelopes"""
        folder_name = folder.split("/")[-1]
        logger.info(f"Processing date folder: {folder}")
        
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
                    envelope_folder = envelope_cp["Prefix"].rstrip("/")
                    envelope_entry = _process_envelope(envelope_folder)
                    date_entry["envelopes"].append(envelope_entry)
        
        return date_entry if date_entry["envelopes"] else None
    
    logger.info("----- Starting List Backup -----")

    config_path = "config/docusign_config.json"
    if not os.path.isfile(config_path):
        raise Exception("Docusign Configuration file not found")

    with open(config_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    logger.info(config)
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
        
        for date_page in paginator.paginate(**date_params):
            if "CommonPrefixes" in date_page:
                for envelope_cp in date_page["CommonPrefixes"]:
                    envelope_folder = envelope_cp["Prefix"].rstrip("/")
                    envelope_entry = _process_envelope(envelope_folder)
                    date_entry["envelopes"].append(envelope_entry)
        
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

        for page in paginator.paginate(**operation_params):
            if "CommonPrefixes" in page:
                for cp in page["CommonPrefixes"]:
                    folder = cp["Prefix"].rstrip("/")
                    folder_name = folder.split("/")[-1]
                    
                    # Only consider date folders (skip docusign-backup folder)
                    if folder_name != "docusign-backup":
                        date_entry = _process_date_folder(folder)
                        if date_entry:
                            backup_data["backups"].append(date_entry)

    total_dates = len(backup_data["backups"])
    total_envelopes = sum(len(date_data["envelopes"]) for date_data in backup_data["backups"])
    backup_data["summary"] = f"{total_dates} dates, {total_envelopes} envelopes"

    if total_dates == 0:
        logger.info("No matching backups found.")
        backup_data["message"] = "No backups found"
    else:
        logger.info(f"Found {total_dates} backup dates with {total_envelopes} envelopes")

    logger.info("----- END List Backup -----")
    logger.info(json.dumps(backup_data, indent=2))
    return backup_data

def recover_docusign_envelope(
    backup_date: Annotated[str, Field(description="Date in YYYY-MM-DD format when the envelope was backed up.")],
    envelope_id: Annotated[str, Field(description="The ID of the envelope to recover.")]
):
    """
    Trigger the restoration of a Docusign envelope from backup. 
    Restored files will be saved to <commvault_temp_directory_path>/docusign-restores/<backup_date>/<envelope_id>. 
    Please update the backup_date and envelope_id before responding. Temp directory path can be left as placeholder, assume user knows it.
    """
    try:
        workflow_entity = _check_workflow_exists(DOCUSIGN_WORKFLOW_NAME)

        if not workflow_entity:
            raise Exception("Docusign backup is not configured. Please setup a vault to run backups.")

        return _trigger_workflow(DOCUSIGN_WORKFLOW_NAME, operation_type="restore", source_path=f"{backup_date}/{envelope_id}")
    except Exception as e:
        logger.error(f"Error triggering workflow {DOCUSIGN_WORKFLOW_NAME}: {e}")
        return ToolError({"error": str(e)})
    

DOCUSIGN_TOOLS = [
    setup_docusign_backup_vault,
    trigger_docusign_backup,
    schedule_docusign_backup,
    list_backedup_docusign_envelopes,
    recover_docusign_envelope,
    get_docusign_jobs
]
