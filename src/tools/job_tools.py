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
from src.wrappers import get_basic_job_details


def get_job_detail(job_id: Annotated[int, Field(description="The ID of the job to retrieve.")],) -> dict:
    """
    Gets complete details about a job for a given job id.

    Returns:
        A dictionary containing all available information about the specified job.
    """
    try:
        endpoint = f"Job/{job_id}"
        jobs_list = commvault_api_client.get(endpoint).get("jobs", [])
        if not jobs_list:
            raise Exception(f"No job found with ID: {job_id}")
        return jobs_list[0]
    except Exception as e:
        logger.error(f"Error getting job detail: {e}")
        return ToolError({"error": str(e)})


def suspend_job(
        job_id: Annotated[int, Field(description="The ID of the job to suspend.")], 
        reason: Annotated[str, Field(description="The reason for suspending the job. Optional.")]="") -> dict:
    """
    Suspends/pauses the job with the given job id.
    """
    try:
        endpoint = f"Jobs/MultiJobOperation"
        payload = {
            "message": 1,
            "jobOpReq": {
            "operationType": 0,
            "jobs": [{"jobId": job_id}]
            },
            "operationDescription": reason
        }
        return commvault_api_client.post(endpoint, data=payload)
    except Exception as e:
        logger.error(f"Error suspending job: {e}")
        return ToolError({"error": str(e)})
    

def resume_job(job_id: Annotated[int, Field(description="The ID of the job to resume.")]) -> dict:
    """
    Resumes the job with the given job id.
    """
    try:
        endpoint = f"Job/{job_id}/Action/Resume"
        return commvault_api_client.post(endpoint, data={})
    except Exception as e:
        logger.error(f"Error resuming job: {e}")
        return ToolError({"error": str(e)})


def resubmit_job(job_id: Annotated[int, Field(description="The ID of the job to resubmit.")]) -> dict:
    """
    Resubmits the job with the given job id.
    """
    try:
        endpoint = f"Job/{job_id}/Action/Resubmit"
        return commvault_api_client.post(endpoint, data={})
    except Exception as e:
        logger.error(f"Error resubmitting job: {e}")
        return ToolError({"error": str(e)})


def kill_job(job_id: Annotated[int, Field(description="The ID of the job to kill.")],) -> dict:
    """
    Kills the job with the given job id.
    """
    try:
        endpoint = f"Job/{job_id}/Action/Kill"
        return commvault_api_client.post(endpoint, data={})
    except Exception as e:
        logger.error(f"Error killing job: {e}")
        return ToolError({"error": str(e)})


def get_jobs_list(
    jobLookupWindow: Annotated[int, Field(description="The time window in seconds to look up for jobs jobs. For example, 86400 for the last 24 hours.")]=86400,
    job_filter: Annotated[str, Field(description="The job types to filter by. Multiple types can be provided, comma-separated. If not used, returns backup jobs. Valid values are: MAGLIBMAINTENANCE, SELECTIVEDELETE, ARCHIVERESTORE, MEDIAINIT, SEND_LOGFILE, AUXCOPY, MEDIAINVENTORY, SHELFMANAGEMENT, Backup, MEDIAPREDICTION, SNAPBACKUP, BACKUP3RD, BROWSEANDDELETE, MEDIARECYCLE, SNAPBACKUP3RD, CATALOG_MIGRATION, MEDIAREFRESHING, SNAPRECOVERY, CATALOGUEMEDIA, MEDIAREFRESHING2, SNAPSHOT, CCMCAPTURE, MININGBACKUP, SNAPTOTAPE, CCMMERGE, MININGCONTENTINDEX, SNAPTOTAPEWORKFLOW, COMMCELLSYNC, MOVE_MOUNT_PATH, SNAPVAULTRESTORE, CONTDATAREPLICATION, MOVEDDB, SPACE_RECLAMATION, CONTENT_INDEXING_ENTITY_EXTRACTION, MULTI_NODE_CONTENT_INDEXING, SRMAGENTLESSOPTYPE, CREATECONSISTENCYPOINT, OFFLINE, SRMOPTYPE, CREATERECOVERYPOINT, OFFLINE_MINING_RESTORE, SRMREPORT, CREATEREPLICA, OFFLINECONTENTINDEX, SRSYSTEMRECOVERY, CSDRBKP, ONLINE, STAMPMEDIA, CVEXPORT, Online_Content_Index, STATELESS_BACKUP, DATA_ANALYTICS, ONLINE_CRAWL, STUBBING, DATA_VERIFICATION, OPENBACKUP, SUBCLIENTCONTENTINDEX, DDBOPS, OTHERADMINOPERATION, SYNTHFULL, DEDUPDBSYNC, PATCHDOWNLOAD, SYSRECOVERYBACKUP, DEDUPDBSYNC_DASH, PATCHUPDATE, SYSSTATEBACKUP, DELAYEDCATALOG, POWERRESTORE, TAPE2TAPECOPY, DELAYEDCATALOGWORKFLOW, POWERSEARCHANDRETRIEVE, TAPEERASE, DMOUTLOOKRST, PRUNE, TAPEIMPORT, DRIVECLEANING, PST_ARCHIVING, TDFSBACKUP, DRIVEVALIDATION, QRCOPYBACK, TURBO_NAS, DRORCHESTRATION, QRROLLBACK, UNINSTALLCLIENT, EXCHANGE_IMPORT, QUICKDMRST, UPDATEREPLICA, EXCHANGE_LIVE_BROWSE_RST, RECOVERY_POINT_CREATION_RST, UPGRADE_CLIENT, FDCCLIENT, REFCOPYPSTARCHIVING, VIRTUALIZEME, FDCOPTYPE, REFERENCECOPY, VM_Management, FDCPREPARATION, REFERENCECOPYWORKFLOW, VSA_BLOCK_REPLICATION, FDCWORKFLOW, REPLICATION, VSA_BLOCK_REPLICATION_DRIVER_INSTALL, FLRCOPYBACK, REPORT, VSA_BLOCK_REPLICATION_DRIVER_UNINSTALL, IMPORT, Restore, VSA_BLOCK_REPLICATION_DRIVER_UPDATE, INDEXFREERESTORE, RESTORE_VALIDATE, W2KFULLBUILDRESTORE, INDEXRESTORE, SCHEDEXPORT, W2KFULLBUILDRESTORE371, INFOMGMT, SCHEDULE, W2KSYSSTRESTORE, INSTALLCLIENT, SEARCHANDRETRIEVE, WORKFLOW, LOG_MONITORING, SECUREERASE, WORKFLOW_MGMT.")]="", 
    job_status: Annotated[str, Field(description="The job status to filter by. Valid values are: Active, Finished, All")] = "All", 
    client_id: Annotated[str, Field(description="The client id to filter jobs by. Not mandatory. If not provided, jobs for all clients will be returned.")] = "",
    limit: Annotated[int, Field(description="The maximum number of jobs to return. Default is 50.")] = 50,
    offset: Annotated[int, Field(description="The offset for pagination.")] = 0) -> dict:
    """
    Gets the list of jobs filtered by job type/status/clientId in a given jobLookupWindow.
    """
    try:
        params = {
            "jobCategory": job_status,
            "completedJobLookupTime": jobLookupWindow,
            "limit": limit
        }
        if job_filter:
            params["jobFilter"] = job_filter
        if client_id:
            params["clientId"] = client_id
        if offset is not None:
            params["offset"] = offset
        response = commvault_api_client.get("Job", params=params)
        return get_basic_job_details(response)
    except Exception as e:
        logger.error(f"Error retrieving jobs by job type: {e}")
        return ToolError({"error": str(e)})
    
def get_failed_jobs(
    jobLookupWindow: Annotated[int, Field(description="The time window in seconds to look up for jobs. For example, 86400 for the last 24 hours.")]=86400,
    limit: Annotated[int, Field(description="The maximum number of jobs to return. Default is 50.")] = 50,
    offset: Annotated[int, Field(description="The offset for pagination.")] = 0) -> dict:
    """
    Gets the list of failed jobs in a given jobLookupWindow.
    """
    try:
        payload = {
            "category": 0,
            "pagingConfig": {
                "offset": offset,
                "limit": limit
            },
            "jobFilter": {
                "completedJobLookupTime": jobLookupWindow,
                "showAgedJobs": False,
                "statusList": ["Failed"]
            }
        }
        response = commvault_api_client.post("Jobs", data=payload)
        return get_basic_job_details(response)
    except Exception as e:
        logger.error(f"Error retrieving failed jobs: {e}")
        return ToolError({"error": str(e)})


def get_job_task_details(job_id: Annotated[int, Field(description="The job id to retrieve task details for.")]) -> dict:
    """
    Gets task details for a given job ID.
    """
    try:
        return commvault_api_client.get(f"Job/{job_id}/taskdetails")
    except Exception as e:
        logger.error(f"Error retrieving job task details: {e}")
        return ToolError({"error": str(e)})


def get_retention_info_of_a_job(job_id: Annotated[int, Field(description="The job id to retrieve retention info for.")]) -> dict:
    """
    Gets retention info for a given job ID.
    """
    try:
        return commvault_api_client.get(f"Job/{job_id}/advanceddetails", params={"infoType": 1})
    except Exception as e:
        logger.error(f"Error retrieving retention info: {e}")
        return ToolError({"error": str(e)})

def create_send_logs_job_for_a_job(emailid: Annotated[str, Field(description="The email id to send logs to.")], job_id: Annotated[int, Field(description="The job id for which to send logs.")]) -> dict:
    """
    Triggers a new send logs job for the specified job ID and sends logs to the provided email address.
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
                                    "jobid": job_id,
                                    "tsDatabase": False,
                                    "galaxyLogs": True,
                                    "getLatestUpdates": False,
                                    "actionLogsStartJobId": 0,
                                    "computersSelected": False,
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
                                    "emailSubject": f"Job {job_id} : Send Logs",
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
                                        {"clientId": 0, "clientName": "null"}
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
        logger.error(f"Error creating send logs job for job: {e}")
        return ToolError({"error": str(e)})
    

JOB_MANAGEMENT_TOOLS = [
    get_job_detail,
    suspend_job,
    resume_job,
    resubmit_job,
    kill_job,
    get_jobs_list,
    get_failed_jobs,
    get_job_task_details,
    get_retention_info_of_a_job,
    create_send_logs_job_for_a_job
]
