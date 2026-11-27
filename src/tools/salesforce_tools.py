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
Salesforce tools for the Commvault MCP Server.

Provides tools to:
  - Resolve a Salesforce organisation ID to the corresponding Commvault
    clientId using the Salesforce list (Organization) API.
  - Fetch backed-up Salesforce records for a given object using the
    Commvault Salesforce records API (Reportsplus Engine dataset endpoint).
    Records are always returned from the latest backup snapshot.
"""

import json

from fastmcp.exceptions import ToolError
from typing import Annotated, Optional
from pydantic import BaseModel, Field

from src.cv_api_client import commvault_api_client
from src.logger import logger
from src.untrusted_data import raise_mcp_tool_error


# ── Filter models ────────────────────────────────────────────────────────────

class QueryBuilderRule(BaseModel):
    """
    A single filter condition on a Salesforce field.

    ColumnType values:  STRING=0, INTEGER=1, DOUBLE=2, DATE=3,
                        BOOLEAN=4, ENUM=5, DATETIME=6, TIME=7
    Condition (QueryOperationType) values:
        EQUALS=0, DOESNOTEQUALS=1, CONTAINS=2, DOESNOTCONTAINS=3,
        LESSTHAN=4, LESSTHANEQUAL=5, GREATERTHAN=6, GREATERTHANEQUAL=7,
        BETWEEN=8, ANYINSELECTION=9, NOTINSELECTION=10, REGEX=11,
        STARTSWITH=12, ENDSWITH=13, ISNULL=14, ISNOTNULL=15,
        ISEMPTY=16, ISNOTEMPTY=17, NOTBETWEEN=18
    """
    column: str = Field(description="Salesforce API field name (e.g. 'Name', 'AccountNumber', 'CreatedDate').")
    condition: int = Field(description="QueryOperationType integer: EQUALS=0, DOESNOTEQUALS=1, CONTAINS=2, DOESNOTCONTAINS=3, LESSTHAN=4, LESSTHANEQUAL=5, GREATERTHAN=6, GREATERTHANEQUAL=7, BETWEEN=8, REGEX=11, STARTSWITH=12, ENDSWITH=13, ISNULL=14, ISNOTNULL=15, ISEMPTY=16, ISNOTEMPTY=17, NOTBETWEEN=18")
    columnType: int = Field(description="ColumnType integer: STRING=0, INTEGER=1, DOUBLE=2, DATE=3, BOOLEAN=4, ENUM=5, DATETIME=6, TIME=7")
    stringParam1: str = Field(default="", description="Primary filter value (or start of range for BETWEEN/NOTBETWEEN).")
    stringParam2: Optional[str] = Field(default=None, description="End of range value for BETWEEN or NOTBETWEEN conditions only.")


class QueryBuilderBlock(BaseModel):
    """
    A group of filter rules or nested sub-blocks combined with AND or OR.
    Either 'rules' (leaf conditions) or 'blocks' (nested groups) should be populated — not both.
    """
    rules: Optional[list[QueryBuilderRule]] = Field(default=None, description="Leaf filter conditions in this block.")
    isAnd: bool = Field(default=True, description="True = combine rules/blocks with AND; False = combine with OR.")
    isNot: Optional[bool] = Field(default=None, description="True = negate the entire block result.")
    blocks: Optional[list['QueryBuilderBlock']] = Field(default=None, description="Nested sub-blocks for complex compound filters.")


QueryBuilderBlock.model_rebuild()


# ── Dataset ID for the Salesforce record browse endpoint ─────────────────────
# This dataset UUID is the stable identifier for the Salesforce record-level
# browse capability in the Commvault Reportsplus Engine.
_SF_RECORDS_DATASET_ID = "4e639029-2764-4fb4-b7e3-0fc636cdd025"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _get_org_id_from_org(org: dict) -> str | None:
    """
    Extract the Salesforce org ID from an org dict returned by the API.

    Handles both the top-level 'sfOrgId' field and the nested legacy path
    'salesforceProps.salesforceAccount.organizationId' for backward compatibility.

    Args:
        org: The org dict to extract the ID from.
    Returns:
        The Salesforce org ID if found, otherwise None.
    """
    return org.get("sfOrgId") or org.get("salesforceProps", {}).get("salesforceAccount", {}).get("organizationId")

def _find_org_in_list(orgs: list, target_org_id: str) -> dict | None:
    """
    Search the /Salesforce/Organization list for a matching sfOrgId.

    Handles both 15-character and 18-character Salesforce org ID variants
    by comparing only the first 15 characters when the lengths differ.

    Args:
        orgs:          List of org dicts returned by the Organization API.
        target_org_id: The Salesforce org ID supplied by the caller.

    Returns:
        The matching org dict, or None if not found.
    """
    target = target_org_id.strip()
    for org in orgs:
        cv_org_id: str = _get_org_id_from_org(org)
        if not cv_org_id:
            continue
        if cv_org_id == target or cv_org_id[:15] == target[:15]:
            return org
    return None

def _extract_ids_from_org(org: dict) -> tuple[int | None, int | None, dict]:
    """
    Pull clientId, backupsetId and the sfSubclient block out of an org dict.

    Returns:
        Tuple of (client_id, backupset_id, sf_subclient_dict).
    """
    sf_subclient: dict = org.get("sfSubclient", org.get("subclient", {}))
    client_id: int | None    = sf_subclient.get("clientId")
    backupset_id: int | None = sf_subclient.get("backupsetId")
    return client_id, backupset_id, sf_subclient


def _keyed_records(columns: list[dict], raw_records: list) -> list[dict]:
    """
    Convert the columnar record arrays returned by the dataset API into a
    list of dicts keyed by column name, which is more LLM-friendly.

    Args:
        columns:     Column metadata list from the API response.
        raw_records: List of value arrays, one per record row.

    Returns:
        List of {column_name: value} dicts.
    """
    col_names = [
        col.get("name") or col.get("displayName") or str(i)
        for i, col in enumerate(columns)
    ]
    return [dict(zip(col_names, row)) for row in raw_records]


# ── Tool 1: Resolve Salesforce org ID → clientId ────────────────────────────

def get_salesforce_client(
    salesforce_org_id: Annotated[
        str,
        Field(
            description=(
                "The Salesforce Organisation ID to look up "
                "(e.g. '00D2w000005mBCpEAM'). Both 15-character and "
                "18-character variants are accepted."
            )
        ),
    ],
) -> dict:
    """
    Resolve a Salesforce organisation ID to the Commvault clientId
    by querying the Salesforce Organisation list API.

    Uses the /Salesforce/Organization endpoint to retrieve all configured
    organisations and then matches on sfOrgId, supporting both the 15-char
    and 18-char Salesforce org ID formats.

    Args:
        salesforce_org_id: The Salesforce org ID (15 or 18 characters).

    Returns:
        A dict containing:
          - client_id           (int)  Commvault client ID for the org.
          - organization_name   (str)  Commvault client / org name.
          - salesforce_org_id   (str)  Canonical SF org ID as stored in Commvault.
          - endpoint            (str)  Salesforce login endpoint URL.
          - last_backup_time    (int | None) Unix timestamp of last backup.
        On failure, returns a dict with 'error' key.
    """
    try:
        logger.info("Fetching Salesforce organisation list to resolve org ID '%s'", salesforce_org_id)

        response = commvault_api_client.get("Salesforce/Organization")
        orgs: list = response.get("orgs", response.get("accounts", []))

        if not orgs:
            logger.warning("No Salesforce organisations returned by the API.")
            raise_mcp_tool_error("No Salesforce organisations found in Commvault.")

        org = _find_org_in_list(orgs, salesforce_org_id)

        if org is None:
            available_ids = [_get_org_id_from_org(o) for o in orgs if _get_org_id_from_org(o)]
            raise_mcp_tool_error(
                f"No Commvault organisation found matching Salesforce org ID "
                f"'{salesforce_org_id}'. "
                f"Checked {len(orgs)} org(s). "
                f"Available SF org IDs: {available_ids}"
            )

        client_id, backupset_id, sf_subclient = _extract_ids_from_org(org)

        logger.info(
            "Resolved sfOrgId='%s' → clientId=%s, backupsetId=%s",
            salesforce_org_id, client_id, backupset_id,
        )

        return {
            "client_id": client_id,
            "backupset_id": backupset_id,
            "backupset_name": sf_subclient.get("backupsetName", ""),
            "organization_name": sf_subclient.get("clientName", ""),
            "salesforce_org_id":  _get_org_id_from_org(org) or salesforce_org_id,
            "endpoint": org.get("endPoint", ""),
            "last_backup_time": org.get("backupTime"),
        }

    except ToolError:
        raise
    except Exception as e:
        logger.error("Error resolving Salesforce org ID '%s': %s", salesforce_org_id, e)
        raise_mcp_tool_error(e)


# ── Tool 2: Fetch backed-up Salesforce records ────────────────────────────────

def get_salesforce_records(
    salesforce_org_id: Annotated[
        str,
        Field(
            description=(
                "The Salesforce Organisation ID used to automatically resolve "
                "the Commvault clientId before fetching records "
                "(e.g. '00D2w000005mBCpEAM')."
            )
        ),
    ],
    object_name: Annotated[
        str,
        Field(
            description=(
                "Salesforce API object name whose backed-up records to retrieve "
                "(e.g. 'Account', 'Contact', 'Opportunity')."
            )
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description=(
                "Maximum number of records to return. Defaults to 50. "
                "Increase for larger result sets (max 1000)."
            ),
            ge=1,
            le=1000,
        ),
    ] = 50,
    offset: Annotated[
        int,
        Field(
            description="Number of records to skip for pagination. Defaults to 0.",
            ge=0,
        ),
    ] = 0,
    filter_query: Annotated[
        str,
        Field(
            description=(
                "Optional JSON filter string (freeQueryProp format) to narrow results. "
                "Build a QueryBuilderBlock JSON from the user's natural language filter. "
                "Example — name contains 'test' AND CreatedDate between two timestamps: "
                '{"isAnd":true,"rules":['
                '{"column":"Name","condition":2,"columnType":0,"stringParam1":"test"},'
                '{"column":"CreatedDate","condition":8,"columnType":6,"stringParam1":"1743465600000","stringParam2":"1775087999000"}]}. '
                "condition values: EQUALS=0, DOESNOTEQUALS=1, CONTAINS=2, DOESNOTCONTAINS=3, "
                "LESSTHAN=4, LESSTHANEQUAL=5, GREATERTHAN=6, GREATERTHANEQUAL=7, BETWEEN=8, "
                "REGEX=11, STARTSWITH=12, ENDSWITH=13, ISNULL=14, ISNOTNULL=15, "
                "ISEMPTY=16, ISNOTEMPTY=17, NOTBETWEEN=18. "
                "columnType values: STRING=0, INTEGER=1, DOUBLE=2, DATE=3, BOOLEAN=4, ENUM=5, DATETIME=6, TIME=7. "
                "Leave empty string to return all records."
            )
        ),
    ] = "",
) -> dict:
    """
    Fetch backed-up Salesforce records for a specific Salesforce object from
    the latest available backup snapshot.

    This tool performs two steps automatically:
      1. Calls the Salesforce Organisation list API to resolve the supplied
         salesforce_org_id into a Commvault clientId.
      2. Queries the Commvault Reportsplus Engine dataset endpoint
         (/cr/reportsplusengine/datasets/{dataset_id}/data) with
         pitOptions='latest' and queryOptions='latest' to ensure the most
         recent backed-up data is always returned.

    Records are returned as a list of dicts keyed by Salesforce field name,
    making them immediately usable without further post-processing.

    Args:
        salesforce_org_id: Salesforce org ID (15 or 18 characters).
        object_name:       Salesforce API object name (e.g. 'Account').
        limit:             Max records to return (default 50, max 1000).
        offset:            Pagination offset (default 0).
        filter_query:      Optional structured QueryBuilderBlock filter.

    Returns:
        A dict containing:
          - organization_name  (str)   Commvault client / org display name.
          - salesforce_org_id  (str)   Canonical SF org ID.
          - client_id          (int)   Resolved Commvault client ID.
          - backupset_id       (int)   Resolved Commvault backupset ID.
          - object_name        (str)   The queried Salesforce object.
          - limit              (int)   Requested record limit.
          - offset             (int)   Requested pagination offset.
          - total_record_count (int)   Total matching records in backup.
          - records_returned   (int)   Number of records in this response.
          - columns            (list)  Column/field names.
          - records            (list)  List of {field: value} dicts.
        On failure, returns a dict with 'error' key describing the problem.
    """
    # ── Step 1: resolve org ID → clientId + backupsetId ──────────────────────
    try:
        logger.info(
            "get_salesforce_records: resolving sfOrgId='%s' for object='%s'",
            salesforce_org_id, object_name,
        )

        resolve_result = get_salesforce_client(salesforce_org_id)

        client_id: int    = resolve_result["client_id"]
        backupset_id: int = resolve_result["backupset_id"]
        org_name: str     = resolve_result.get("organization_name", "")

        logger.info(
            "Resolved: clientId=%s, backupsetId=%s — fetching records for object='%s'",
            client_id, backupset_id, object_name,
        )

    except ToolError:
        raise
    except Exception as e:
        logger.error("Unexpected error resolving org ID '%s': %s", salesforce_org_id, e)
        raise_mcp_tool_error(f"Failed to resolve organisation ID: {e}")

    # ── Step 2: fetch records from the latest backup snapshot ─────────────────
    try:
        params = {
            # Pagination
            "limit": limit,
            "offset": offset,
            # Salesforce-specific dataset parameters
            "parameter.backupSetId": backupset_id,
            "parameter.tableName": object_name,
            # Always return the latest backed-up data
            "parameter.pitOptions": "latest",
            "parameter.queryOptions": "latest",
        }

        if filter_query:
            params["freeQueryProp"] = filter_query

        endpoint = f"cr/reportsplusengine/datasets/{_SF_RECORDS_DATASET_ID}/data"

        logger.info(
            "Querying records endpoint for object='%s', backupsetId=%s, limit=%s, offset=%s",
            object_name, backupset_id, limit, offset,
        )

        data = commvault_api_client.get(endpoint, params=params)

        # Surface any application-level error returned by the dataset API
        if data.get("errorCode", 0) != 0:
            err_msg = data.get("errorMessage") or data.get("errorString") or str(data)
            logger.error("Dataset API returned an error for object='%s': %s", object_name, err_msg)
            raise_mcp_tool_error({"error": err_msg, "raw_response": data})

        columns: list[dict] = data.get("columns", [])
        raw_records: list   = data.get("records", [])
        total: int          = data.get("totalRecordCount", 0)
        records_count: int  = data.get("recordsCount", len(raw_records))

        col_names     = [col.get("name") or col.get("displayName") or str(i) for i, col in enumerate(columns)]
        keyed         = _keyed_records(columns, raw_records)

        logger.info(
            "Fetched %d record(s) of %d total for object='%s', backupsetId=%s",
            records_count, total, object_name, backupset_id,
        )

        return {
            "organization_name": org_name,
            "salesforce_org_id": resolve_result.get("salesforce_org_id", salesforce_org_id),
            "client_id": client_id,
            "backupset_id": backupset_id,
            "object_name": object_name,
            "limit": limit,
            "offset": offset,
            "total_record_count": total,
            "records_returned": records_count,
            "columns": col_names,
            "records": keyed,
        }

    except Exception as e:
        logger.error(
            "Error fetching Salesforce records for object='%s', backupsetId=%s: %s",
            object_name, backupset_id, e,
        )
        raise_mcp_tool_error(e)


# ── Tool registry ─────────────────────────────────────────────────────────────

SALESFORCE_TOOLS = [
    get_salesforce_client,
    get_salesforce_records,
]
