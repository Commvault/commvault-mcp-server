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
Outcomes-first protection tools for the Commvault MCP Server.

These three goal-level, READ-ONLY tools let an assistant turn a one-line backup
goal ("protect all prod VMs with 3-2-1, copy us-west-1 to us-east-2") into a
single reviewed plan, instead of hand-chaining low-level lookups and re-entering
overlapping information (the "wizard" feel):

  - ``explain_objective`` : expand an objective like 3-2-1 into concrete copies.
  - ``resolve_scope``     : answer "which resources does this scope cover?".
  - ``plan_protection``   : compile scope + objective into a reviewed proposal.

Honesty is a feature here. This server can DISCOVER and PREVIEW protection but
cannot yet CREATE plans or protection groups, so ``plan_protection`` produces a
proposal a human/UI enacts -- it changes nothing. Selectors are limited to what
the read APIs can actually verify (client group, name, application type); tag and
resource-region selection are refused explicitly rather than guessed at.
"""

import fnmatch
import hashlib
import json
from typing import Annotated, Optional

from pydantic import Field

from src.cv_api_client import commvault_api_client
from src.logger import logger
from src.wrappers import (
    filter_client_list_response,
    filter_storage_pool_response,
    filter_subclient_response,
    get_basic_client_group_details,
)

# Number of matched subclients echoed back as a sample (the full set still drives
# counts and the plan token).
_SAMPLE_LIMIT = 25

# ---------------------------------------------------------------------------
# Protection objectives
# ---------------------------------------------------------------------------
# A named objective expands into an explicit list of copies. Media/region values
# are INTENTIONS; plan_protection reconciles them against storage pools that
# actually exist. The 3-2-1 badge is DERIVED from this structure, never asserted.
OBJECTIVE_CATALOG = {
    "3-2-1": {
        "name": "3-2-1",
        "description": (
            "Three copies of the data, on at least two different media, with at "
            "least one copy kept offsite in a different region. The standard "
            "resilient backup posture."
        ),
        "copies": [
            {"role": "primary", "media": "disk", "locality": "onsite", "region": None,
             "purpose": "Fast local copy for day-to-day restores."},
            {"role": "secondary", "media": "cloud", "locality": "onsite", "region": None,
             "purpose": "Second copy on different media to survive a media-class failure."},
            {"role": "offsite", "media": "cloud", "locality": "offsite-region", "region": None,
             "purpose": "Offsite copy in a second region to survive a site or region loss."},
        ],
        "retentionDays": 30,
        "rpoHours": 24,
        "notes": [
            "'2 media' requires at least two storage-pool media types (e.g. disk + "
            "cloud). If only disk pools exist, this objective cannot be fully met -- "
            "plan_protection will flag it.",
        ],
    },
    "2-copy": {
        "name": "2-copy",
        "description": "Two copies: one local for fast restores and one offsite copy for site resilience.",
        "copies": [
            {"role": "primary", "media": "disk", "locality": "onsite", "region": None,
             "purpose": "Fast local copy for day-to-day restores."},
            {"role": "offsite", "media": "cloud", "locality": "offsite-region", "region": None,
             "purpose": "Offsite copy in a second region for site resilience."},
        ],
        "retentionDays": 30,
        "rpoHours": 24,
        "notes": [],
    },
    "1-copy": {
        "name": "1-copy",
        "description": "A single local copy. Lowest cost, no offsite resilience -- suitable only for non-critical or staging data.",
        "copies": [
            {"role": "primary", "media": "disk", "locality": "onsite", "region": None,
             "purpose": "Single local copy."},
        ],
        "retentionDays": 30,
        "rpoHours": 24,
        "notes": ["No offsite copy: this does not survive a site or region loss."],
    },
}

_SUPPORTED_RESOURCE_TYPES = {"vm", "fileserver", "database"}

# appName substrings used to classify a subclient's resource type (the read APIs
# expose application name, not a clean type enum).
_TYPE_KEYWORDS = {
    "vm": ["virtual server", "vsa", "vmware", "hyper-v", "hyperv", "azure", "amazon",
           "nutanix", "virtual machine", "oracle vm", "google cloud"],
    "fileserver": ["file system", "windows file", "linux file", "unix file", "nas",
                   "file server", "big data apps"],
    "database": ["sql", "oracle", "db2", "postgres", "mysql", "sap hana", "sybase",
                 "mongodb", "informix", "database"],
}


def _derive_321_badge(copies) -> dict:
    """Derive the 3-2-1 badge from the copy structure (never assume it)."""
    n = len(copies)
    media = {c.get("media") for c in copies if c.get("media")}
    offsite = sum(1 for c in copies if c.get("locality") == "offsite-region")
    satisfied = n >= 3 and len(media) >= 2 and offsite >= 1
    return {
        "name": "3-2-1",
        "satisfied": satisfied,
        "basis": (
            f"{n} copies, {len(media)} distinct media, {offsite} offsite -> rule is "
            ">=3 copies across >=2 media with >=1 offsite copy"
        ),
    }


def _expand_objective(objective, copy_overrides=None, retention_override=None) -> dict:
    """Expand a named objective into concrete copies + a derived 3-2-1 badge."""
    key = (objective or "").strip().lower()
    spec = OBJECTIVE_CATALOG.get(key)
    if not spec:
        return {
            "status": "ERROR",
            "reason": f"Unknown objective '{objective}'.",
            "supportedObjectives": sorted(OBJECTIVE_CATALOG),
        }
    copies = []
    for c in spec["copies"]:
        copy = dict(c)
        if copy_overrides and copy["role"] in copy_overrides and copy_overrides[copy["role"]]:
            copy["region"] = copy_overrides[copy["role"]]
        copies.append(copy)
    retention = retention_override if retention_override is not None else spec["retentionDays"]
    return {
        "status": "OK",
        "objective": spec["name"],
        "description": spec["description"],
        "copies": copies,
        "retentionDays": retention,
        "rpoHours": spec["rpoHours"],
        "badge": _derive_321_badge(copies),
        "notes": spec.get("notes", []) + [
            "Media and region assignments are intentions; plan_protection reconciles "
            "them against the storage pools that actually exist in this CommCell.",
        ],
    }


# ---------------------------------------------------------------------------
# Scope resolution helpers
# ---------------------------------------------------------------------------
def _matches_type(app_name, resource_type) -> bool:
    if not app_name:
        return False
    keywords = _TYPE_KEYWORDS.get((resource_type or "").lower().strip(), [])
    lowered = app_name.lower()
    return any(k in lowered for k in keywords)


def _find_client_group(name):
    """Return {id, name, clientCount} for a client group matched by name, or None."""
    resp = commvault_api_client.get("ClientGroup")
    groups = get_basic_client_group_details(resp).get("clientGroups", [])
    for g in groups:
        if (g.get("clientGroupName") or "").lower() == (name or "").lower():
            return {
                "id": g.get("clientGroupId"),
                "name": g.get("clientGroupName"),
                "clientCount": g.get("clientCount"),
            }
    return None


def _extract_associated_clients(resp):
    """Best-effort extraction of member clients from a ClientGroup/{id} response."""
    if not isinstance(resp, dict):
        return []
    for path in (
        ("clientGroupDetail", "associatedClients"),
        ("associatedClients",),
        ("clientGroup", "associatedClients"),
        ("clientGroupDetail", "associatedClientsList", "associatedClients"),
    ):
        node = resp
        ok = True
        for key in path:
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                ok = False
                break
        if ok and isinstance(node, list):
            out = []
            for c in node:
                if isinstance(c, dict):
                    cid = c.get("clientId", c.get("id"))
                    if cid is not None:
                        out.append({"clientId": cid, "clientName": c.get("clientName", c.get("name"))})
            if out:
                return out
    return []


def _clients_in_group(group):
    """Return (clients, basis). clients may be empty if membership can't be read."""
    gid = group["id"]
    try:
        resp = commvault_api_client.get(f"ClientGroup/{gid}")
    except Exception as e:  # noqa: BLE001 - report, don't crash
        return [], (f"client group '{group['name']}' reports {group.get('clientCount')} "
                    f"members but reading its properties failed ({e})")
    clients = _extract_associated_clients(resp)
    if clients:
        return clients, f"client group '{group['name']}': enumerated {len(clients)} member client(s)"
    return [], (f"client group '{group['name']}' reports {group.get('clientCount')} members, "
                "but its properties did not expose a member-client list in a recognised "
                "field, so member subclients could not be enumerated")


def _subclients_for_client(client_id):
    resp = commvault_api_client.get("subclient", params={"clientId": client_id})
    return filter_subclient_response(resp).get("subClients", [])


def _resolve_scope_full(client_group=None, name_glob=None, resource_type=None,
                        tag=None, resource_region=None) -> dict:
    """Core scope resolver returning the FULL match list (uncapped)."""
    supported = ["client_group", "name_glob", "resource_type (VM|fileserver|database)"]

    # 1. Refuse selector dimensions the read APIs cannot honour -- explicitly.
    if tag or resource_region:
        return {
            "status": "NOT_SUPPORTED",
            "requested": {"tag": tag, "resource_region": resource_region},
            "reason": (
                "This server cannot select resources by free-form tag or by the "
                "resource's own region. The read APIs expose client name/ID, host, "
                "and application type only -- not tags, and region only on storage "
                "pools."
            ),
            "guidance": (
                "Use 'client_group' as the stand-in for environments like prod/"
                "staging, or 'name_glob'/'resource_type'. For regional protection, "
                "set a copy's target region in plan_protection -- 'regional' means "
                "placing a COPY in a region, not selecting resources that live there."
            ),
            "supportedSelectors": supported,
        }

    # 2. Validate resource_type rather than silently matching nothing.
    if resource_type and resource_type.lower().strip() not in _SUPPORTED_RESOURCE_TYPES:
        return {
            "status": "ERROR",
            "reason": f"Unknown resource_type '{resource_type}'.",
            "supportedResourceTypes": sorted(_SUPPORTED_RESOURCE_TYPES),
        }

    # 3. Require at least one selector -- never silently match everything.
    if not (client_group or name_glob or resource_type):
        return {
            "status": "ERROR",
            "reason": ("No selector provided. Specify at least one of client_group, "
                       "name_glob, or resource_type so the scope is explicit."),
            "supportedSelectors": supported,
        }

    basis = []

    # 4. Build the candidate client set.
    if client_group:
        group = _find_client_group(client_group)
        if not group:
            return {
                "status": "ERROR",
                "reason": f"No client group named '{client_group}' was found.",
                "hint": "List available client groups (get_client_group_list) and retry with an exact name.",
            }
        clients, member_basis = _clients_in_group(group)
        basis.append(member_basis)
        if not clients:
            return {
                "status": "UNRESOLVED",
                "reason": ("The client group was found but its member clients could not "
                           "be enumerated, so coverage is unknown. Do not assume zero or "
                           "all -- resolve membership another way before planning."),
                "group": group,
                "resolution_basis": "; ".join(basis),
            }
    else:
        resp = commvault_api_client.get("Client")
        clients = filter_client_list_response(resp).get("clients", [])
        basis.append(f"scanned {len(clients)} client(s) in the CommCell")

    # 5. Optional name glob over client names.
    if name_glob:
        before = len(clients)
        pattern = name_glob.lower()
        clients = [c for c in clients
                   if c.get("clientName") and fnmatch.fnmatch(c["clientName"].lower(), pattern)]
        basis.append(f"name_glob '{name_glob}' matched {len(clients)}/{before} client(s)")

    # 6. Enumerate subclients per client; optional resource_type filter via appName.
    matches = []
    for c in clients:
        cid = c.get("clientId")
        if cid is None:
            continue
        try:
            subs = _subclients_for_client(cid)
        except Exception as e:  # noqa: BLE001 - skip unreadable client, keep going
            logger.warning(f"resolve_scope: could not list subclients for client {cid}: {e}")
            continue
        for s in subs:
            if resource_type and not _matches_type(s.get("appName"), resource_type):
                continue
            matches.append({
                "clientName": s.get("clientName") or c.get("clientName"),
                "clientId": cid,
                "subclientId": s.get("subclientId"),
                "subclientName": s.get("subclientName") or s.get("displayName"),
                "backupsetId": s.get("backupsetId"),
                "appName": s.get("appName"),
            })
    if resource_type:
        basis.append(f"resource_type '{resource_type}' applied via appName classification")

    if not matches:
        return {
            "status": "EMPTY",
            "matchedCount": 0,
            "matches": [],
            "reason": ("The selector resolved to zero protectable subclients. Stop and "
                       "report this; do not proceed as if everything matched."),
            "resolution_basis": "; ".join(basis),
        }

    return {
        "status": "OK",
        "matchedCount": len(matches),
        "matches": matches,
        "selector": {"client_group": client_group, "name_glob": name_glob, "resource_type": resource_type},
        "resolution_basis": "; ".join(basis),
    }


# ---------------------------------------------------------------------------
# Storage-pool helpers (for regional copy placement)
# ---------------------------------------------------------------------------
def _list_pools():
    try:
        resp = commvault_api_client.get("StoragePool")
        return filter_storage_pool_response(resp).get("storagePools", [])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"plan_protection: could not list storage pools: {e}")
        return []


def _slim_pool(pool):
    if not pool:
        return None
    return {
        "storagePoolName": pool.get("storagePoolName"),
        "storagePoolId": pool.get("storagePoolId"),
        "regionName": pool.get("regionName"),
    }


def _choose_pool(pools, region):
    """Return (pool, note) choosing a pool for a region (or any pool if none)."""
    if region:
        wanted = region.lower()
        matches = [p for p in pools
                   if wanted in {(p.get("regionName") or "").lower(),
                                 (p.get("regionDisplayName") or "").lower()}]
        if not matches:
            return None, None
        if len(matches) > 1:
            return matches[0], (f"Multiple storage pools in region '{region}'; proposing "
                                f"'{matches[0].get('storagePoolName')}' -- confirm this is intended.")
        return matches[0], None
    return (pools[0], None) if pools else (None, None)


def _plan_token(scope, objective_spec, copies, plan_name) -> str:
    """Deterministic hash binding the resolved desired state to this proposal."""
    payload = {
        "subclientIds": sorted(m.get("subclientId") for m in scope.get("matches", [])
                               if m.get("subclientId") is not None),
        "matchedCount": scope.get("matchedCount"),
        "objective": objective_spec.get("objective"),
        "retentionDays": objective_spec.get("retentionDays"),
        "copies": [{"role": c.get("role"), "region": c.get("region"),
                    "pool": (c.get("assignedStoragePool") or {}).get("storagePoolId")}
                   for c in copies],
        "planName": plan_name,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "cvplan-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
def explain_objective(
    objective: Annotated[str, Field(description="Protection objective to expand. Supported: '3-2-1', '2-copy', '1-copy'.")] = "3-2-1",
    source_region: Annotated[Optional[str], Field(description="Region for the primary copy (e.g. 'us-west-1'). Use exact region strings from commvault://regions. Optional.")] = None,
    target_region: Annotated[Optional[str], Field(description="Region for the offsite copy (e.g. 'us-east-2'). Optional.")] = None,
    retention_days: Annotated[Optional[int], Field(description="Override the default retention in days. Optional.")] = None,
) -> dict:
    """
    Expand a named protection objective (e.g. '3-2-1') into the concrete set of
    backup copies it implies -- their role, media, and region -- so the real copy
    topology is visible before anything is built. Pure read-only computation; it
    calls no API. The 3-2-1 badge is DERIVED from the resulting structure (>=3
    copies across >=2 media with >=1 offsite copy), not assumed.
    """
    overrides = {}
    if source_region:
        overrides["primary"] = source_region
    if target_region:
        overrides["offsite"] = target_region
    return _expand_objective(objective, overrides or None, retention_days)


def resolve_scope(
    client_group: Annotated[Optional[str], Field(description="Client group name to scope to (the stand-in for environments like 'prod' or 'staging'). Optional.")] = None,
    name_glob: Annotated[Optional[str], Field(description="Glob pattern matched against client names, e.g. 'web-*' or '*-prod'. Optional.")] = None,
    resource_type: Annotated[Optional[str], Field(description="Filter by resource type: 'VM', 'fileserver', or 'database'. Optional.")] = None,
    tag: Annotated[Optional[str], Field(description="NOT SUPPORTED in v1: selecting by free-form tag. If provided, the tool explains why and offers a supported alternative.")] = None,
    resource_region: Annotated[Optional[str], Field(description="NOT SUPPORTED in v1: selecting resources by their own region. If provided, the tool explains the supported alternative (regional copies are set on the plan).")] = None,
) -> dict:
    """
    Answer "which resources does this scope actually cover?" in one read-only
    call, so coverage can be sanity-checked before planning protection. Resolves a
    selector (client_group, name_glob, and/or resource_type) to the concrete
    subclients it matches and returns the match count, a sample of matches, and an
    explicit resolution_basis. Selecting by free-form tag or by a resource's own
    region is refused explicitly rather than guessed. An empty match is reported
    as empty -- never silently treated as "everything".
    """
    try:
        full = _resolve_scope_full(client_group, name_glob, resource_type, tag, resource_region)
        if full.get("status") != "OK":
            return full
        matches = full["matches"]
        return {
            "status": "OK",
            "matchedCount": full["matchedCount"],
            "sampleMatches": matches[:_SAMPLE_LIMIT],
            "truncated": len(matches) > _SAMPLE_LIMIT,
            "selector": full["selector"],
            "resolution_basis": full["resolution_basis"],
            "note": "Read-only. These are existing protectable subclients; nothing was changed.",
        }
    except Exception as e:  # noqa: BLE001
        logger.error(f"Error resolving scope: {e}")
        return {"status": "ERROR", "reason": str(e)}


def plan_protection(
    client_group: Annotated[Optional[str], Field(description="Client group to protect (stand-in for 'prod'/'staging'). Provide at least one selector.")] = None,
    name_glob: Annotated[Optional[str], Field(description="Glob over client names to protect, e.g. 'web-*'. Optional.")] = None,
    resource_type: Annotated[Optional[str], Field(description="Restrict to 'VM', 'fileserver', or 'database'. Optional.")] = None,
    objective: Annotated[str, Field(description="Protection objective, e.g. '3-2-1', '2-copy', '1-copy'.")] = "3-2-1",
    source_region: Annotated[Optional[str], Field(description="Region for the primary copy (exact string from commvault://regions). Optional.")] = None,
    target_region: Annotated[Optional[str], Field(description="Region to place the offsite copy in, e.g. 'us-east-2'. Optional.")] = None,
    retention_days: Annotated[Optional[int], Field(description="Override default retention in days. Optional.")] = None,
) -> dict:
    """
    Turn a one-line protection goal into a single reviewed plan-of-record. Resolves
    the scope, expands the objective (e.g. 3-2-1) into concrete copies, assigns
    storage pools by region, and returns the full proposed change set: matched
    resources, the proposed plan and its copies, regional copy placement, any open
    questions, and a plan_token.

    PREVIEW ONLY -- it creates nothing. This server cannot yet enact provisioning;
    hand the proposal to a Commvault admin or the Command Center UI to apply.
    "Regional" means placing a copy in target_region (configured on the plan), not
    selecting resources that live in that region.
    """
    try:
        scope = _resolve_scope_full(client_group, name_glob, resource_type)
        if scope.get("status") != "OK":
            return {
                "status": "CANNOT_PLAN",
                "reason": "Scope did not resolve to protectable resources; see 'scope' for details.",
                "scope": scope,
            }

        overrides = {}
        if source_region:
            overrides["primary"] = source_region
        if target_region:
            overrides["offsite"] = target_region
        objective_spec = _expand_objective(objective, overrides or None, retention_days)
        if objective_spec.get("status") != "OK":
            return {"status": "CANNOT_PLAN", "reason": objective_spec.get("reason"),
                    "supportedObjectives": objective_spec.get("supportedObjectives")}

        pools = _list_pools()
        open_questions = []
        available_regions = sorted({p.get("regionName") for p in pools if p.get("regionName")})
        compiled_copies = []
        for copy in objective_spec["copies"]:
            region = copy.get("region")
            chosen, note = _choose_pool(pools, region)
            if region and not chosen:
                open_questions.append(
                    f"No storage pool found in region '{region}' for the {copy['role']} copy. "
                    f"Available pool regions: {available_regions or 'none discovered'}."
                )
            elif note:
                open_questions.append(note)
            compiled_copies.append({**copy, "assignedStoragePool": _slim_pool(chosen)})

        if not objective_spec["badge"]["satisfied"] and objective.strip().lower() == "3-2-1":
            open_questions.append(
                "The objective expands to a topology that does not satisfy 3-2-1 "
                f"({objective_spec['badge']['basis']}). Review the copies before applying."
            )

        scope_label = (client_group or name_glob or (f"all-{resource_type}" if resource_type else "selected"))
        proposed_plan_name = f"mcp-{scope_label}-{objective}".replace(" ", "-").lower()

        operations = [
            {"action": "create-new (proposed)",
             "target": f"plan '{proposed_plan_name}'",
             "detail": f"{len(compiled_copies)} copy/copies, {objective_spec['retentionDays']}-day retention",
             "blastRadius": "new object; affects nothing existing"},
            {"action": "create-new (proposed)",
             "target": f"protection group binding {scope['matchedCount']} subclient(s) to plan '{proposed_plan_name}'",
             "blastRadius": f"{scope['matchedCount']} subclient(s) would move to this plan"},
        ]

        token = _plan_token(scope, objective_spec, compiled_copies, proposed_plan_name)

        return {
            "status": "PREVIEW_ONLY",
            "planToken": token,
            "proposal": {
                "goal": {"client_group": client_group, "name_glob": name_glob,
                         "resource_type": resource_type, "objective": objective_spec["objective"],
                         "source_region": source_region, "target_region": target_region},
                "objectiveBadge": objective_spec["badge"],
                "retentionDays": objective_spec["retentionDays"],
                "rpoHours": objective_spec["rpoHours"],
                "proposedPlanName": proposed_plan_name,
                "copies": compiled_copies,
                "matchedCount": scope["matchedCount"],
                "sampleMatches": scope["matches"][:_SAMPLE_LIMIT],
                "truncatedMatches": scope["matchedCount"] > _SAMPLE_LIMIT,
                "operations": operations,
                "openQuestions": open_questions,
            },
            "resolution_basis": scope.get("resolution_basis"),
            "note": (
                "Preview only. No plan, copy, or protection group was created. This "
                "server cannot yet enact provisioning; hand this proposal (and its "
                "planToken) to a Commvault admin or the Command Center UI to apply."
            ),
        }
    except Exception as e:  # noqa: BLE001
        logger.error(f"Error planning protection: {e}")
        return {"status": "ERROR", "reason": str(e)}


PROTECTION_TOOLS = [
    resolve_scope,
    explain_objective,
    plan_protection,
]
