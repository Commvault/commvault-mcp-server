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
MCP resources for the Commvault MCP Server.

Resources are read-first context the model can pull BEFORE acting, so it grounds
itself in the domain instead of interrogating the user for vocabulary or guessing
IDs and region strings (which produces the "wizard" feel):

  - commvault://glossary     : domain vocabulary + what 3-2-1 means here.
  - commvault://capabilities : an HONEST list of what this server can/can't do.
  - commvault://regions      : real region strings, enumerated from storage pools.
  - commvault://objectives   : supported protection objectives, fully expanded.

Static read-only context like this never triggers destructive-action gating.
"""

import json

from src.cv_api_client import commvault_api_client
from src.logger import logger
from src.tools.protection_tools import OBJECTIVE_CATALOG, _expand_objective
from src.wrappers import filter_storage_pool_response


_GLOSSARY = """\
# Commvault glossary (for assistants)

Use this to speak the user's language and pick the right tool. Commvault models
"how/when/where to protect" and "what is protected" as **separate objects** -- that
separation is why setup can feel like re-entering information; explain outcomes,
not the object graph.

## Core entities
- **CommCell** - the whole Commvault environment managed by one CommServe.
- **Client** - a protected machine/server (physical, virtual, or a cloud account).
- **Client group** - a named set of clients. In this server it is the practical
  stand-in for an environment like "prod" or "staging" (there are no free-form tags).
- **Subclient** - the unit of protected content on a client (the actual thing that
  gets backed up). A client can have several subclients.
- **Backupset** - a container of subclients for an agent on a client.
- **Plan** - the policy that says how/when/where to protect: schedules, retention,
  and which storage to copy to. The modern replacement for storage policies.
- **Storage policy / copy** - the older policy model; a "copy" is one retained
  instance of the data on a storage target. A plan compiles down to copies.
- **Storage pool** - the physical/cloud storage a copy lands on. Pools carry a
  **region**, which is how regional protection is actually expressed.
- **Protection group** - the binding of subclients to a plan so they get protected.

## What "3-2-1" means here
3 copies of the data, on >=2 different media (e.g. disk + cloud), with >=1 copy
kept offsite in a different region. In Commvault terms: a plan with a primary copy
plus secondary/offsite copies on distinct storage pools, at least one of which is
in a second region. Use `explain_objective` to see the exact copy topology.

## Picking a tool
- "What does 'prod' cover?" -> `resolve_scope`.
- "What does 3-2-1 actually mean?" -> `explain_objective`.
- "Protect X with 3-2-1, copy region A to B" -> `plan_protection` (returns a
  reviewed proposal; it does not create anything).
- Day-to-day reads (jobs, plans, storage, users) -> the `get_*` tools.
"""

_CAPABILITIES = """\
# What this server can and cannot do (be honest with the user)

A confident-but-wrong "done" on a backup product is worse than saying "I can't do
that here." Stay inside these lines.

## Can do today
- **Discover**: list and inspect clients, client groups, subclients, plans,
  storage policies/pools/libraries, schedules, jobs, users, roles, and CommCell
  posture (SLA, security score, storage utilisation).
- **Preview protection**: `resolve_scope`, `explain_objective`, and
  `plan_protection` turn a goal into a reviewed plan-of-record. These are
  **read-only** -- they change nothing.
- **Operate existing protection**: run a backup on an existing subclient; suspend/
  resume/resubmit/kill jobs; enable/disable schedules; enable/disable users; send
  logs. (Some are destructive -- confirm first.)

## Cannot do yet
- **Create plans, protection groups, or subclients.** There is no provisioning/
  create path. `plan_protection` produces a proposal for a human or the Command
  Center UI to enact; it never creates anything itself.
- **Select resources by free-form tag.** The read APIs expose client name/ID,
  host, and application type only. Use a **client group** as the stand-in for
  prod/staging.
- **Select resources by their own region.** Region is known for storage **pools**,
  not for protected resources. "Regional protection" therefore means placing a
  **copy** in a target region (set on the plan), not filtering resources by where
  they live.

## General Guidance
Resolve names to IDs yourself with the read tools; report outcomes, not raw API
dumps; confirm before any destructive action; if a scope resolves to zero, stop
and say so.
"""


def _regions_payload() -> dict:
    """Enumerate the distinct regions known to this CommCell, from storage pools."""
    try:
        resp = commvault_api_client.get("StoragePool")
        pools = filter_storage_pool_response(resp).get("storagePools", [])
    except Exception as e:  # noqa: BLE001 - resource read should degrade, not crash
        logger.warning(f"commvault://regions could not list storage pools: {e}")
        return {
            "regions": [],
            "error": f"Could not enumerate regions from storage pools: {e}",
            "note": "Region data comes from storage pools; none could be read.",
        }
    seen = {}
    for p in pools:
        name = p.get("regionName")
        if name and name not in seen:
            seen[name] = {"regionName": name, "regionDisplayName": p.get("regionDisplayName")}
    return {
        "regions": sorted(seen.values(), key=lambda r: r["regionName"]),
        "note": ("Use the exact 'regionName' value when calling plan_protection / "
                 "explain_objective. Regions are derived from storage pools, so only "
                 "regions with storage appear here."),
    }


def _objectives_payload() -> dict:
    """Expand the supported protection objectives for grounding."""
    return {
        "objectives": [_expand_objective(name) for name in sorted(OBJECTIVE_CATALOG)],
        "note": "These are the objectives explain_objective and plan_protection accept.",
    }


def register_resources(mcp_server) -> None:
    """Register read-only grounding resources on the MCP server."""

    @mcp_server.resource(
        "commvault://glossary",
        name="Commvault glossary",
        description="Domain vocabulary (plan, subclient, copy, storage pool, ...) and what terms like '3-2-1' means here.",
        mime_type="text/markdown",
    )
    def glossary() -> str:
        return _GLOSSARY

    @mcp_server.resource(
        "commvault://capabilities",
        name="Commvault MCP capabilities",
        description="Honest list of what this server can and cannot do (e.g. it cannot create storage pools yet).",
        mime_type="text/markdown",
    )
    def capabilities() -> str:
        return _CAPABILITIES

    @mcp_server.resource(
        "commvault://regions",
        name="Available storage regions",
        description="Real region strings enumerated from storage pools, for use in protection planning.",
        mime_type="application/json",
    )
    def regions() -> str:
        return json.dumps(_regions_payload(), indent=2)

    @mcp_server.resource(
        "commvault://objectives",
        name="Supported protection objectives",
        description="Supported protection objectives (3-2-1, 2-copy, 1-copy), fully expanded into copies.",
        mime_type="application/json",
    )
    def objectives() -> str:
        return json.dumps(_objectives_payload(), indent=2)

    logger.info("Registered Commvault resources: glossary, capabilities, regions, objectives")
