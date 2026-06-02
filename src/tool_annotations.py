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
MCP tool annotations for the Commvault MCP Server.

Annotations are behavioural HINTS the MCP host uses to decide how to treat a
tool: a client can call read-only tools freely (to resolve names to IDs, check
job status, preview scope) without bouncing the user for confirmation, and gate
ONLY the genuinely irreversible actions (kill a running job, disable protection,
overwrite a vault). Without these hints a host must treat every call as
suspicious, which turns the conversation into a wall of confirmations -- the
"wizard" feel we are removing.

Default policy: any tool NOT listed in ``_MUTATING`` is treated as a safe,
read-only lookup against the live CommServe (``readOnlyHint=True``,
``openWorldHint=True``). New read tools therefore get sensible hints for free;
only state-changing tools need an explicit entry here.
"""

from mcp.types import ToolAnnotations

# State-changing tools. ``destructiveHint`` is only meaningful when the tool is
# not read-only; ``idempotentHint`` marks calls that are safe to repeat with the
# same arguments (re-issuing after a timeout won't compound the effect).
_MUTATING = {
    "run_backup_for_subclient":          dict(title="Run backup for a subclient",          destructiveHint=False, idempotentHint=False),
    "suspend_job":                       dict(title="Suspend a running job",               destructiveHint=True,  idempotentHint=True),
    "kill_job":                          dict(title="Kill a running job",                  destructiveHint=True,  idempotentHint=True),
    "resume_job":                        dict(title="Resume a job",                        destructiveHint=False, idempotentHint=True),
    "resubmit_job":                      dict(title="Resubmit a job",                      destructiveHint=False, idempotentHint=False),
    "enable_schedule":                   dict(title="Enable a schedule",                   destructiveHint=False, idempotentHint=True),
    "disable_schedule":                  dict(title="Disable a schedule (stops future backups)", destructiveHint=True, idempotentHint=True),
    "set_user_enabled":                  dict(title="Enable or disable a user",            destructiveHint=False, idempotentHint=True),
    "create_send_logs_job_for_a_job":    dict(title="Send logs for a job",                 destructiveHint=False, idempotentHint=False),
    "create_send_logs_job_for_commcell": dict(title="Send CommCell logs",                  destructiveHint=False, idempotentHint=False),
    "setup_docusign_backup_vault":       dict(title="Set up DocuSign backup vault",        destructiveHint=True,  idempotentHint=True),
    "trigger_docusign_backup":           dict(title="Trigger a DocuSign backup",           destructiveHint=False, idempotentHint=False),
    "schedule_docusign_backup":          dict(title="Schedule DocuSign backups",           destructiveHint=False, idempotentHint=True),
    "recover_docusign_envelope":         dict(title="Recover a DocuSign envelope",         destructiveHint=False, idempotentHint=False),
}

# Read-only tools that do NOT touch the live CommServe (pure local computation),
# so the model knows their result is stable rather than live external state.
_LOCAL_READONLY = {"explain_objective"}

# Friendlier display titles for selected read-only tools (otherwise the title is
# humanised from the function name, e.g. ``get_jobs_list`` -> "Get jobs list").
_READ_TITLES = {
    "resolve_scope": "Preview which resources a scope matches",
    "explain_objective": "Explain a protection objective (3-2-1, etc.)",
    "plan_protection": "Plan backup protection from a goal",
}


def _humanize(name: str) -> str:
    return name.replace("_", " ").strip().capitalize()


def annotations_for(tool_fn) -> ToolAnnotations:
    """Return the :class:`ToolAnnotations` for a tool function.

    Mutating tools get an explicit, conservative hint set; everything else is a
    safe read-only lookup (idempotent; live external state unless local-compute).
    """
    name = getattr(tool_fn, "__name__", "")

    if name in _MUTATING:
        spec = dict(_MUTATING[name])
        title = spec.pop("title", _humanize(name))
        return ToolAnnotations(
            title=title,
            readOnlyHint=False,
            openWorldHint=True,
            **spec,
        )

    return ToolAnnotations(
        title=_READ_TITLES.get(name, _humanize(name)),
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=name not in _LOCAL_READONLY,
    )
