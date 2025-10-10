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

# Wrappers for Hypervisor related functionalities.
def filter_hypervisor_list_response(response):
    """
    Filters the hypervisor list response to return only relevant information.
    """
    filtered_clients = []
    for item in response.get("Hypervisors", []):
        filtered_clients.append({
            "clientName": item.get("name"),
            "clientId": item.get("id"),
            "description": item.get("description"),
            "vendor": item.get("HypervisorType"),
            "instanceId": item.get("instance", {}).get("id"),
            "instanceName": item.get("instance", {}).get("name"),
        })
    return {"hypervisors": filtered_clients}