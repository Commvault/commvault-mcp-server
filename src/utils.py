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

import os
from typing import Optional
from dotenv import load_dotenv
import json
from pathlib import Path

load_dotenv()

def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    value = get_json_config_var(var_name) or os.getenv(var_name, default) #json config is used for arlie lite mode
    if value is None:
        raise ValueError(f"Please check if you have set all the environment/configuation variables {var_name}. You can run the setup script to set them.")
    return value

def get_json_config_var(var_name: str) -> str:
    try:
        config_path = Path(__file__).resolve().parents[2] / "config.json"
        with open(config_path, "r") as f:
            config = json.load(f)
        var_name = var_name.lower()
        return config.get(var_name)
    except Exception as e:
        return None
