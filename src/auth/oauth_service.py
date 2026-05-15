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

from fastmcp.server.dependencies import get_access_token

from src.logger import logger


class OAuthService:

    def get_tokens(self):
        access_token = get_access_token()
        if access_token is None or not access_token.token:
            logger.error("Authentication validation failed: no upstream access token in request context")
            raise Exception("Authentication validation failed. Please relogin and try again.")
        return f"Bearer {access_token.token}", None
