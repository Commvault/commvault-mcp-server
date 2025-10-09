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

from fastmcp.server.dependencies import get_http_request

from src.logger import logger


class OAuthService:

    def get_tokens(self):
        request = get_http_request()
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            logger.error("Authentication validation failed")
            raise Exception("Authentication validation failed. Please relogin and try again.")
        return auth_header, None
