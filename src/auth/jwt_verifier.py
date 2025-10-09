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

import time
from typing import cast

from authlib.jose.errors import JoseError
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp.server.auth import AccessToken


class CustomJWTVerifier(JWTVerifier):
    async def load_access_token(self, token: str) -> AccessToken | None:
        try:
            # Get verification key (static or from JWKS)
            verification_key = await self._get_verification_key(token)

            # Decode and verify the JWT token
            claims = self.jwt.decode(token, verification_key)

            # Extract client ID early for logging
            client_id = claims.get("client_id") or claims.get("sub") or "unknown"

            # Validate expiration
            exp = claims.get("exp")
            if exp and exp < time.time():
                self.logger.debug(
                    "Token validation failed: expired token for client %s", client_id
                )
                self.logger.info("Bearer token rejected for client %s", client_id)
                return None

            # Validate issuer - note we use issuer instead of issuer_url here because
            # issuer is optional, allowing users to make this check optional
            if self.issuer:
                if claims.get("iss") != self.issuer:
                    self.logger.debug(
                        "Token validation failed: issuer mismatch for client %s",
                        client_id,
                    )
                    self.logger.info("Bearer token rejected for client %s", client_id)
                    return None

            # Validate audience if configured
            if self.audience:
                aud = claims.get("aud")

                # Handle different combinations of audience types
                audience_valid = False
                if isinstance(self.audience, list):
                    # self.audience is a list - check if any expected audience is present
                    if isinstance(aud, list):
                        # Both are lists - check for intersection
                        audience_valid = any(
                            expected in aud for expected in self.audience
                        )
                    else:
                        # aud is a string - check if it's in our expected list
                        audience_valid = aud in cast(list, self.audience)
                else:
                    # self.audience is a string - use original logic
                    if isinstance(aud, list):
                        audience_valid = self.audience in aud
                    else:
                        audience_valid = aud == self.audience

                if not audience_valid:
                    self.logger.debug(
                        "Token validation failed: audience mismatch for client %s",
                        client_id,
                    )
                    self.logger.info("Bearer token rejected for client %s", client_id)
                    return None

            return AccessToken(
                token=token,
                client_id=str(client_id),
                scopes=self.required_scopes, # this will be verified in the CV API layer (workaround to support azure oauth)
                expires_at=int(exp) if exp else None,
                claims=claims,
            )

        except JoseError:
            self.logger.debug("Token validation failed: JWT signature/format invalid")
            return None
        except Exception as e:
            self.logger.debug("Token validation failed: %s", str(e))
            return None
