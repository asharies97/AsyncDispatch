import os
import time
import requests

from urllib.parse import urljoin

from jose import jwt


class TokenService:
    client_id = os.getenv("AUTH_CLIENT_ID")
    client_secret = os.getenv("AUTH_CLIENT_SECRET")
    auth_url = os.getenv("AUTH_URL")
    audience = os.getenv("AUTH_AUDIENCE")

    def __init__(self):
        self.token = None

    @property
    def _is_valid(self):
        if not self.token:
            return False
        claims = jwt.get_unverified_claims(self.token)
        if time.time() > claims["exp"]:
            return False
        return True

    def _get_url(self):
        return urljoin(self.auth_url, "/oauth/token")

    def _retrieve_token(self, account: str):
        print("Hereeeee",self._get_url())
        resp = requests.post(
            self._get_url(),
            json={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "audience": self.audience,
                "grant_type": "client_credentials",
            }
        )
        self.token = resp.json().get("access_token")

    def get_token(self, account: str):
        if not self._is_valid:
            self._retrieve_token(account)
        return self.token