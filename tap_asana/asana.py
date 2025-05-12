import asana
import singer
import requests

LOGGER = singer.get_logger()


""" Simple wrapper for Asana. """


class Asana():
    """Base class for tap-asana"""

    def __init__(
        self, client_id, client_secret, redirect_uri, refresh_token, access_token=None
    ):  # pylint: disable=too-many-arguments
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.refresh_token = refresh_token
        self.access_token = access_token
        self._client = self._access_token_auth()
        # self.refresh_access_token()

    def _access_token_auth(self):
        """Check for access token"""
        if self.access_token is None:
            self.access_token = self.refresh_access_token()

        if self.access_token:
            try:
                configuration = asana.Configuration()
                configuration.access_token = self.access_token
                return asana.ApiClient(configuration)
            except Exception as e:
                LOGGER.error(f"Error creating Asana client: {e}")
                return None

    def refresh_access_token(self):
        """Get the access token using the refresh token"""
        url = "https://app.asana.com/-/oauth_token"
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "refresh_token": self.refresh_token,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        try:
            response = requests.post(url, data=payload, headers=headers)

            if response.status_code == 200:
                LOGGER.debug("Access token refreshed successfully.")
                if "access_token" in response.json():
                    self.access_token = response.json()["access_token"]
                    return response.json()["access_token"]
            return None
        except Exception as e:
            LOGGER.error(f"Failed to refresh access token: {e}")
            return None

    @property
    def client(self):
        return self._client
