import os
import json
import time
from requests_oauthlib import OAuth1Session
from singer_sdk import typing as th
from singer_sdk import Stream


class SearchStream(Stream):
    def __init__(self, tap=None, search=None):
        self._search = search
        self._search_results = None
        super().__init__(tap=tap)

    @property
    def name(self):
        """Return primary key dynamically based on user inputs."""
        return self._search["stream"]

    @property
    def primary_keys(self):
        return ["system_id"]

    # @property
    # def replication_key(self):
    #     """Return replication key dynamically based on user inputs."""
    #     result = self.config.get("replication_key")
    #     if not result:
    #         self.logger.warning("Danger: could not find replication key!")
    #     return result

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """

        data = self._load_saved_search()
        fields = data[0]["values"].keys()

        properties = [
            th.Property(field, th.StringType)
            for field in ["system_id", *fields]
        ]

        return th.PropertiesList(*properties).to_dict()

    def get_records(self, context):
        data = self._load_saved_search()

        for row in data:
            record = row["values"]
            record["system_id"] = time.time_ns()
            yield record

    def _load_saved_search(self):
        if self._search_results is not None:
            return self._search_results

        account_id = str(self.config.get('account_id') or os.environ.get('TAP_NETSUITE_ACCOUNT'))
        restlet_id = self.config.get('restlet_id') or os.environ.get('TAP_NETSUITE_RESTLET')
        url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script={restlet_id}&deploy=1"
        oauth = OAuth1Session(
            client_key=self.config.get('consumer_key') or os.environ.get('TAP_NETSUITE_CONSUMER_KEY'),
            client_secret=self.config.get('consumer_secret') or os.environ.get('TAP_NETSUITE_CONSUMER_SECRET'),
            resource_owner_key=self.config.get('token_key') or os.environ.get('TAP_NETSUITE_TOKEN_KEY'),
            resource_owner_secret=self.config.get('token_secret') or os.environ.get('TAP_NETSUITE_TOKEN_SECRET'),
            realm=account_id,
            signature_method="HMAC-SHA256",
        )

        resp = oauth.post(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"searchID": self._search["id"]})
        )

        self._search_results = json.loads(resp.text)["results"]
        return self._search_results
