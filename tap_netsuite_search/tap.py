"""NetsuiteSearch tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_netsuite_search.streams import SearchStream

class TapNetsuiteSearch(Tap):
    """NetsuiteSearch tap class."""
    name = "tap-netsuite-search"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "restlet_id",
            th.StringType,
        ),
        th.Property(
            "account_id",
            th.StringType,
        ),
        th.Property(
            "consumer_key",
            th.StringType,
        ),
        th.Property(
            "consumer_secret",
            th.StringType,
        ),
        th.Property(
            "token_key",
            th.StringType,
        ),
        th.Property(
            "token_secret",
            th.StringType,
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [
            SearchStream(tap=self, search=search)
            for search in self.config.get("searches")
        ]
