"""EFD connection utility class."""

import logging
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import requests
from astropy.time import Time

__all__ = [
    "EfdClient",
]


class EfdClient:
    """An EFD client to retrieve simple EFD data.

    Parameters
    ----------
    efd_instance : `str`, optional
        EFD instance name to connect to.
    log : `logging.Logger`, optional
        Log to write messages to.
    """

    def __init__(self, efd_instance: str = "usdf_efd", log: logging.Logger | None = None) -> None:
        self.log = log if log else logging.getLogger(__name__)

        auth_dict = self._get_auth(efd_instance)
        self._auth = (auth_dict["username"], auth_dict["password"])
        self._databaseName = "efd"
        self._databaseUrl = urljoin(f"https://{auth_dict['host']}", auth_dict["path"])

    def _get_auth(self, instance_alias: str) -> dict:
        """Get authorization credentials.

        Parameters
        ----------
        instance_alias : `str`
            EFD instance to get credentials for.

        Returns
        -------
        credentials : `dict` [`str`, `str`]
            A dictionary of authorization credentials, including at
            least these key/value pairs:

            ``"username"``
                Login username.
            ``"password"``
                Login passwords.
            ``"host"``
                Host to connect to.
            ``"path"``
                Directory path for EFD instance.

        Raises
        ------
        RuntimeError :
            Raised if the HTTPS request fails.

        Notes
        -----
        This authentication method should be replaced by Vault secrets
        for services, or authentication tokens for notebooks, as
        segwarides is to be deprecated (https://dmtn-250.lsst.io/#efd).
        """
        service_endpoint = "https://roundtable.lsst.codes/segwarides/"
        url = urljoin(service_endpoint, f"creds/{instance_alias}")
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            return response.json()
        else:
            raise RuntimeError(f"Could not connect to {url}")

    def query(self, query: str) -> dict:
        """Execute an EFD query.

        Parameters
        ----------
        query : `str`
            Query to run.

        Returns
        -------
        results : `dict`
            Dictionary of results returned.

        Raises
        ------
        RuntimeError :
            Raised if the database could not be read from.
        """
        params = {
            "db": self._databaseName,
            "q": query,
        }

        try:
            response = requests.get(
                f"{self._databaseUrl}/query",
                params=params,
                auth=self._auth,
                timeout=(30, 120),
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Could not read data from database with query: {query}") from e

    def get_fields(self, topic_name: str) -> list:
        """Get fields for a topic.

        Parameters
        ----------
        topic_name : `str`
            Topic to get datatypes for

        Returns
        -------
        fields : `list` [`str`]
            List of field names.
        """
        dtypes = self.get_schema_dtypes(topic_name)
        return [fi[0] for fi in dtypes]

    def get_schema_dtypes(self, topic_name: str) -> list:
        """Get datatypes for a topic.

        Parameters
        ----------
        topic_name : `str`
            Topic to get datatypes for

        Returns
        -------
        dtype : `list` [`tuple` [`str`, `str`]]
            List of tuples of field names and data types.
        """
        query = f'SHOW FIELD KEYS FROM "{topic_name}"'
        data = self.query(query)

        values = data["results"][0]["series"][0]["values"]

        dtype = [("time", "str")]
        for field_name, field_type in values:
            if field_type == "float":
                field_dtype = np.float64
            elif field_type == "integer":
                field_dtype = np.int64
            elif field_type == "string":
                field_dtype = "str"
            else:
                field_dtype = "object"
            dtype.append((field_name, field_dtype))

        return dtype

    def to_dataframe(self, series: dict) -> pd.DataFrame:
        data = pd.DataFrame(series.get("values", []), columns=series["columns"])
        if "time" not in data.columns:
            return data
        data: pd.DataFrame = data.set_index(pd.to_datetime(data["time"])).drop("time", axis=1)
        # If index is not time-zone aware: df.index.tz_localize('UTC')
        data.index.name = None
        if "tags" in series:
            for k, v in series["tags"].items():
                data[k] = v
        if "name" in series:
            data.name = series["name"]
        return data

    def select_time_series(
        self,
        topic_name: str,
        fields: list | None = None,
        start_time: Time | None = None,
        end_time: Time | None = None,
        sal_index: int | None = None,
    ) -> pd.DataFrame:
        """Query a topic for a time series.

        Parameters
        ----------
        topic_name : `str`
            Database "topic" to query.
        fields : `list` or None, optional
            List of fields to return.  If empty, all fields are
            returned.
        start_time : `astropy.time.Time` or None, optional
            Start date (in UTC) to limit the results returned.
        end_time : `astropy.time.Time` or None, optional
            End date (in UTC) to limit the results returned.
        sal_index : `int` or None, optional
            Restrict query to a particular salIndex.

        Returns
        -------
        table : `astropy.table.Table`
            A table containing the fields requested, with each row
            corresponding to one date (available in the ``"time"``
            column).
        """
        query = "SELECT "

        if fields is None:
            query += "*"
        else:
            query += ",".join(fields)

        query += f' FROM "{topic_name}"'

        if start_time is not None or end_time is not None or sal_index is not None:
            query += " WHERE"

        if start_time is not None:
            query += f" time >= '{start_time.utc.isot}Z'"
            if end_time is not None:
                query += " AND"
        if end_time is not None:
            query += f" time <= '{end_time.utc.isot}Z'"

        if sal_index is not None:
            if start_time is not None or end_time is not None:
                query += " AND"
            query += f" salIndex = {sal_index}"

        # There are probably ways to get different kinds of query results
        # But for our simple queries .. I think this is probably ok.
        data = self.query(query)["results"][0]

        # If no data, return empty dataframe
        if "series" not in data:
            return pd.DataFrame()

        series = data["series"][0]

        return self.to_dataframe(series)
