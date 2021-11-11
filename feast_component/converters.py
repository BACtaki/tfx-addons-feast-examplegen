import abc
from typing import Any, Dict, Optional

from google.cloud import bigquery
from tfx.extensions.google_cloud_big_query import utils


class _Converter(abc.ABC):
    """Takes in data as a dictionary of string -> value and returns the serialized form
    of whatever representation we want.
    """

    @abc.abstractmethod
    def RowToExampleBytes(self, instance: Dict[str, Any]) -> bytes:
        """Generate tf.Example bytes from dictionary.

        Args:
            instance (Dict[str, Any]): dictionary of data

        Returns:
            bytes: Serialized tf.SequenceExample
        """
        pass

    @abc.abstractmethod
    def RowToSequenceExampleBytes(self, instance: Dict[str, Any]) -> bytes:
        """Generate tf.SequenceExample bytes from dictionary.

        Args:
            instance (Dict[str, Any]): dictionary of data

        Returns:
            bytes: Serialized tf.SequenceExample
        """
        pass


class _BQConverter(_Converter):
    """Converter class for BigQuery source data"""

    def __init__(self, query: str, project: Optional[str]) -> None:
        client = bigquery.Client(project=project)
        # Dummy query to get the type information for each field.
        query_job = client.query("SELECT * FROM ({}) LIMIT 0".format(query))
        results = query_job.result()
        self._type_map = {}
        for field in results.schema:
            self._type_map[field.name] = field.field_type

    def RowToExampleBytes(self, instance: Dict[str, Any]) -> bytes:
        """Convert bigquery result row to tf example."""
        ex_pb2 = utils.row_to_example(self._type_map, instance)
        return ex_pb2.SerializeToString()

    def RowToSequenceExampleBytes(self, instance: Dict[str, Any]) -> bytes:
        raise NotImplementedError("SequenceExample not implemented yet.")
