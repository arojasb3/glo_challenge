"""Wrapper around BigQuery call taken from
https://dev.to/stack-labs/13-tricks-for-the-new-bigquery-storage-write-api-in-python-296e."""
from __future__ import annotations
from typing import Any, Iterable
import logging
from google.cloud import bigquery_storage
from google.cloud.bigquery_storage_v1 import exceptions as bqstorage_exceptions

from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import Descriptor



class DefaultStreamManager:  # pragma: no cover
    """Manage access to the _default stream write streams."""

    def __init__(
        self,
        table_path: str,
        message_protobuf_descriptor: Descriptor,
        bigquery_storage_write_client: bigquery_storage.BigQueryWriteClient,
    ):
        """Init."""
        self.stream_name = f"{table_path}/_default"
        self.message_protobuf_descriptor = message_protobuf_descriptor
        self.write_client = bigquery_storage_write_client
        self.append_rows_stream = None

    def _init_stream(self):
        """Init the underlying stream manager."""
        # Create a template with fields needed for the first request.
        request_template = types.AppendRowsRequest()
        # The initial request must contain the stream name.
        request_template.write_stream = self.stream_name
        # So that BigQuery knows how to parse the serialized_rows, generate a
        # protocol buffer representation of our message descriptor.
        proto_schema = types.ProtoSchema()
        proto_descriptor = descriptor_pb2.DescriptorProto()  # pylint: disable=no-member
        self.message_protobuf_descriptor.CopyToProto(proto_descriptor)
        proto_schema.proto_descriptor = proto_descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = proto_schema
        request_template.proto_rows = proto_data
        # Create an AppendRowsStream using the request template created above.
        self.append_rows_stream = writer.AppendRowsStream(
            self.write_client, request_template
        )

    def send_appendrowsrequest(
        self, request: types.AppendRowsRequest
    ) -> writer.AppendRowsFuture:
        """Send request to the stream manager. Init the stream manager if needed."""
        try:
            if self.append_rows_stream is None:
                self._init_stream()
            return self.append_rows_stream.send(request)
        except bqstorage_exceptions.StreamClosedError:
            # the stream needs to be reinitialized
            self.append_rows_stream.close()
            self.append_rows_stream = None
            raise

    # Use as a context manager

    def __enter__(self) -> DefaultStreamManager:
        """Enter the context manager. Return the stream name."""
        self._init_stream()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the context manager : close the stream."""
        if self.append_rows_stream is not None:
            # Shutdown background threads and close the streaming connection.
            self.append_rows_stream.close()


class BigqueryWriteManager:
    """Encapsulation for bigquery client."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        bigquery_storage_write_client: bigquery_storage.BigQueryWriteClient,
        pb2_descriptor: Descriptor,
    ):  # pragma: no cover
        """Create a BigQueryManager."""
        self.bigquery_storage_write_client = bigquery_storage_write_client

        self.table_path = self.bigquery_storage_write_client.table_path(
            project_id, dataset_id, table_id
        )
        self.pb2_descriptor = pb2_descriptor

    def write_rows(self, pb_rows: Iterable[Any]) -> None:
        """Write data rows."""
        with DefaultStreamManager(
            self.table_path, self.pb2_descriptor, self.bigquery_storage_write_client
        ) as target_stream_manager:
            proto_rows = types.ProtoRows()
            # Create a batch of row data by appending proto2 serialized bytes to the
            # serialized_rows repeated field.
            for row in pb_rows:
                proto_rows.serialized_rows.append(row.SerializeToString())
            # Create an append row request containing the rows
            request = types.AppendRowsRequest()
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.rows = proto_rows
            request.proto_rows = proto_data

            future = target_stream_manager.send_appendrowsrequest(request)

            # Wait for the append row requests to finish.
            print(future.result())
