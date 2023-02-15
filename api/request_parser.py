import proto_message_pb2 as bqm
import datetime
import uuid
import json
import bigquery_manager
from typing import List
import logging
import os
from google.cloud import bigquery_storage

def parse_message(req, proto_class):
    proto = proto_class()
    for attributes in req.keys():
        try:
            setattr(proto, attributes, req[attributes])
        except AttributeError as e:
            raise e 
        except TypeError as e:
            raise e 
    return proto

def parse_wrong_record(req, error_class, table_name, error, datetime_utc):
    err = error_class()
    setattr(err, 'uuid', str(uuid.uuid4()))
    setattr(err,'content', json.dumps(req) )
    setattr(err,'table', table_name )
    setattr(err,'datetime', datetime_utc )
    setattr(err,'error', error )
    return err
    
    
def parse_request(request, proto_class, table_name):
    json_request = request.get_json()
    parsed_request = []
    bad_requests = []
    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    current_record = 1
    for row in json_request:
        try:
            parsed_request.append(parse_message(row, proto_class))
        except AttributeError as e:
            error_string = str(e)
            bad_requests.append(
                parse_wrong_record(
                    row,
                    bqm.Wrong_record,
                    table_name,
                    f"{error_string} on record number {current_record}",
                    current_time
                )
            )
            #return ({"msg": f"{error_string} on record number {current_record}"}), 400  
        except TypeError as e:
            error_string = str(e)
            bad_requests.append(
                parse_wrong_record(
                    row,
                    bqm.Wrong_record,
                    table_name,
                    f"{error_string} on record number {current_record}",
                    current_time
                )
            )
            #return ({"msg": f"{error_string} on record number {current_record}"}), 400
        
        current_record = current_record + 1
    return parsed_request, bad_requests
        
def send_parsed_messages_to_bq(
        request, proto_class, table_name: str
    ):
    logging.info('Starting request parser process...')
    good_rows, bad_rows = parse_request(request, proto_class, table_name)

    logging.info("Request parsed, starting to send data into BQ...")
    table_bq_writer = (
        bigquery_manager.BigqueryWriteManager(
            project_id = os.environ['GCP_PROJECT'],
            dataset_id = 'globant',
            table_id = table_name,
            bigquery_storage_write_client = bigquery_storage.BigQueryWriteClient(),
            pb2_descriptor = bqm.Job.DESCRIPTOR
        )
    )
    table_bq_writer.write_rows(good_rows)

    logging.info("Starting to send error data into BQ...")
    errors_bq_writer = (
        bigquery_manager.BigqueryWriteManager(
            project_id = os.environ['GCP_PROJECT'],
            dataset_id = 'globant',
            table_id = 'error_logs',
            bigquery_storage_write_client = bigquery_storage.BigQueryWriteClient(),
            pb2_descriptor = bqm.Wrong_record.DESCRIPTOR
        )
    )
    errors_bq_writer.write_rows(bad_rows)

    return len(good_rows), len(bad_rows)

class Dummy_request():
    def __init__(self, json_request):
        self.json_request = json_request
    
    def get_json(self):
        return self.json_requests



if __name__ == '__main__':
    # this are tests for the previous functions
    good_job_request = Dummy_request([
        {'id': 184, 'job': 'Test Job'},
        {'id': 185, 'job': 'Test Job'},
        {'id': 186, 'job': 'Test Job'},
        {'id': 187, 'job': 'Test Job'},
        {'id': 188, 'job': 'Test Job'}
    ])
    print(parse_request(good_job_request, bqm.Job))

    # or a bad request
    bad_job_request_attr_1 = Dummy_request([
        {'id': 184, 'job': 'Test Job'},
        {'id': 185, 'job': 'Test Job'},
        {'id': 186, 'job': 'Test Job'},
        {'idi': 187, 'job': 'Test Job'},
        {'id': 188, 'job': 'Test Job'}
    ])
    print(parse_request(bad_job_request_attr_1, bqm.Job))

    bad_job_request_attr_2 = Dummy_request([
        {'id': 184, 'job': 'Test Job'},
        {'id': 185, 'job': 'Test Job'},
        {'id': '186', 'job': 'Test Job'},
        {'id': 187, 'job': 'Test Job'},
        {'id': 188, 'job': 'Test Job'}
    ])

    print(parse_request(bad_job_request_attr_2, bqm.Job))