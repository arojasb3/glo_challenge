# from google.cloud import bigquery
# client = bigquery.Client()
# bucket_name = 'my-bucket'
import os
from google.cloud import bigquery

def create_table_backup(table_id):
    """Taken from GCP docs"""
    client = bigquery.Client()
    project = os.environ['GCP_PROJECT']
    dataset_id = 'globant'
    bucket_name = os.environ['GCP_BUCKET'] + '/backups'
    destination_uri = "gs://{}/{}".format(bucket_name, f"{table_id}.avro")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="us-east1",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    return(
        "Exported table {}".format( table_id)
    )

