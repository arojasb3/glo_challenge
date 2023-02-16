"""Based of GCP's official docs"""
import os
from google.cloud import bigquery

def create_table_backup(table_id):
    """Taken from GCP docs"""
    client = bigquery.Client()
    project = os.environ['GCP_PROJECT']
    dataset_id = 'globant'
    bucket_name = os.environ['GCP_BUCKET'] + '/backups'
    destination_uri = "gs://{}/{}".format(bucket_name, f"{table_id}")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="us-east1",
        job_config=bigquery.job.ExtractJobConfig(destination_format='AVRO')
    )
    extract_job.result()

    return(
        "Exported table {}".format(table_id)
    )

def restore_table(table_id):
    client = bigquery.Client()
    project = os.environ['GCP_PROJECT']
    dataset_id = 'globant'
    bucket_name = os.environ['GCP_BUCKET'] + '/backups'
    full_table_id = project+'.'+dataset_id+'.'+table_id
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.AVRO,
    )

    uri = 'gs://' + bucket_name + '/' + table_id

    load_job = client.load_table_from_uri(
        uri, full_table_id, job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(full_table_id)
    return("Table {} restored with {} rows.".format(table_id, destination_table.num_rows))

