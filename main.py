"""
Write a Python program to export data from datastore to google cloud storage. import data from google cloud storage to bigquery

1. Export from datastore for kind "ds_kind" in the 'default' namespace to Google cloud storage bucket "gcs_bucket_name"
2. Delete table "bq_table_id" from bigquery dataset "bq_dataset_id"
3. Import data from google cloud storage bucket 'gcs_bucket_name' to bigquery dataset "bq_dataset_id" and table "bq_table_id"

"""

from google.cloud import bigquery
from google.cloud.datastore_admin_v1 import DatastoreAdminClient, EntityFilter

# Export Datastore entity kind to Google Cloud Storage
def export_datastore_to_gcs(project_id, kind, bucket_name):
    client = DatastoreAdminClient()

    output_url_prefix = f"gs://{bucket_name}"
    entity_filter = EntityFilter()
    entity_filter.kinds.append(kind)

    print(f"Exporting {kind} entities in default namespace to Cloud Storage bucket {bucket_name}.")
    op = client.export_entities(
        {"project_id": project_id, "output_url_prefix": output_url_prefix, "entity_filter": entity_filter}
    )
    response = op.result(timeout=300)
    
    print(response)
    return response.output_url


# Delete a BigQuery table
def delete_bigquery_table(dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        client.delete_table(table_ref)
        print(f"Deleted BigQuery table '{table_id}' from dataset '{dataset_id}'.")
    except Exception as e:
        print(f"An error occurred while deleting the table: {e}")


# Import data from Google Cloud Storage to BigQuery
def import_gcs_to_bigquery(bucket_name, dataset_id, table_id, timestamp):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.DATASTORE_BACKUP

    uri = f"gs://{bucket_name}/{timestamp}/all_namespaces/kind_{table_id}/all_namespaces_kind_{table_id}.export_metadata"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Imported data from Google Cloud Storage bucket '{bucket_name}' to BigQuery table '{table_id}'.")


# Main program
def main():
    kind = 'ds_kind'
    gcs_bucket_name = 'gcs_bucket_name'
    bq_dataset_id = 'bq_dataset_id'
    bq_table_id = 'bq_table_id'
    project_id = 'gcp-project-id'

    # Step 1: Export from Datastore to Google Cloud Storage
    output_url = export_datastore_to_gcs(project_id, kind, gcs_bucket_name)

    # Step 2: Delete BigQuery table
    delete_bigquery_table(bq_dataset_id, bq_table_id)

    #get "2023-06-24T19:21:06_77248" from output_url gs://gcs_bucket_name/2023-06-24T19:21:06_77248/2023-06-24T19:21:06_77248.overall_export_metadata
    timestamp = output_url.split('/')[3]

    # Step 3: Import from Google Cloud Storage to BigQuery
    import_gcs_to_bigquery(gcs_bucket_name, bq_dataset_id, bq_table_id, timestamp)


if __name__ == '__main__':
    main()
