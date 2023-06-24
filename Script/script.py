from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import BlobServiceClient
from tenacity import retry, stop_after_attempt, wait_fixed
import csv
import pymssql

# Variables (Replace these with your own values)
tenant_id = "your_tenant_id"
client_id = "your_client_id"
client_secret = "your_client_secret"
subscription_id = "your_subscription_id"
storage_connection_string = "your_storage_connection_string"
storage_container_name = "your_storage_container_name"
resource_group_name = "your_resource_group_name"
datafactory_name = "your_datafactory_name"
pipeline_name = "your_pipeline_name"
database_server = "your_database_server"
database_name = "your_database_name"
table_prefix = "your_table_prefix"

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def trigger_adf_pipeline(adf_client, pipeline_name, table_name, blob_name):
    try:
        # Set up your parameters
        parameters = {
            'filename': blob_name,
            'table_name': table_name
        }

        # Trigger the pipeline
        run_response = adf_client.pipelines.create_run(
            resource_group_name,
            datafactory_name,
            pipeline_name,
            parameters=parameters  # Pass parameters using keyword argument
        )

        print(f"Triggered pipeline: {pipeline_name}, Run ID: {run_response.run_id}")
    except Exception as e:
        logging.error(f"Error occurred while triggering Azure Data Factory Pipeline: {pipeline_name}: {str(e)}")
        raise

def setup_azure_clients():
    adf_client = DataFactoryManagementClient(
        ClientSecretCredential(tenant_id, client_id, client_secret),
        subscription_id
    )

    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    return adf_client, blob_service_client

def infer_schema_from_csv_data(blob_data):
    # Read a sample of the CSV data to infer the column names and data types
    reader = csv.reader(blob_data.splitlines())
    column_names = next(reader) if reader else []
    
    # Assume all columns as NVARCHAR(MAX) for demonstration purposes
    column_types = ["NVARCHAR(MAX)"] * len(column_names)

    return column_names, column_types

def generate_create_table_query(table_name, column_names, column_types):
    # Generate the SQL query to create the table dynamically
    query = f"CREATE TABLE {table_name} (\n"
    columns = [f"[{name}] {data_type}" for name, data_type in zip(column_names, column_types)]
    query += ",\n".join(columns)
    query += "\n)"
    return query

def create_table_if_not_exists(table_name, blob_data):
    # Infer the schema from the CSV data
    column_names, column_types = infer_schema_from_csv_data(blob_data)

    # Establish a connection to the Azure SQL Database
    conn = pymssql.connect(server=database_server, user="your_db_username", password="your_db_password", database=database_name)
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    existing_table = cursor.fetchone()

    if existing_table:
        print(f"Table '{table_name}' already exists.")
    else:
        # Generate the CREATE TABLE query using the inferred schema
        create_table_query = generate_create_table_query(table_name, column_names, column_types)
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created.")

    conn.commit()
    conn.close()

def main():
    adf_client, blob_service_client = setup_azure_clients()

    # Fetch all blobs with prefix 'DATA_ID_'
    blob_list = blob_service_client.get_container_client(storage_container_name).list_blobs(name_starts_with='DATA_ID_')
    for blob in blob_list:
        print(f"Found blob: {blob.name}")
        if blob.name:
            table_name = table_prefix + blob.name.replace('.csv', '').replace('-', '_')  
# Generate the table name based on the blob name
            blob_client = blob_service_client.get_blob_client(container=storage_container_name, blob=blob.name)
            blob_data = blob_client.download_blob().content_as_text(encoding='utf-8', max_concurrency=16)

            create_table_if_not_exists(table_name, blob_data)  # Create the table if it doesn't exist
            
            trigger_adf_pipeline(adf_client, pipeline_name, table_name, blob.name)
        else:
            print("Blob name is empty.")
    print("Completed triggering pipelines")


if __name__ == "__main__":
    main()
