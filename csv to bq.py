

from google.cloud import bigquery
import function_framework


def hello_world(request):


    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Year", "INTEGER"),
        bigquery.SchemaField("Make", "STRING"),
        bigquery.SchemaField("Model", "STRING"),
        bigquery.SchemaField("Kilometres", "INTEGER"),
        bigquery.SchemaField("Body_Type", "STRING"),
        bigquery.SchemaField("Engine", "INTEGER"),
        bigquery.SchemaField("Transmission", "STRING"),
        bigquery.SchemaField("Drivetrain", "STRING"),
        bigquery.SchemaField("Exterior_Colour", "STRING"),
        bigquery.SchemaField("Interior_Colour", "STRING"),
        bigquery.SchemaField("Passengers", "INTEGER"),
        bigquery.SchemaField("Doors", "INTEGER"),
        bigquery.SchemaField("Fuel_Type", "STRING"),
        bigquery.SchemaField("City", "FLOAT"),
        bigquery.SchemaField("Highway", "FLOAT"),
        bigquery.SchemaField("Price", "INTEGER")


    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    )

    table_id = "savvy-container-384307.cloud_function.formatteddata"


    uri = "gs://test-bucket0012/formattedData.csv"

    load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
    )


    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)


    print("Loaded {} rows.".format(destination_table.num_rows))
    return f'check the results in the logs'

