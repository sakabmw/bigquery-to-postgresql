import psycopg2
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from io import StringIO
from datetime import datetime as dt

# PostgreSQL database connection parameters and table name
host_name       = "{host_name}"
database_name   = "{database_name}"
user_name       = "{user_name}"
password        = "{password}"
schema_name     = "{schema_name}"
table_name_pg   = "{table_name_pg}"
param_dict = {
    "host"      : "%s" % (host_name),
    "database"  : "%s" % (database_name),
    "user"      : "%s" % (user_name),
    "password"  : "%s" % (password)
}
table_pg = "%s.%s.%s" % (database_name, schema_name, table_name_pg) # Target table in PostgreSQL

# BigQuery credentials and table ID
key_path        = "path/to/credentials.json"
client_bigquery = bigquery.Client.from_service_account_json(key_path)
credentials     = service_account.Credentials.from_service_account_file(key_path)
project_id      = "{project_id}"
dataset_name    = "{dataset_name}"
table_name_bq   = "{table_name_bq}"
table_bq        = "%s.%s.%s" % (project_id, dataset_name, table_name_bq) # Data source to be ingested to the PostgreSQL table
sorting_column  = "{sorting_column}" # One column in the BigQuery table to sort the data in ord

# Query to get data from the BigQuery table
# Since the query is using limit and offset, it has to be sorted to ensure 
# the offsetting process takes all rows which have been queried
query = """
select * 
from {table_bq} 
order by {sorting_column}
limit {limit} offset {offset}
"""

# Function to establish the connection to the database
def connect(param_dict):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # Connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**param_dict)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

time_start = dt.now() # Start time of the whole process
print(time_start)

conn = connect(param_dict) # Connect to the database

table   = client_bigquery.get_table(table_bq)
rows    = table.num_rows # Get the number of rows
limit   = 100000 # Act as a chunksize
chunks  = range(0, rows, limit)

for n, offset in enumerate(chunks):
    time_start_ing = dt.now() # Start time of querying and ingesting one dataframe
    print(time_start_ing)
    print("Query chunk", n)
    df = pandas_gbq.read_gbq(
        query.format(table_bq=table_bq, sorting_column=sorting_column, limit=limit, offset=offset),
        credentials=credentials,
        progress_bar_type="tqdm"
    )
    print("Ingesting dataframe", n)
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep=",")
    buffer.seek(0)
    cursor = conn.cursor()
    cursor.copy_from(buffer, table_pg, sep=",")
    conn.commit()
    print("Dataframe", n, "has been ingested, processing time:", str(dt.now() - time_start_ing))

print("Ingestion done")
cursor.close()
print(dt.now())
print("Processing time: " + str(dt.now() - time_start))