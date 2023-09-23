import pandas as pd
import pandas_gbq
import json
import requests
from datetime import datetime
import functions_framework
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-1470391734037"
DATASET_ID = "ghg"
TABLE_STG_ID = "cw_data_stg"
TABLE_TARGET_ID = "cw_data"

table_id_stg = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_STG_ID}"
table_id_target = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_TARGET_ID}"

gbq_client = bigquery.Client()

data_columns = [
    {"source_name": "id", "gbq_name": "id", "gbq_type": "INT64", "gbq_mode": "REQUIRED"},
    {"source_name": "iso_code3", "gbq_name": "iso_code3", "gbq_type": "STRING", "gbq_mode": "REQUIRED"},
    {"source_name": "sector", "gbq_name": "sector", "gbq_type": "STRING", "gbq_mode": "REQUIRED"},
    {"source_name": "unit", "gbq_name": "unit", "gbq_type": "STRING", "gbq_mode": "NULLABLE"},
    {"source_name": "emissions.year", "gbq_name": "year", "gbq_type": "INT64", "gbq_mode": "REQUIRED"},
    {"source_name": "emissions.value", "gbq_name": "value", "gbq_type": "FLOAT64", "gbq_mode": "REQUIRED"},
    {"source_name": "upload_timestamp_utc", "gbq_name": "upload_timestamp_utc", "gbq_type": "TIMESTAMP",
     "gbq_mode": "NULLABLE"}
]
columns_to_be_uploaded = [item["source_name"] for item in data_columns]
schema_df = [{'name': i['gbq_name'], 'type': i['gbq_type'], 'mode': i['gbq_mode']} for i in data_columns]


@functions_framework.http
def load_data(request):
    request_json = request.get_json()

    start_year = request_json['start'] if 'start' in request_json else datetime.now().year
    end_year = request_json['end'] if 'end' in request_json else datetime.now().year
    regions = request_json['regions'] if 'regions' in request_json else None

    # Get the data from Climate Watch API
    url = 'https://www.climatewatchdata.org/api/v1/data/historical_emissions'
    payload = {"data_sources": 190,
               "gases": 441,
               "start_year": start_year,
               "end_year": end_year}
    if regions:
        payload["regions"] = regions

    r = requests.get(url, params=payload)
    data = r.json()['data']

    while r.links.get('next'):
        r = requests.get(r.links['next']['url'], params=payload)
        data.extend(r.json()['data'])

    df = pd.DataFrame.from_dict(data)

    # Filter the data (Climate Watch API is not working properly)
    df = df.loc[(df['data_source'] == "Climate Watch") & (df['gas'] == "All GHG")]
    df.drop(columns=['country', 'data_source', 'gas'], inplace=True)

    # Flatten the nested list
    df = df.explode("emissions")
    df = pd.concat(
        [df[['id', 'iso_code3', 'sector', 'unit']].reset_index(drop=True), pd.json_normalize(df["emissions"])],
        axis=1)
    df['upload_timestamp_utc'] = pd.Timestamp.now(tz='UTC')  # Add a timestamp to the dataframe

    # Insert the data to the Google BigQuery
    try:
        gbq_client.get_table(table_id_target)
    except NotFound:
        df.to_gbq(destination_table='{}.{}'.format(DATASET_ID, TABLE_TARGET_ID),
                  project_id=PROJECT_ID,
                  table_schema=schema_df)  # Load the data directly to the target table if it does not exist
        print(f"Data inserted to {DATASET_ID}.{TABLE_TARGET_ID}")
    else:
        df.to_gbq(destination_table='{}.{}'.format(DATASET_ID, TABLE_STG_ID),
                  project_id=PROJECT_ID,
                  if_exists='replace',
                  table_schema=schema_df)  # Load the data to the staging table
        print(f"Data inserted into {DATASET_ID}.{TABLE_STG_ID}")

        # Merge the staged data with the target table data
        query_merge = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_TARGET_ID}` T
            USING `{PROJECT_ID}.{DATASET_ID}.{TABLE_STG_ID}` S
            ON      T.id = S.id
                AND T.year = S.year
            WHEN MATCHED THEN
                UPDATE SET
                    T.unit = S.unit,
                    T.value = S.value,
                    T.upload_timestamp_utc = S.upload_timestamp_utc
            WHEN NOT MATCHED THEN
                INSERT ROW
        """
        gbq_client.query(query_merge)  # Make an API request.
        print(f"Data from {DATASET_ID}.{TABLE_STG_ID} merged with {DATASET_ID}.{TABLE_TARGET_ID}")
