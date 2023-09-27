import functions_framework
import traceback
import pandas as pd
import pandas_gbq

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


PROJECT_ID = "my-project-1470391734037"
DATASET = "ghg"
TABLE_STG = "fao_data_stg"
TABLE_TARGET = "fao_data"

table_id_stg = f"{PROJECT_ID}.{DATASET}.{TABLE_STG}"
table_id_target = f"{PROJECT_ID}.{DATASET}.{TABLE_TARGET}"

gcs_client = storage.Client()
gbq_client = bigquery.Client()
job_config = bigquery.LoadJobConfig()

data_columns = [
    {"csv_name": "Area Code (M49)", "gbq_name": "area_code", "gbq_type": "INT64", "gbq_mode": "REQUIRED"},
    {"csv_name": "Item Code", "gbq_name": "item_code", "gbq_type": "INT64", "gbq_mode": "REQUIRED"},
    {"csv_name": "Year", "gbq_name": "year", "gbq_type": "INT64", "gbq_mode": "REQUIRED"},
    {"csv_name": "Unit", "gbq_name": "unit", "gbq_type": "STRING", "gbq_mode": "NULLABLE"},
    {"csv_name": "Value", "gbq_name": "value", "gbq_type": "FLOAT64", "gbq_mode": "NULLABLE"},
    {"csv_name": "Flag", "gbq_name": "flag", "gbq_type": "STRING", "gbq_mode": "NULLABLE"},
    {"csv_name": "Note", "gbq_name": "note", "gbq_type": "STRING", "gbq_mode": "NULLABLE"},
    {"csv_name": "UTC Timestamp", "gbq_name": "upload_timestamp_utc", "gbq_type": "TIMESTAMP", "gbq_mode": "NULLABLE"}
]

schema = [bigquery.SchemaField(i['gbq_name'], i['gbq_type'], mode=i['gbq_mode']) for i in data_columns]
schema_df = [{'name': i['gbq_name'], 'type': i['gbq_type'], 'mode': i['gbq_mode']} for i in data_columns]
rename_df_dict = {i['csv_name']: i['gbq_name'] for i in data_columns}

note_edit = "Calculated entry"


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def trigger_load(cloud_event):
    data = cloud_event.data

    load_data(data)


def load_data(data):
    """
    Load data from Google Storage to Google BigQuery.

    :param data: Data about event that triggered the trigger_load function
    """
    bucket_name = data['bucket']
    file_name = data['name']

    try:
        df = pd.read_csv(f'gs://{bucket_name}/{file_name}')
        df = _transform_data(df)
        _upload_to_bigquery(df)
    except Exception:
        print('Error streaming file. Cause: %s' % (traceback.format_exc()))


def _extract_rows_by_condition(df_src: pd.DataFrame, conditions: list, remove: bool = False) -> pd.DataFrame:
    """
    Extracts rows from source dataframe based on conditions passed in conditions list. If the remove parameter is set
    to True, the extracted rows are deleted from the source dataframe.

    :param df_src: Source dataframe.
    :param conditions: List of conditions. Each list item is expected to be a dictionary with 'Element Code' and
    'Item Code' keys.
    :param remove: Boolean parameter representing whether the extracted rows should be deleted from the source
    dataframe or not.
    :return: Dataframe with rows extracted from the source dataframe.
    """
    # Create temporary dataframe and change types of its columns
    df_tmp = pd.DataFrame().reindex(columns=df_src.columns)
    df_tmp = df_tmp.astype(dtype=df_src.dtypes.to_dict())

    for cond in conditions:
        df_tmp = pd.concat([df_tmp, df_src[(df_src['Item Code'] == cond['Item Code']) &
                                           (df_src['Element Code'] == cond['Element Code'])]])
        if remove:
            df_src.drop(df_src[(df_src['Item Code'] == cond['Item Code']) &
                               (df_src['Element Code'] == cond['Element Code'])].index, inplace=True)

    return df_tmp


def _transform_data(df_w: pd.DataFrame) -> pd.DataFrame:
    """
    Data transformation procedure.

    :param df_w: DataFrame that should be transformed.
    :return: Transformed DataFrame.
    """
    # Keep only 'Emissions Totals' records from FAO Tier 1 source and drop description fields
    df_w = df_w[(df_w['Domain Code'] == "GT") & (df_w['Source Code'] == 3050)]
    df_w.drop(columns=['Domain Code', 'Domain', 'Source Code', 'Source',
                       'Area', 'Element', 'Item', 'Year Code', 'Flag Description'],
              inplace=True)

    # Transformation 1: Food Retail (CO2, CH4, N2O)
    conditions_1 = [
        {'Item Code': 6508, 'Element Code': 7273},  # Emissions (CO2)
        {'Item Code': 6508, 'Element Code': 724413},  # Emissions (CO2eq) from CH4 (AR5)
        {'Item Code': 6508, 'Element Code': 724313}  # Emissions (CO2eq) from N2O (AR5)
    ]
    df_trans_1 = _extract_rows_by_condition(df_w, conditions_1, remove=True)
    df_trans_1 = df_trans_1.groupby(['Area Code (M49)', 'Item Code', 'Year'], as_index=False)['Value'].sum()
    df_trans_1['Item Code'] = 4000001
    df_trans_1['Element Code'] = 723113
    df_trans_1['Unit'] = "kt"
    df_trans_1['Note'] = note_edit

    df_w = pd.concat([df_w, df_trans_1])

    # Check the units of extracted rows
    df_trans_1_errors = df_w[df_w['Unit'] != "kt"]
    if not df_trans_1_errors.empty:
        print("ERROR: There are some values in other units than kt!")

    print("First transformation completed successfully!")

    # Transformation 2: Food Retail (F-gases)
    conditions_2 = [
        {'Item Code': 6508, 'Element Code': 717815}
    ]
    df_trans_2 = _extract_rows_by_condition(df_w, conditions_2, remove=True)

    df_trans_2['Item Code'] = 4000002
    df_trans_2['Element Code'] = 723113
    df_trans_2['Note'] = note_edit

    df_w = pd.concat([df_w, df_trans_2])
    print("Second transformation completed successfully!")

    # Transformation 3: Energy not related to Agriculture
    # Items that are related to agriculture and IPCC Energy sector
    conditions_3 = [
        {'Item Code': 6994, 'Element Code': 723113},
        {'Item Code': 6504, 'Element Code': 723113},
        {'Item Code': 6997, 'Element Code': 723113},
        {'Item Code': 6999, 'Element Code': 723113},
        {'Item Code': 6510, 'Element Code': 723113},
        {'Item Code': 6507, 'Element Code': 723113},
        {'Item Code': 6506, 'Element Code': 723113},
        {'Item Code': 6505, 'Element Code': 723113},
        {'Item Code': 6815, 'Element Code': 723113},
        {'Item Code': 4000001, 'Element Code': 723113}
    ]
    df_trans_3 = _extract_rows_by_condition(df_w, conditions_3)
    df_trans_3 = df_trans_3.groupby(['Area Code (M49)', 'Element Code', 'Year'], as_index=False)['Value'].sum()
    df_trans_3.set_index(['Area Code (M49)', 'Year'], inplace=True)

    condition_energy = [{'Item Code': 6821, 'Element Code': 723113}]
    df_IPCC_energy = _extract_rows_by_condition(df_w, condition_energy)
    df_IPCC_energy.set_index(['Area Code (M49)', 'Year'], inplace=True)

    df_trans_3['Value'] = df_IPCC_energy['Value'].subtract(df_trans_3['Value'])
    df_trans_3.reset_index(inplace=True)

    df_trans_3['Item Code'] = 5000001
    df_trans_3['Element Code'] = 723113
    df_trans_3['Unit'] = "kt"
    df_trans_3['Note'] = note_edit

    df_w = pd.concat([df_w, df_trans_3])
    print("Third transformation completed successfully!")

    # Transformation 4: IPPU not related to Agriculture
    conditions_4 = [
        {'Item Code': 4000002, 'Element Code': 723113},
    ]
    df_trans_4 = _extract_rows_by_condition(df_w, conditions_4)
    df_trans_4.set_index(['Area Code (M49)', 'Year'], inplace=True)

    condition_ippu = [{'Item Code': 6817, 'Element Code': 723113}]
    df_IPCC_ippu = _extract_rows_by_condition(df_w, condition_ippu)
    df_IPCC_ippu.set_index(['Area Code (M49)', 'Year'], inplace=True)

    df_trans_4['Value'] = df_IPCC_ippu['Value'].subtract(df_trans_4['Value'])
    df_trans_4.reset_index(inplace=True)

    df_trans_4['Item Code'] = 5000002
    df_trans_4['Element Code'] = 723113
    df_trans_4['Unit'] = "kt"
    df_trans_4['Note'] = note_edit

    df_w = pd.concat([df_w, df_trans_4])
    print("Fourth transformation completed successfully!")

    # Transformation 5: Waste not related to Agriculture
    conditions_5 = [
        {'Item Code': 6991, 'Element Code': 723113},
    ]
    df_trans_5 = _extract_rows_by_condition(df_w, conditions_5)
    df_trans_5.set_index(['Area Code (M49)', 'Year'], inplace=True)

    condition_waste = [{'Item Code': 6818, 'Element Code': 723113}]
    df_IPCC_waste = _extract_rows_by_condition(df_w, condition_waste)
    df_IPCC_waste.set_index(['Area Code (M49)', 'Year'], inplace=True)

    df_trans_5['Value'] = df_IPCC_waste['Value'].subtract(df_trans_5['Value'])
    df_trans_5.reset_index(inplace=True)

    df_trans_5['Item Code'] = 5000003
    df_trans_5['Element Code'] = 723113
    df_trans_5['Unit'] = "kt"
    df_trans_5['Note'] = note_edit

    df_w = pd.concat([df_w, df_trans_5])
    print("Fifth transformation completed successfully!")

    # Prepare the DataFrame for BigQuery table
    df_w = df_w[df_w['Element Code'] == 723113]  # Keep only entries related to Emissions CO2eq
    df_w.drop(columns=['Element Code'], inplace=True)
    df_w['UTC Timestamp'] = pd.Timestamp.now(tz='UTC')  # Add a timestamp to the dataframe
    df_w.rename(columns=rename_df_dict, inplace=True)  # Rename the dataframe

    return df_w


def _upload_to_bigquery(df_w: pd.DataFrame):
    # Push the data to the Google BigQuery
    try:
        gbq_client.get_table(table_id_target)
    except NotFound:
        df_w.to_gbq(destination_table='{}.{}'.format(DATASET, TABLE_TARGET),
                    project_id=PROJECT_ID,
                    table_schema=schema_df)  # Load the data directly to the target table if it does not exist
        print(f"Data inserted into {DATASET}.{TABLE_TARGET}")
    else:
        df_w.to_gbq(destination_table='{}.{}'.format(DATASET, TABLE_STG),
                    project_id=PROJECT_ID,
                    if_exists='replace',
                    table_schema=schema_df)  # Load the data to the staging table
        print(f"Data inserted into {DATASET}.{TABLE_STG}")
        
        # Merge the staged data with the target table data
        query_merge = f"""
            MERGE `{PROJECT_ID}.{DATASET}.{TABLE_TARGET}` T
            USING `{PROJECT_ID}.{DATASET}.{TABLE_STG}` S
            ON      T.area_code = S.area_code
                AND T.item_code = S.item_code
                AND T.year = S.year
            WHEN MATCHED THEN
                UPDATE SET
                    T.unit = S.unit,
                    T.value = S.value,
                    T.flag = S.flag,
                    T.note = S.note,
                    T.upload_timestamp_utc = S.upload_timestamp_utc
            WHEN NOT MATCHED THEN
                INSERT ROW
        """
        gbq_client.query(query_merge)  # Make an API request.
        print(f"Data from {DATASET}.{TABLE_STG} merged with {DATASET}.{TABLE_TARGET}")
