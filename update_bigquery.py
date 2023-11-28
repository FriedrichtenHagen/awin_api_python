# Function to update BigQuery table
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery


def update_bigquery(df):
    client = bigquery.Client()

    # Configure your BigQuery settings
    DATASET_ID = os.getenv("DATASET_ID")
    TABLE_ID = os.getenv("TABLE_ID")
    PROJECT_ID = os.getenv("PROJECT_ID")
    CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # Set the environment variable for the service account key file path
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

    try:
        dataset_ref = client.dataset(DATASET_ID, project=PROJECT_ID)
        
        # Convert 'date' column to datetime if not already
        df['transactionDate'] = pd.to_datetime(df['transactionDate'])
        # Replace dots in column names with underscores
        df.columns = [col.replace('.', '_') for col in df.columns]
        # Fetch existing data from BigQuery
        try:
            existing_data = client.query(f'SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`').to_dataframe()
        except Exception as e:
            print(f"Error fetching existing data from BigQuery: {e}")
            existing_data = pd.DataFrame()

        # Identify new transactions and changed rows
        if not existing_data.empty:
            # rows with ids that do not currently exist in bq
            new_data = df[~df['id'].isin(existing_data['id'])]
            # rows that exist in bq. These will be overwritten. 
            changed_data = df[df['id'].isin(existing_data['id'])]

            # Update existing rows
            for idx, row in changed_data.iterrows():
                existing_row = existing_data[existing_data['id'] == row['id']]
                # the matching existing row is not empty
                if not existing_row.empty:
                    # print(f"existing_row.index length: {len(existing_row.index)}")
                    # print(f"row length: {len(row)}")

                    # Ensure that 'row' is in the same shape as 'existing_row'
                    row_as_dataframe = pd.DataFrame(row).transpose()
                    # print(f"existing_row.index length: {len(existing_row.index)}")
                    # print(f"row_as_dataframe length: {len(row_as_dataframe)}")

                     # Convert data types to match existing_row
                    row_as_dataframe = row_as_dataframe.astype(existing_row.dtypes.to_dict())

                    # Set the index to match existing_row
                    row_as_dataframe.index = existing_row.index

                    # update each column of the existing row
                    for col in existing_data.columns:
                                existing_data.at[existing_row.index[0], col] = row_as_dataframe[col].iloc[0]
        else:
            # If existing_data is empty, consider all data as new
            new_data = df.copy()
            changed_data = pd.DataFrame() 

        # do a check for the amount of rows replaced and added
        print(f'rows updated: {existing_data.shape[0]}')
        print(f'rows added: {new_data}')
        # Append new rows
        updated_data = pd.concat([existing_data, new_data], ignore_index=True, sort=False)

        # Update BigQuery
        if not updated_data.empty:
            
            # create staging table and then replace existing table
            # The temporary table name is hardcoded as 'temp_table'. If you're running this script concurrently
            # or periodically, you might want to generate a unique name for the temporary table to avoid conflicts.
            temp_table_ref = client.dataset(DATASET_ID).table('temp_table')
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE'  # This overwrites the existing data
            )

            client.load_table_from_dataframe(updated_data, temp_table_ref, job_config=job_config).result()

            # Use a SQL query to overwrite the target table from the temporary table
            sql = f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS
                SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.temp_table`
            """

            client.query(sql).result()
            
            num_rows = updated_data.shape[0]
            num_columns = updated_data.shape[1]
            print(f'Number of rows updated: {num_rows}. \n Number of columns: {num_columns}')
        else:
            print('No rows were updated.')
    except Exception as e:
        raise RuntimeError(f"Error updating BigQuery table: {e}")
    