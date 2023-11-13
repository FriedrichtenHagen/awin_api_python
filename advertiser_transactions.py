import requests
import os
from dotenv import load_dotenv
import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
# Load environment variables from the .env file
load_dotenv()

# Access environment variables
API_KEY = os.getenv("API_KEY")
ADVERTISER_ID = os.getenv("ADVERTISER_ID")

# function to access awin api
def fetch_data():
    start_date = '2023-11-01' # T00%3A00%3A00
    end_date = '2023-11-12' # T01%3A59%3A59

    # template url
    # https://api.awin.com/advertisers/<yourAdvertiserId>/transactions/?startDate=yyyy-MM-ddThh%3Amm%3Ass&endDate=yyyy-MM-ddThh%3Amm%3Ass&timezone=UTC&dateType=transaction&status=pending&publisherId=<publisherIdForWhichToFilter>

    # Example:
    # https://api.awin.com/advertisers/1001/transactions/?startDate=2017-02-20T00%3A00%3A00&endDate=2017-02-21T01%3A59%3A59&timezone=UTC

    url = f'https://api.awin.com/advertisers/{ADVERTISER_ID}/transactions/?startDate={start_date}T00%3A00%3A00&endDate={end_date}T01%3A59%3A59&timezone=UTC&dateType=transaction'

    # To use your token for authentication, send it via the http headers. Please use "Authorization" as the key, and "Bearer <addYourTokenHere>" as the value.
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # API request was successful
        data = response.json()
        # Create a DataFrame from the JSON data
        df = pd.json_normalize(data)
        df.sort_values(by='transactionDate', inplace=True, ascending=False)
        # Display the DataFrame
        awin_df = df[['id', 'transactionDate', 'voucherCode', 'url', 'saleAmount.amount', 'commissionAmount.amount', 'commissionStatus']]
        print(awin_df)

        return awin_df
    else:
        print("Error:", response.status_code)


# Function to update BigQuery table
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
        table_ref = dataset_ref.table(TABLE_ID)

        # Convert 'date' column to datetime if not already
        df['transactionDate'] = pd.to_datetime(df['transactionDate'])

        # Fetch existing data from BigQuery
        try:
            existing_data = client.query(f'SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`').to_dataframe()
        except Exception as e:
            print(f"Error fetching existing data from BigQuery: {e}")
            existing_data = pd.DataFrame()

        # Identify new transactions and changed rows
        new_data = df[~df['id'].isin(existing_data['id'])]
        changed_data = df[df['id'].isin(existing_data['id'])]
        print(df.columns)

        # Update existing rows
        for idx, row in changed_data.iterrows():
            existing_row = existing_data[existing_data['id'] == row['id']]
            if not existing_row.empty:
                # Update existing row with new data
                existing_data.loc[existing_row.index] = row

        # Append new rows
        updated_data = pd.concat([existing_data, new_data], ignore_index=True, sort=False)

        # Update BigQuery
        if not updated_data.empty:
            client.load_table_from_dataframe(updated_data, table_ref, job_config=bigquery.LoadJobConfig(autodetect=True, create_disposition='CREATE_IF_NEEDED')).result()

    except Exception as e:
        raise RuntimeError(f"Error updating BigQuery table: {e}")
    

# only run functions if script is run on its own
# if the script is loaded as a module it will be skipped
if __name__ == "__main__":
    # Fetch data from the API
    data = fetch_data()

    # Update BigQuery table
    update_bigquery(data)
