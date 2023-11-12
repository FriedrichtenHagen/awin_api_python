import requests
import os
from dotenv import load_dotenv
import json
import pandas as pd

# Load environment variables from the .env file
load_dotenv()

# Access environment variables
API_KEY = os.getenv("API_KEY")
ADVERTISER_ID = os.getenv("ADVERTISER_ID")




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
    # print(json.dumps(response, indent=2))


    # Create a DataFrame from the JSON data
    df = pd.json_normalize(data)
    df.sort_values(by='transactionDate', inplace=True, ascending=False)

    # Display the DataFrame
    print(df[['id', 'transactionDate', 'voucherCode', 'url', 'saleAmount.amount', 'commissionAmount.amount', 'commissionStatus']])

else:
    print("Error:", response.status_code)



# # Function to fetch data from the API
# def fetch_data():
#     # Implement your logic to fetch data from the API
#     # ...

# # Function to update BigQuery table
# def update_bigquery(data):
#     client = bigquery.Client()

#     # Configure your BigQuery settings
#     dataset_id = 'your_dataset_id'
#     table_id = 'your_table_id'
#     project_id = 'your_project_id'

#     dataset_ref = client.dataset(dataset_id, project=project_id)
#     table_ref = dataset_ref.table(table_id)

#     # Convert data to DataFrame
#     df = pd.DataFrame(data)

#     # Convert 'date' column to datetime if not already
#     df['date'] = pd.to_datetime(df['date'])

#     # Fetch existing data from BigQuery
#     try:
#         existing_data = client.query(f'SELECT * FROM `{project_id}.{dataset_id}.{table_id}`').to_dataframe()
#     except Exception as e:
#         print(f"Error fetching existing data from BigQuery: {e}")
#         existing_data = pd.DataFrame()

#     # Identify new transactions and changed rows
#     new_data = df[~df['transaction_id'].isin(existing_data['transaction_id'])]
#     changed_data = df[df['transaction_id'].isin(existing_data['transaction_id'])]

#     # Update BigQuery
#     if not new_data.empty:
#         client.load_table_from_dataframe(new_data, table_ref, job_config=bigquery.LoadJobConfig(autodetect=True)).result()

#     if not changed_data.empty:
#         client.load_table_from_dataframe(changed_data, table_ref, job_config=bigquery.LoadJobConfig(autodetect=True, write_disposition='WRITE_TRUNCATE')).result()

# if __name__ == "__main__":
#     # Fetch data from the API
#     data = fetch_data()

#     # Update BigQuery table
#     update_bigquery(data)
