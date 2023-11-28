import pandas as pd
import requests
from dotenv import load_dotenv
from update_bigquery import update_bigquery
import os

# Load environment variables from the .env file
load_dotenv()

# function to access awin api
def fetch_data(start_date, end_date):
    # template url
    # https://api.awin.com/advertisers/<yourAdvertiserId>/transactions/?startDate=yyyy-MM-ddThh%3Amm%3Ass&endDate=yyyy-MM-ddThh%3Amm%3Ass&timezone=UTC&dateType=transaction&status=pending&publisherId=<publisherIdForWhichToFilter>
    # Example:
    # https://api.awin.com/advertisers/1001/transactions/?startDate=2017-02-20T00%3A00%3A00&endDate=2017-02-21T01%3A59%3A59&timezone=UTC

    API_KEY = os.getenv("API_KEY")
    ADVERTISER_ID = os.getenv("ADVERTISER_ID")

    url = f'https://api.awin.com/advertisers/{ADVERTISER_ID}/transactions/?startDate={start_date}T00%3A00%3A00&endDate={end_date}T23%3A59%3A59&timezone=UTC&dateType=transaction'

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
        # print(awin_df)

        return awin_df
    else:
        print("Error:", response.status_code)
