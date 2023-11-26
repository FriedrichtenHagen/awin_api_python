# this script gets all advertiser_transactions for the selected time frame.
# Due to the restriction of the awin api to a maximum of 31 days per request,
# this script will make repeated api requests, bundle that data and then push it to bigquery.
# The idea is to run this script once to get all historical data and the use the advertiser_transactions.py for daily updates. 

# https://wiki.awin.com/index.php/Advertiser_API
# Awin has a throttling system in place that limits the number of API requests to 20 API calls per minute per user.
# the maximum date range between startDate and endDate currently supported is 31 days

from datetime import date, timedelta, datetime
import math
import pandas as pd
import requests
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Access environment variables
API_KEY = os.getenv("API_KEY")
ADVERTISER_ID = os.getenv("ADVERTISER_ID")

# function to access awin api
def fetch_data(start_date, end_date):
    # template url
    # https://api.awin.com/advertisers/<yourAdvertiserId>/transactions/?startDate=yyyy-MM-ddThh%3Amm%3Ass&endDate=yyyy-MM-ddThh%3Amm%3Ass&timezone=UTC&dateType=transaction&status=pending&publisherId=<publisherIdForWhichToFilter>
    # Example:
    # https://api.awin.com/advertisers/1001/transactions/?startDate=2017-02-20T00%3A00%3A00&endDate=2017-02-21T01%3A59%3A59&timezone=UTC

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

def loop_api_calls():
    total_start_date_str = '2021-01-01'
    total_start_date = datetime.strptime(total_start_date_str, '%Y-%m-%d').date()
    total_end_date = date.today()
    total_date_difference = (total_end_date - total_start_date).days
    number_of_necessary_api_calls = math.ceil(total_date_difference/32)
    days_for_last_api_call = total_date_difference % 32
    print(f'days in last api call: {days_for_last_api_call}')
    print(f'Number of api calls: {number_of_necessary_api_calls}')

    result_df = pd.DataFrame()
    for i in range(number_of_necessary_api_calls):
        if i % 20 == 0 and i != 0:
            # pause every 20 API calls for 1 minute?
            print('theoretical pause')
        # last api call. Uses less than 31 days
        if(days_for_last_api_call and i == number_of_necessary_api_calls-1):
            start_date = total_start_date + (i)*timedelta(days=32)
            end_date = start_date + timedelta(days=days_for_last_api_call)
            
        else:
            start_date = total_start_date + i*timedelta(days=32)
            end_date = start_date + timedelta(days=31)
        print(start_date, end_date)


        # is there overlap between the different time frames??
        # set times to cover whole day
        # adjust 


        newest_dataframe = fetch_data(start_date, end_date)
        
        result_df = pd.concat([result_df, newest_dataframe], ignore_index=True)
        print(f"Shape of result_df: {result_df.shape}")

    print(result_df)
    result_df.to_csv('output.csv', index=False)

loop_api_calls()

