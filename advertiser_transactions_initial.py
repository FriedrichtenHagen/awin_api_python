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
from dotenv import load_dotenv
import time
from update_bigquery import update_bigquery
from fetch_awin_data import fetch_data
# Load environment variables from the .env file
load_dotenv()

def loop_api_calls():

    total_end_date = date.today()
    # going back 2 years as a standard time frame 
    total_start_date = total_end_date - timedelta(days=365 * 2)

    total_date_difference = (total_end_date - total_start_date).days
    number_of_necessary_api_calls = math.ceil(total_date_difference/32)
    days_for_last_api_call = total_date_difference % 32
    print(f'days in last api call: {days_for_last_api_call}')
    print(f'Number of api calls: {number_of_necessary_api_calls}')

    result_df = pd.DataFrame()
    for i in range(number_of_necessary_api_calls):
        if i % 20 == 0 and i != 0:
            # pause every 20 API calls for 1 minute?
            print('Pause for 60seconds')
            time.sleep(60)
        # last api call. Uses less than 31 days
        if(days_for_last_api_call and i == number_of_necessary_api_calls-1):
            start_date = total_start_date + (i)*timedelta(days=32)
            end_date = start_date + timedelta(days=days_for_last_api_call)
            
        else:
            start_date = total_start_date + i*timedelta(days=32)
            end_date = start_date + timedelta(days=31)
        print(start_date, end_date)

        newest_dataframe = fetch_data(start_date, end_date)

        result_df = pd.concat([result_df, newest_dataframe], ignore_index=True)
        print(f"Shape of result_df: {result_df.shape}")

    print(result_df)
    update_bigquery(result_df)



loop_api_calls()

