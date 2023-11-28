from datetime import date, timedelta
from fetch_awin_data import fetch_data
from update_bigquery import update_bigquery

end_date = date.today()
# updates the last 30 days
start_date = end_date - timedelta(days=30)
print("End Date:", end_date)
print("Start Date:", start_date)


# Fetch data from the API
data = fetch_data(start_date, end_date)

# Update BigQuery table
update_bigquery(data)

