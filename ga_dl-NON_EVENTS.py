import sys
import config
from datetime import datetime, timedelta
from apiclient import discovery
from googleapiclient.discovery import build
import google.oauth2.credentials
from google.auth.transport.requests import AuthorizedSession
from googleapiclient.discovery import build as google_build
import pandas as pd
import json

reporttype = "NonEvents"
user_agent = None
token_uri = 'https://accounts.google.com/o/oauth2/token'
token_expiry = datetime.now() - timedelta(days = 1)
thetitle = sys.argv[1]
viewId = sys.argv[2]
theepoch = sys.argv[3]

access_token = config.access_token
refresh_token = config.refresh_token
client_id = config.client_id
client_secret = config.client_secret

credentials = google.oauth2.credentials.Credentials(
    None,
    refresh_token=refresh_token,
    token_uri=token_uri,
    client_id=client_id,
    client_secret=client_secret)

api_name = 'analyticsreporting'
api_version = 'v4'

api_client = google_build(serviceName=api_name, version=api_version, credentials=credentials)

sample_request = {
      'viewId': viewId,
      'dateRanges': {
          'startDate': theepoch,
          'endDate': theepoch
      },
      'dimensions': [{'name': 'ga:campaign'}, {'name': 'ga:devicecategory'}, {'name': 'ga:city'}, {'name': 'ga:country'}, {'name': 'ga:userType'}, {'name': 'ga:sourceMedium'}, {'name': 'ga:landingPagePath'}, {'name': 'ga:referralPath'}, {'name': 'ga:pageDepth'}],
      'metrics': [{'expression': 'ga:Users'},  {'expression': 'ga:bounces'}, {'expression': 'ga:pageviews'}, {'expression': 'ga:sessions'},  {'expression': 'ga:organicSearches'},  {'expression': 'ga:totalEvents'}, {'expression': 'ga:sessionDuration'}, {'expression': 'ga:uniqueEvents'}, {'expression': 'ga:sessionsWithEvent'}],
      "pageSize" : 100000
    }

response = api_client.reports().batchGet(
      body={
        'reportRequests': sample_request
      }).execute()

def resp2frame(resp):
        # return object
        out = pd.DataFrame()
        # GA data type to data frame conversion
        lookup = {
          "INTEGER": "int32",
          "FLOAT": "float32",
          "CURRENCY": "float32",
          "PERCENT": "float32",
          "TIME": "object",
          "STRING": "object"
        }

        # Loop through reports and get metrics and dimensions
        for report in resp.get('reports', []):
            col_hdrs = report.get('columnHeader', {})
            # Get the initial dimensions
            cols = col_hdrs['dimensions']
            metric_cols = []
            if 'metricHeader' in list(col_hdrs.keys()):
                metrics = col_hdrs.get('metricHeader', {}).get('metricHeaderEntries', [])
                cols_data_type = {}
                for m in metrics:
                    # Get each metric and the data type
                    cols = cols + [m.get('name')]
                    cols_data_type[m.get('name')] = lookup[m.get('type')]

            # Take out any "ga:" prefixes
            cols = list(map(lambda x: x.replace("ga:", ""), cols))
            # Set the dataframe with the column names
            df = pd.DataFrame(columns=cols)
            # Get the rows from the GA report
            rows = report.get('data', {}).get('rows')
            # Let's loop through the rows to get the dimensions and metrics to row list
            for row in rows:
                row_list = row.get('dimensions', [])

                if 'metrics' in list(row.keys()):
                    metrics = row.get('metrics', [])
                    for m in metrics:
                        row_list = row_list + m.get('values')

                # Make each row an enumerated dictionary with index value starting
                # at 0
                drow = {}
                for i, c in enumerate(cols):
                    drow.update({c : row_list[i]})

                # Concatanate the row to the overall list
                df = pd.concat((df, pd.DataFrame(drow, index=[0])),
                               ignore_index=True)

            # Copy the dataframe to the returning object
            out = pd.concat((out, df), ignore_index=True)
            # Convert the object types to the inferred ones
            out = out.apply(pd.to_numeric, errors='ignore', axis=1)
            # Explicitly convert date back to a date object
            if 'date' in out.columns:
                out['date'] = pd.to_datetime(out['date'], format="%Y%m%d")

        return out


pierced = resp2frame(response)
print(pierced)
pierced['date']=theepoch
pierced['segment']=thetitle
pierced['reporttype']=reporttype
filename=thetitle+'_'+viewId+'_'+reporttype+'_'+theepoch+'.csv'
pierced.to_csv('./_Non_Events/'+filename,index=False,header=True)

#Optionally uncomment & add in creds + table name to store in a mysql compliant db

#from sqlalchemy import create_engine
#engine = create_engine('mysql://%s:%s@%s/%s?charset=utf8mb4' % (MY_SQL_USER, MY_SQL_PASSWORK, MY_SQL_HOST, MY_SQL_DB), encoding='utf8')
#pierced.to_sql(con=engine, name='MY_SQL_TABLE_NAME', if_exists='append')
