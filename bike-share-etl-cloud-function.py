import requests
import pandas as pd
from pandas.io.json import json_normalize
from google.cloud import storage
from io import StringIO


def run_etl(request):
    ## initialize parameters
    # API parameters
    limit = "35000"
    app_token = "YOUR_APP_TOKEN"
    url = "https://data.bts.gov/resource/7m5x-ubud.json?$limit=" + limit + "&$$app_token=" + app_token

    # GCP parameters
    bucket_name = "bike-sharing-bucket"
    blob_name = "bike-sharing-data.csv"



    # Retrieve data from public API and convert to json object
    response_body = requests.get(url).json()

    # Create dataframe from json object
    data = json_normalize(response_body)

    # Convert format of year and system_id to integers
    data['year'] = pd.to_numeric(data['year']).astype(int)
    data['system_id'] = pd.to_numeric(data['system_id']).astype(int)

    # Aggregate data: sum of bikes per system_id per year
    grouped_data = data.groupby(['system_id','year'])['system_id'].count().reset_index(name="sum_bikes")



    ## Upload dataframe to GCP Cloud Storage

    # Create GCP Storage client
    storage_client = storage.Client()

    # Create in-memory file-like object
    file = StringIO()
    grouped_data.to_csv(file, index = False)

    # Read from beginning of file
    file.seek(0)

    # Upload in-memory file to GCP bucket as a blob
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(file, content_type='text/csv')


    ## Response to HTTP request

    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'<h1>Bike Share Data Pipeline succesfully posted data to <a href="https://console.cloud.google.com/storage/browser/bike-sharing-bucket;tab=objects?forceOnBucketsSortingFiltering=false&project=bike-sharing-project-302421&prefix=&forceOnObjectsSortingFiltering=false">Google Cloud Storage</a></h1>'
