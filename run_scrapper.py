import pandas as pd
from imdb_scrapper.utils import read_yaml

aws_credentials = read_yaml('credentials.yaml')
# print(aws_credentials)

storage_options = {
    'key': aws_credentials['aws']['access_key'],
    'secret': aws_credentials['aws']['secret_access_key']
}
uri = f's3://{storage_options["aws"]["bucket"]}/metadata/metadata.json'

metadata = pd.read_json(uri, storage_options=storage_options)
print(metadata)
