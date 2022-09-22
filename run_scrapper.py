import pandas as pd
from imdb_scrapper.utils import read_yaml

aws_credentials = read_yaml('credentials.yaml')
# print(aws_credentials)

storage_options = {
    'key': aws_credentials['aws']['access_key'],
    'secret': aws_credentials['aws']['secret_access_key']
}
uri = f's3://{aws_credentials["aws"]["bucket"]}/metadata/metadata.json'

try:
    metadata = pd.read_json(uri, storage_options=storage_options, orient='index')
except FileNotFoundError:
    print('catch!')
print(metadata)

# d = {
#     'first_movie': {
#         'technical_field': None
#     },
#     'second_movie': {
#         'technical_field': None
#     },
#     'third movie': {
#         'technical_field': None
#     }
# }

# df = pd.DataFrame.from_dict(d, orient='index')
# print(df)

# df.to_json('here.json', orient='index')

# df = pd.read_json('here.json', orient='index')
# print(df)