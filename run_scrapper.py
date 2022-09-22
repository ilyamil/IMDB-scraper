from argparse import ArgumentParser
from imdb_scrapper.movie_metadata import collect_metadata
from imdb_scrapper.utils import read_yaml


CONFIG_FILE = 'config.yaml'
CREDENTIALS_FILE = 'credentials.yaml'
ENTITIES = ['metadata', 'reviews']


def parse_arguments():
    parser = ArgumentParser(
        description='Python script for web scraping of IMDB'
    )
    parser.add_argument(
        '-e', '--entity', type=str,
        help=f'Entity to collect: {", ".join(ENTITIES)}'
    )
    return parser.parse_args()


def run(entity: str):
    config = read_yaml(CONFIG_FILE)
    credentials = read_yaml(CREDENTIALS_FILE)
    if entity == 'metadata':
        collect_metadata(config['metadata'], credentials)
    elif entity == 'reviews':
        raise NotImplementedError
    else:
        raise ValueError('Unsupported entity')


if __name__ == '__main__':
    arguments = parse_arguments()
    run(arguments.entity)

# storage_options = {
#     'key': aws_credentials['aws']['access_key'],
#     'secret': aws_credentials['aws']['secret_access_key']
# }
# uri = f's3://{aws_credentials["aws"]["bucket"]}/metadata/metadata.json'
# a = pd.DataFrame({'a': [1, 2, 3], 'b': [5, 6, 7]})
# a[['c', 'd', 'e']] = 0
# print(a)

# try:
#     metadata = pd.read_json(uri, storage_options=storage_options, orient='index')
# except FileNotFoundError:
#     print('catch!')
# print(metadata)

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