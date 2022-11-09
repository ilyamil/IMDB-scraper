from argparse import ArgumentParser
from imdb_scraper.movie_metadata import collect_metadata
from imdb_scraper.movie_reviews import collect_reviews
from imdb_scraper.preprocessing import preprocess_metadata, preprocess_reviews
from imdb_scraper.utils import read_yaml


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
        collect_reviews(config['reviews'], credentials)
    elif entity == 'preprocessing':
        preprocess_metadata(config['preprocessing'], credentials)
    else:
        raise ValueError('Unsupported entity')


if __name__ == '__main__':
    arguments = parse_arguments()
    run(arguments.entity)
