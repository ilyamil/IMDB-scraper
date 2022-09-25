import re
import time
import pandas as pd
from typing import Optional, Dict, Any, List, Set, Union
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
from imdb_scraper.utils import send_request, create_logger


BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'
# total number of movies collected manually as of December 2021
GENRES = [
    'action',
    'adventure',
    'animation',
    'biography',
    'comedy',
    'crime',
    'drama',
    'family',
    'fantasy',
    'film-noir',
    'history',
    'horror',
    'music',
    'musical',
    'mystery',
    'romance',
    'sci-fi',
    'sport',
    'thriller',
    'war',
    'western'
]
GENRE_URL = (
    'https://www.imdb.com/search/title/?title_type=feature&genres={}'
    '&sort=num_votes,desc&start={}&explore=genres&ref_=adv_nxt'
)
BASE_URL = 'https://www.imdb.com{}'
STEP = 50
TOP_N_ACTORS = 10
BATCH_SIZE = 100
SLEEP_TIME = 0.2


def collect_metadata(config: Dict[str, Any], credentials: Dict[str, Any]):
    """
    Main function to collect metadata from IMDB. All related parameters must
    be specified in configs and passed as function argument.

    Args:
        * config (Dict[str, Any]): scraper configuration with the following
        list of required fields:
            * metadata_file (str): name of a file we want to populate with
            metadata.
            * genres (str or List[str]): genres that movies must be assigned
            to. It is possible to either explicitly set this field with a list
            of genres or use a keyword 'all' if you want to collect movies
            in all genres.
            * pct_titles (float): percent of titles in each genre scraper will
            try to collect.
            * overwrite (bool): whether to overwrite metadata after each run.
            I recommend to set this parameter with False.
            * log_file (str): path to log file with logs related to metadata.
            * log_level (str): no logs below this level will be written to log
            file.
            * log_msg_format (str): log message format
            * log_dt_format (str): datetime format
        * credentials (Dict[str, Any]): all related credentials to read from
        and write to AWS s3. Required fields:
            * access_key (str)
            * secret_access_key (str)
            * bucket (str)
    """
    if config['genres'] == 'all':
        use_genres = GENRES
    else:
        use_genres = {g for g in GENRES if g in config['genres']}

    if (config['pct_titles'] > 100) | (config['pct_titles'] < 0):
        raise ValueError(
            'Configuration parameter pct_reviews must be in range [0, 100]'
        )

    log_config = {k: v for k, v in config.items() if 'log_' in k}
    logger = create_logger(**log_config)

    storage_options = {
        'key': credentials['aws']['access_key'],
        'secret': credentials['aws']['secret_access_key']
    }
    s3_uri = f's3://{credentials["aws"]["bucket"]}/{config["metadata_file"]}'

    try:
        if config['overwrite']:
            raise ValueError

        logger.info('Trying to read metadata file from s3')

        metadata = pd.read_json(
            s3_uri,
            storage_options=storage_options,
            orient='index'
        )
        print('Metadata file was found.')

        logger.info('Metadata file was loaded from s3')
    except Exception:
        print('Metadata file wasn`t found.\nCollecting movie identifiers...')

        logger.info('Collecting movie identifiers')

        ids = collect_ids(use_genres, config['pct_titles'], logger)
        metadata_ = {
            id_: {
                'metadata_collected_flg': False,
                'reviews_collected_flg': False
            }
            for id_ in ids
        }
        metadata = pd.DataFrame.from_dict(data=metadata_, orient='index')
        metadata.to_json(
            s3_uri,
            orient='index',
            storage_options=storage_options
        )

        logger.info('Identifiers were collected')

    collected = []
    print('Collecting movie metadata...')
    for i, id_ in enumerate(tqdm(metadata.index)):
        if metadata.at[id_, 'metadata_collected_flg']:
            continue

        logger.info(f'Collecting metadata for movie {id_}')
        try:
            id_metadata = collect_single_movie_metadata(BASE_URL.format(id_))
            logger.info(f'Successfully collected metadata for movie {id_}')
        except Exception as e:
            logger.info(f'Catched exception "{str(e)}"')
            continue

        for k, v in id_metadata.items():
            if k not in metadata.columns:
                metadata[k] = None
            metadata.at[id_, k] = v

        collected.append(id_)
        if (len(collected) == BATCH_SIZE) | (i == len(metadata.index) - 1):
            logger.info('Writing updated metadata')

            for c in collected:
                metadata.at[c, 'metadata_collected_flg'] = True

            metadata.to_json(
                s3_uri,
                orient='index',
                storage_options=storage_options
            )
            collected = []

            logger.info('Updated metadata was successfully written')

        time.sleep(SLEEP_TIME)


def collect_ids(genres: List[str], pct_titles: float, logger) -> Set[str]:
    ids = set()
    for genre in genres:
        total_titles_count = get_total_count(GENRE_URL.format(genre, 1))
        max_titles = int(total_titles_count * pct_titles / 100)
        partitions = range(1, max_titles + 1, STEP)
        for partition in tqdm(partitions, unit_scale=STEP, desc=genre):
            new_ids = collect_ids_from_single_page(
                GENRE_URL.format(genre, partition)
            )
            ids.update(new_ids)
            logger.info(
                f'Collected ids in genre {genre}: '
                f'{partition} - {partition + STEP - 1}'
            )
            time.sleep(SLEEP_TIME)
    return ids


def collect_ids_from_single_page(url: str) -> Set[str]:
    page = send_request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    containers = soup.find_all('div', {'class': 'lister-item-content'})
    return {t.a['href'] for t in containers}


def get_total_count(url: str) -> int:
    page = send_request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    total_count_str = (
        soup
        .find('div', {'class': 'desc'})
        .find('span')
        .text
        .split(' ')
        [2]
    )
    return int(total_count_str.replace(',', ''))


def collect_single_movie_metadata(url: str) -> Dict[str, Any]:
    page = send_request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    return {
        'original_title': collect_original_title(soup),
        'genres': collect_genres(soup),
        'director': collect_director(soup),
        'poster_url': collect_poster_url(soup),
        'review_summary': collect_review_summary(soup),
        'agg_rating': collect_aggregate_rating(soup),
        'actors': collect_actors(soup),
        'imdb_recommendations': collect_imdb_recommendations(soup),
        'details': collect_details_summary(soup),
        'boxoffice': collect_boxoffice(soup)
    }


def collect_director(soup: BeautifulSoup) -> Optional[str]:
    filters = {
        'class':
        (
            'ipc-metadata-list ipc-metadata-list--dividers-all '
            'title-pc-list ipc-metadata-list--baseAlt'
        )
    }
    try:
        return soup.find_all('ul', filters)[0].find('a').text
    except Exception:
        return None


def collect_original_title(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'hero-title-block__title'}
    try:
        return soup.find('h1', filters).text
    except Exception:
        return None


def collect_poster_url(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'hero-media__poster'}
    try:
        return soup.find('div', filters).img['src']
    except Exception:
        return None


def collect_review_summary(soup: BeautifulSoup)\
        -> Optional[Dict[str, Any]]:
    keys = ['user_review_num', 'critic_review_num', 'metascore']
    try:
        scores = [sc.text for sc in soup.find_all('span', class_=['score'])]
    except Exception:
        scores = [None, None, None]
    return dict(zip(keys, scores))


def collect_aggregate_rating(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    filters = {'data-testid': 'hero-rating-bar__aggregate-rating'}
    try:
        rating_raw = soup.find('div', filters).text
        rating, votes = (
            rating_raw
            .replace('IMDb RATING', '')
            .replace('/10', '/10?')
            .split('?')
        )
        return {'avg_rating': rating, 'num_votes': votes}
    except Exception:
        return None


def get_id_and_rank(s: str) -> Dict[str, Any]:
    id_ = s.split('?')[0] if s else None
    rank = s.split('_t_')[1] if s else None
    return id_, rank


def collect_actors(soup: BeautifulSoup) -> Dict[str, str]:
    filters = {'data-testid': 'title-cast-item__actor'}
    try:
        actors_raw = soup.find_all('a', filters)
        actors = {}
        for actor in actors_raw[:TOP_N_ACTORS]:
            id_, rank = get_id_and_rank(actor.get('href', None))
            actors[rank] = id_
        return actors
    except Exception:
        return {}


def collect_imdb_recommendations(soup: BeautifulSoup)\
        -> Optional[List[str]]:
    filters = {'class': re.compile('ipc-poster-card__title')}
    try:
        recom_raw = soup.find_all('a', filters)
        recommendations = {}
        for recom in recom_raw:
            id_, rank = get_id_and_rank(recom.get('href', None))
            recommendations[rank] = id_
        return recommendations
    except Exception:
        return {}


def collect_genres(soup: BeautifulSoup) -> Optional[List[str]]:
    filters = {'data-testid': 'genres'}
    try:
        genres_raw = soup.find('div', filters).find_all('a')
        return [el.text for el in genres_raw]
    except Exception:
        return None


def collect_details_summary(soup: BeautifulSoup)\
        -> Dict[str, Union[List[str], str]]:
    filters = {
        'release_date':
            {'data-testid': 'title-details-releasedate'},
        'countries_of_origin':
            {'data-testid': 'title-details-origin'},
        'language':
            {'data-testid': 'title-details-languages'},
        'also_known_as':
            {'data-testid': 'title-details-akas'},
        'production_companies':
            {'data-testid': 'title-details-companies'},
        'filming_locations':
            {'data-testid': 'title-details-filminglocations'}
    }
    details = {}
    for name, f in filters.items():
        try:
            raw_entity = soup.find('li', f).find_all('li')
            entity = [entry.text for entry in raw_entity]
        except Exception:
            entity = None
        details[name] = entity

    # add runtime info
    runtime_filter = {'data-testid': 'title-techspec_runtime'}
    try:
        runtime = soup.find('li', runtime_filter).div.text
    except Exception:
        runtime = None
    details['runtime'] = runtime

    return details


def collect_boxoffice(soup: BeautifulSoup) -> Optional[Dict[str, List[str]]]:
    filters = {
        'budget':
            {'data-testid': 'title-boxoffice-budget'},
        'boxoffice_gross_domestic':
            {'data-testid': 'title-boxoffice-grossdomestic'},
        'boxoffice_gross_opening':
            {'data-testid': 'title-boxoffice-openingweekenddomestic'},
        'boxoffice_gross_worldwide':
            {'data-testid': 'title-boxoffice-cumulativeworldwidegross'}
    }
    boxoffice = dict()
    for name, f in filters.items():
        try:
            entity = soup.find('li', f).li.text
        except Exception:
            entity = None
        boxoffice[name] = entity
    return boxoffice


def extract_main_genre(s: str) -> str:
    return re.split(', | ', s.replace('\n', ''))[0]
