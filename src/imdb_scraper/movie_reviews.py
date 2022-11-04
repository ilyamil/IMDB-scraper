import time
import warnings
import pandas as pd
from math import ceil
from requests import Session
from typing import List, Dict, Any, Optional
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
from bs4.element import Tag
from imdb_scraper.utils import send_request, create_logger

warnings.filterwarnings('ignore')


BAR_FORMAT = '{percentage:3.0f}%|{bar:20}{r_bar}'
COLUMNS = [
    'title_id',
    'text',
    'rating',
    'date',
    'title',
    'author',
    'helpfulness'
]
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 6.1)'
    'AppleWebKit/537.36 (KHTML, like Gecko)'
    'Chrome/88.0.4324.150 Safari/537.36'
)
START_URL_TEMPLATE = (
    'https://www.imdb.com{}reviews?sort=helpfulnessScore'
    '&dir=desc&ratingFilter=0'
)
LINK_URL_TEMPLATE = (
    'https://www.imdb.com{}reviews/_ajax/?sort=helpfulnessScore'
    '&dir=desc&ratingFilter=0'
)
PARTITION_NAME_TEMPLATE = 's3://{}/reviews/movie_reviews_partition_{}.csv'
SLEEP_TIME = 0.1
BATCH_SIZE = 2


def collect_reviews(
    config: Dict[str, Any],
    credentials: Dict[str, Any],
    limit: int = BATCH_SIZE,
    return_results: bool = False
):
    """
    Main function to collect reviews from IMDB. All related parameters must
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
        * limit (int, optional): technical argument to limit number of movies
        to collect reviews from. Defaults to BATCH_SIZE.
        * return_results (bool, optional): technical argument to define
        whether the functions returns results or save them. Defaults to False.
    """
    log_config = {k: v for k, v in config.items() if 'log_' in k}
    logger = create_logger(**log_config)

    storage_options = {
        'key': credentials['aws']['access_key'],
        'secret': credentials['aws']['secret_access_key']
    }
    s3_uri = f's3://{credentials["aws"]["bucket"]}/{config["metadata_file"]}'
    metadata = pd.read_json(
        s3_uri,
        storage_options=storage_options,
        orient='index'
    )

    if config['overwrite']:
        metadata['reviews_collected_flg'] = False
        metadata.to_json(
            s3_uri, storage_options=storage_options, orient='index'
        )

    reviews = []
    for i, movie_id in enumerate(tqdm(metadata.index), 1):
        if metadata.at[movie_id, 'reviews_collected_flg']:
            continue

        logger.info(f'Started collecting reviews for movie {movie_id}')
        single_movie_reviews = collect_single_movie_reviews(
                movie_id, config['pct_reviews'], logger
        )
        reviews.extend(single_movie_reviews)
        metadata.at[movie_id, 'reviews_collected_flg'] = True
        logger.info(f'Collected reviews for movie {movie_id}')

        if (len(reviews) >= limit) | (i == len(metadata.index)):
            if return_results:
                return reviews
            else:
                metadata.to_json(
                    s3_uri, storage_options=storage_options, orient='index'
                )

                partition_num = ceil(i / limit)
                partition_uri = PARTITION_NAME_TEMPLATE.format(
                    credentials["aws"]["bucket"], partition_num
                )
                partition = pd.DataFrame.from_records(reviews)
                partition.to_csv(
                    partition_uri,
                    storage_options=storage_options
                )

                logger.info(
                    f'{len(reviews)} reviews have been written to '
                    f'partition #{partition_num}'
                )

                reviews = []


def collect_single_movie_reviews(
    movie_id: str,
    pct_reviews: int,
    logger
) -> List[Dict[str, Any]]:
    request_params = {
        'params': {
            'ref_': 'undefined',
            'paginationKey': ''
        }
    }

    with Session() as session:
        session.headers['User-Agent'] = USER_AGENT
        start_url = START_URL_TEMPLATE.format(movie_id)
        try:
            res = send_request(start_url, session=session)
        except Exception as e:
            logger.warn(
                f'Exception of sending start requests to ID {movie_id}'
                f' with message: {e}'
            )

        movie_reviews_num = get_reviews_total_count(start_url)
        reviews_num_max = int(movie_reviews_num * pct_reviews / 100)

        title_reviews = []
        load_another_reviews = True
        while load_another_reviews:
            time.sleep(SLEEP_TIME)

            reviews_batch = []
            soup = BeautifulSoup(res.content, 'html.parser')
            for tag in soup.select('.review-container'):
                review = collect_single_review(tag)
                review['movie_id'] = movie_id
                reviews_batch.append(review)

            title_reviews.extend(reviews_batch)
            logger.info(
                f'Collected {len(reviews_batch)} reviews'
                f' for title ID {movie_id}'
            )

            if len(title_reviews) > reviews_num_max:
                break

            # imitate clicking load-more button
            try:
                pagination_key = (
                    soup
                    .select_one(".load-more-data[data-key]")
                    .get("data-key")
                )
            except AttributeError:
                load_another_reviews = False

            if load_another_reviews:
                link_url = LINK_URL_TEMPLATE.format(movie_id)
                request_params['params']['paginationKey'] = pagination_key
                try:
                    res = send_request(link_url, **request_params)
                except Exception as e:
                    logger.warn(
                        f'Exception of sending link requests to ID {movie_id}'
                        f' with message: {e}'
                    )

        session.close()
        res.close()

    return title_reviews


def collect_single_review(tag: Tag) -> Dict[str, Any]:
    return {
        'text': collect_text(tag),
        'rating': collect_rating(tag),
        'date': collect_date(tag),
        'title': collect_title(tag),
        'author': collect_author(tag),
        'helpfulness': collect_helpfulness(tag)
    }


def get_reviews_total_count(url: str) -> int:
    page = send_request(url)
    try:
        review_cnt = (
            BeautifulSoup(page.content, 'html.parser')
            .find('div', {'class', 'header'})
            .find('div')
            .text
            .replace(' ', '')
            .replace(',', '')
            .split('Reviews')[0]
        )
        return int(review_cnt)
    except Exception:
        return 0


def collect_date(tag: Tag) -> Optional[str]:
    filters = {'class': 'review-date'}
    try:
        date_raw = tag.find('span', filters)
        return date_raw.text
    except Exception:
        return None


def collect_title(tag: Tag) -> Optional[str]:
    filters = {'class': 'title'}
    try:
        title_raw = tag.find('a', filters)
        return title_raw.text
    except Exception:
        return None


def collect_text(tag: Tag) -> Optional[str]:
    filters = {'class': 'text show-more__control'}
    try:
        text_raw = tag.find('div', filters)
        return text_raw.text
    except Exception:
        return None


def collect_rating(tag: Tag) -> Optional[float]:
    try:
        rating_raw = tag.find_all('span')
        rating = rating_raw[1].text
        # If no rating was given, span block containes review date
        if len(rating) > 2:
            return None
        return int(rating)
    except Exception:
        return None


def collect_author(tag: Tag) -> Optional[str]:
    filters = {'class': 'display-name-link'}
    try:
        author_raw = tag.find('span', filters)
        return author_raw.a['href']
    except Exception:
        return None


def collect_helpfulness(tag: Tag) -> Optional[str]:
    filters = {'class': 'actions text-muted'}
    try:
        helpfulness_raw = tag.find('div', filters)
        return helpfulness_raw.text
    except Exception:
        return None
