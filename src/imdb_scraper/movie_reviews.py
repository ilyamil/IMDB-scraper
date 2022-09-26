import os
import time
import warnings
import pandas as pd
from requests import Session
from time import sleep
from pathlib import Path
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
SLEEP_TIME = 0.1


def collect_reviews(config: Dict[str, Any], credentials: Dict[str, Any]):
    pass


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


# class ReviewCollector:
#     """
#     Contains methods for parsing IMDB movie reviews.
#     Public methods:
#         * collect_review: parses reviews of a given movie.
#         * find_reviews_num: finds number of reviews for a given movie.
#         * is_all_reviews_collected: check if there any title we can scrape
#         details about.
#         * collect: parses pages and saves IDs on a disk or cloud.
#     """
#     def __init__(self, config: Dict[str, Any]):
#         """
#         Initializes Review Collector class. All parameters related to web
#         scraping of movie reviews must be specified in config.
#         The config must contains the following fields:
#         * mode: specifies where the results should be saved. When set up to
#         'local' all movie related data will be saved on local machine, where
#         application is running. When set up to 'cloud' related data saves on
#         AWS. Using 'cloud' mode you also need to set up the following
#         environment variables: AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY and
#         AWS_S3_BUCKET.
#         * metadata_file: name of file (possibly with folder) movies metadata
#         will be saved to.
#         * n_reviews: maximum number of reviews to scrape for a given movie.
#         * pct_reviews: percent of total number of reviews to scrape for a
#         given movie. If set to 100, then all reviews about movie will be
#         collected.
#         * chunk_size: number of movies a program try to parse in one iteration.
#         After each iteration there is a timout period to prevent too many
#         requests.
#         * sleep_time: time in seconds a program will be wait for before going
#         to next movie. This parameter should be set reasonably, not too high
#         (web scraping will last too long), not too low (increasing load on IMDB
#         server for a long period of time is not ethical and such requests could
#         be rate limited as a result).
#         * log_file: file name to write logs related to collecting IDs.
#         * log_level: minimal level of log messages.
#         * log_msg_format: message format in logs.
#         * log_dt_format: datetime format in logs.
#         Note:
#         * There must be specified either pct_reivews or n_reviews for program
#         to run correctly.
#         """
#         self._mode = config['mode']
#         self._n_reviews = config['n_reviews']
#         self._chunk_size = config['chunk_size']
#         self._pct_reviews = config['pct_reviews']
#         self._sleep_time = config['sleep_time']

#         if self._mode == 'cloud':
#             load_dotenv()
#             self._storage_options = {
#                 'key': os.getenv('AWS_ACCESS_KEY'),
#                 'secret': os.getenv('AWS_SECRET_ACCESS_KEY')
#             }
#             if (not self._storage_options['key'])\
#                     or (not self._storage_options['secret']):
#                 raise ValueError(
#                     'AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY'
#                     + ' must be specified in environment variables'
#                 )

#             self._bucket = os.getenv('AWS_S3_BUCKET')
#             if not self._bucket:
#                 raise ValueError(
#                     'AWS_S3_BUCKET must be specified in environment variables'
#                 )

#             self._metadata_file = os.path.join(
#                 's3://', self._bucket, config['metadata_file']
#             )
#             self._review_folder = os.path.join(
#                 's3://', self._bucket, 'reviews'
#             )
#         elif self._mode == 'local':
#             self._storage_options = None
#             self._root_dir = str(Path(__file__).parents[2])
#             self._metadata_file = os.path.join(
#                 self._root_dir,
#                 'data',
#                 config['metadata_file']
#             )
#             self._review_folder = os.path.join(
#                 self._root_dir, 'data', 'reviews'
#             )
#         else:
#             raise ValueError('Supported modes: "local", "cloud"')

#         metadata = read_json(
#             self._metadata_file,
#             storage_options=self._storage_options,
#             orient='index'
#         )
#         if 'reviews_collected_flg' not in metadata.columns:
#             metadata['reviews_collected_flg'] = 0
#             metadata.to_json(
#                 self._metadata_file,
#                 storage_options=self._storage_options,
#                 orient='index'
#             )
#         del metadata

#         self._logger = create_logger(
#             filename=config['log_file'],
#             msg_format=config['log_msg_format'],
#             dt_format=config['log_dt_format'],
#             level=config['log_level']
#         )

#         if not (self._pct_reviews or self._n_reviews):
#             raise ValueError(
#                 'Only one of these arguments needs to be set'
#                 ' in config file: n_reviews or pct_reviews'
#             )

#         if self._pct_reviews and not 0 <= self._pct_reviews <= 100:
#             raise ValueError(
#                     'pct_reviews must lie in the interval [0, 100]'
#             )

#         self._logger.info('Successfully initialized ReviewCollector')

#     @staticmethod
#     def collect_review(id_: str, tag: Tag) -> Dict[str, Any]:
#         return {
#             'id': id_,
#             'text': collect_text(tag),
#             'rating': collect_rating(tag),
#             'date': collect_date(tag),
#             'title': collect_title(tag),
#             'author': collect_author(tag),
#             'helpfulness': collect_helpfulness(tag)
#         }

#     @staticmethod
#     def find_reviews_num(response: requests.Response) -> int:
#         bs = BeautifulSoup(response.text, 'lxml')
#         try:
#             review_cnt = (
#                 bs
#                 .find('div', {'class', 'header'})
#                 .find('div')
#                 .text
#                 .replace(' ', '')
#                 .replace(',', '')
#                 .split('Reviews')[0]
#             )
#             return int(review_cnt)
#         except Exception:
#             return 0

#     def collect_title_reviews(self, id_: str) -> List[Dict[str, Any]]:
#         request_params = {
#             'params': {
#                 'ref_': 'undefined',
#                 'paginationKey': ''
#             }
#         }

#         with requests.Session() as session:
#             session.headers['User-Agent'] = USER_AGENT
#             start_url = START_URL_TEMPLATE.format(id_)
#             try:
#                 res = send_request(start_url, session=session)
#             except Exception as e:
#                 self._logger.warn(
#                     f'Exception of sending start requests to ID {id_}'
#                     f' with message: {e}'
#                 )

#             reviews_num = ReviewCollector.find_reviews_num(res)
#             if self._n_reviews:
#                 reviews_num_max = self._n_reviews
#             elif self._pct_reviews:
#                 reviews_num_max = int(reviews_num * self._pct_reviews / 100)

#             title_reviews = []
#             load_another_reviews = True
#             while load_another_reviews:
#                 sleep(self._sleep_time)

#                 reviews_batch = []
#                 soup = BeautifulSoup(res.text, 'lxml')
#                 for tag in soup.select('.review-container'):
#                     review = ReviewCollector.collect_review(id_, tag)
#                     reviews_batch.append(review)

#                 title_reviews.extend(reviews_batch)
#                 self._logger.info(
#                     f'Collected {len(reviews_batch)} reviews'
#                     f' for title ID {id_}'
#                 )

#                 if len(title_reviews) > reviews_num_max:
#                     break

#                 # imitate clicking load-more button
#                 try:
#                     pagination_key = (
#                         soup
#                         .select_one(".load-more-data[data-key]")
#                         .get("data-key")
#                     )
#                 except AttributeError:
#                     load_another_reviews = False

#                 if load_another_reviews:
#                     link_url = LINK_URL_TEMPLATE.format(id_)
#                     request_params['params']['paginationKey'] = pagination_key
#                     try:
#                         res = send_request(link_url, **request_params)
#                     except Exception as e:
#                         self._logger.warn(
#                             f'Exception of sending link requests to ID {id_}'
#                             f' with message: {e}'
#                         )

#             session.close()
#             res.close()

#         return title_reviews

#     def is_all_reviews_collected(self) -> bool:
#         """
#         Checks if reviews were collected for all available titles.
#         """
#         # As status file has only one column it must be read in columnar
#         # orientation
#         metadata = read_json(
#             self._metadata_file,
#             storage_options=self._storage_options,
#             orient='index'
#         )
#         already_collected = metadata['reviews_collected_flg'].sum()
#         total_movies = len(metadata)

#         print(
#             f'Movie reviews are already collected for {already_collected}'
#             + f' out of {total_movies} titles'
#         )
#         return total_movies == already_collected

#     def collect(self) -> bool:
#         print('Collecting reviews...')

#         metadata = read_json(
#             self._metadata_file,
#             storage_options=self._storage_options,
#             orient='index'
#         )

#         counter = 0
#         for title_id in tqdm(metadata.index, bar_format=BAR_FORMAT):
#             if metadata.at[title_id, 'reviews_collected_flg']:
#                 continue

#             title_reviews = self.collect_title_reviews(title_id)

#             # This line extracts pure title id from raw form
#             # e.g. '/title/tt0468569/' -> 'tt0468569'
#             id_ = title_id.split('/')[-2]
#             title_path = os.path.join(self._review_folder, id_ + '.csv')
#             try:
#                 if len(title_reviews) > 0:
#                     DataFrame.from_records(title_reviews).to_csv(
#                         title_path,
#                         storage_options=self._storage_options
#                     )

#                 # Update status on each iteration, because it takes a long
#                 # time to parse reviews even for a single title.
#                 metadata.at[title_id, 'reviews_collected_flg'] = 1
#                 metadata.to_json(
#                     self._metadata_file,
#                     storage_options=self._storage_options,
#                     orient='index'
#                 )

#                 counter += 1

#                 self._logger.info(
#                     f'Total collected {len(title_reviews)} reviews '
#                     + f'for title ID {id_}'
#                 )
#             except Exception as e:
#                 self._logger.warn(
#                     f'Exception {str(e)} while collecting reviews'
#                     + f' about title {id_}'
#                 )

#             if counter == self._chunk_size:
#                 self._logger.info('Stop parsing due to requests limit')
#                 return


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
