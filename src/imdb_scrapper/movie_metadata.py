import os
import re
from time import sleep
from typing import Dict, Any
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup
from pandas import DataFrame
from dotenv import load_dotenv
from recsys.utils import send_request, create_logger


BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'
# total number of movies collected manually as of December 2021
MOVIE_COUNT_BY_GENRE = {
    'action': 49210,
    'adventure': 24075,
    'animation': 7479,
    'biography': 7519,
    'comedy': 98571,
    'crime': 33263,
    'drama': 209137,
    'family': 15800,
    'fantasy': 15831,
    'film-noir': 818,
    'history': 8345,
    'horror': 33292,
    'music': 6883,
    'musical': 10226,
    'mystery': 17137,
    'romance': 48572,
    'sci-fi': 15326,
    'sport': 4824,
    'thriller': 49018,
    'war': 9499,
    'western': 8880
}
URL_TEMPLATE = (
    'https://www.imdb.com/search/title/?title_type=feature&genres={}'
    '&sort=num_votes,desc&start={}&explore=genres&ref_=adv_nxt'
)
STEP = 50


import os
import re
from time import sleep
from pathlib import Path
from typing import Optional, Dict, List, Any, Union
from pandas import DataFrame, read_json
from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv
from recsys.utils import send_request, create_logger


BAR_FORMAT = '{percentage:3.0f}%|{bar:20}{r_bar}'
BASE_URL = 'https://www.imdb.com{}'
TOP_N_ACTORS = 10
BATCH_SIZE = 50


class MetadataCollector:
    """
    Contains methods for parsing IMDB movie details.
    Public methods:
        * collect_title_details: parses web page of a given movie.
        * is_all_metadata_collected: check if there any title we can scrape
        details about.
        * collect: parses pages and saves IDs on a disk or cloud.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes Metadata Collector class. All parameters related to web
        scraping of movie metadata must be specified in config.
        The config must contains the following fields:
        * mode: specifies where the results should be saved. When set up to
        'local' all movie related data will be saved on local machine, where
        application is running. When set up to 'cloud' related data saves on
        AWS. Using 'cloud' mode you also need to set up the following
        environment variables: AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY and
        AWS_S3_BUCKET.
        * metadata_file: name of file (possibly with folder) movies metadata
        will be saved to.
        * chunk_size: number of movies a program try to parse in one iteration.
        After each iteration there is a timout period to prevent too many
        requests.
        * sleep_time: time in seconds a program will be wait for before going
        to next movie. This parameter should be set reasonably, not too high
        (web scraping will last too long), not too low (increasing load on IMDB
        server for a long period of time is not ethical and such requests could
        be rate limited as a result).
        * log_file: file name to write logs related to collecting IDs.
        * log_level: minimal level of log messages.
        * log_msg_format: message format in logs.
        * log_dt_format: datetime format in logs.
        """
        self._mode = config['mode']
        self._chunk_size = config['chunk_size']
        self._sleep_time = config['sleep_time']

        if self._mode == 'cloud':
            load_dotenv()
            self._storage_options = {
                'key': os.getenv('AWS_ACCESS_KEY'),
                'secret': os.getenv('AWS_SECRET_ACCESS_KEY')
            }
            if (not self._storage_options['key'])\
                    or (not self._storage_options['secret']):
                raise ValueError(
                    'AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY'
                    + ' must be specified in environment variables'
                )

            self._bucket = os.getenv('AWS_S3_BUCKET')
            if not self._bucket:
                raise ValueError(
                    'AWS_S3_BUCKET must be specified in environment variables'
                )

            self._metadata_file = os.path.join(
                's3://', self._bucket, config['metadata_file']
            )
        elif self._mode == 'local':
            self._storage_options = None

            self._root_dir = str(Path(__file__).parents[2])
            self._metadata_file = os.path.join(
                self._root_dir,
                'data',
                config['metadata_file']
            )
        else:
            raise ValueError('Supported modes: "local", "cloud"')

        self._logger = create_logger(
            filename=config['log_file'],
            msg_format=config['log_msg_format'],
            dt_format=config['log_dt_format'],
            level=config['log_level']
        )

        self._logger.info('Successfully initialized MetadataCollector')

    @staticmethod
    def collect_title_details(soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Collects the following details (if exists) about a single movie:
            * original title
            * poster url
            * summary of reviews scores
            * average rating
            * actors starred in the movie
            * imdb recommendations to the movie
            * boxoffice
            * runtime
        """
        return {
            'original_title': collect_original_title(soup),
            'genres': collect_genres(soup),
            'poster_url': collect_poster_url(soup),
            'review_summary': collect_review_summary(soup),
            'agg_rating': collect_aggregate_rating(soup),
            'actors': collect_actors(soup),
            'imdb_recommendations': collect_imdb_recommendations(soup),
            'details': collect_details_summary(soup),
            'boxoffice': collect_boxoffice(soup)
        }

    def is_all_metadata_collected(self) -> bool:
        """
        Checks are there any movie in a database which metadata was not
        collected yet.
        """
        metadata_df = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        if 'genres' not in metadata_df.columns:
            already_collected = 0
        else:
            already_collected = (~metadata_df['genres'].isna()).sum()
        total_movies = len(metadata_df['genres'])

        print(
            f'Movie metadata is already collected for {already_collected}'
            + f' out of {total_movies} titles'
        )
        return total_movies == already_collected

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on a disk or cloud.
        """
        print('Collecting metadata...')

        movie_metadata_df = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        movie_metadata = movie_metadata_df.T.to_dict()

        title_ids = [t for t, _ in movie_metadata.items()]
        counter = 0
        session_counter = 0
        for i, title_id in tqdm(
            enumerate(title_ids), total=len(title_ids), bar_format=BAR_FORMAT
        ):
            if movie_metadata[title_id].get('original_title', None):
                continue

            url = BASE_URL.format(title_id)
            try:
                title_page = send_request(url)
                soup = BeautifulSoup(title_page.text, 'lxml')

                details = self.collect_title_details(soup)
                movie_metadata[title_id] |= details

                counter += 1

                self._logger.info(f'Collected metadata for title {title_id}')
                title_page.close()
            except Exception as e:
                self._logger.warn(f'Exception {str(e)} in parsing {url}')
            finally:
                sleep(self._sleep_time)

            # save results after if we have enough new data
            if (counter == BATCH_SIZE) | (i == len(title_ids) - 1):
                session_counter += counter
                counter = 0

                # update metadata file
                DataFrame(movie_metadata).to_json(
                    self._metadata_file,
                    storage_options=self._storage_options
                )

                self._logger.info(
                    f'Updated metadata file with {BATCH_SIZE} titles'
                )

                # stop program if we scraped many pages. This could be useful
                # if we have a limit on total running time (e.g. using
                # AWS Lambda)
                if session_counter >= self._chunk_size:
                    self._logger.info('Stop parsing due to requests limit')
                    return


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


class IDCollector:
    """
    Contains methods for parsing IMDB movie search web pages,
    then extract movie identifiers from them.
    Public method:
        collect: parses pages and saves IDs on a disk.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes Identifier Collector class. All parameters related to web
        scraping of movie identifiers must be specified in config.
        The config must contains the following fields:
        * mode: specifies where the results should be saved. When set up to
        'local' all movie related data will be saved on local machine, where
        application is running. When set up to 'cloud' related data saves on
        AWS. Using 'cloud' mode you also need to set up the following
        environment variables: AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY and
        AWS_S3_BUCKET.
        * metadata_file: name of file (possibly with folder) movies metadata
        will be saved to.
        * genres: list of genres you want to collect metadata about. It's also
        possible to set this field with 'all', in this case all available
        genres will be used. All possible genres can be found here
        https://www.imdb.com/feature/genre/?ref_=nv_ch_gr under
        "Popular Movies by Genre" title.
        * n_titles: number of titles to scrape information about in each genre.
        Set to null if want to use not absolute number of titles, but percent
        fraction. Titles in different genres could be overlapped.
        * pct_titles: percent of titles to scrape information about in each
        genre. Set to null if want to use absolute number of titles (parameter
        n_titles). Titles in different genres could be overlapped.
        * sleep_time: time in seconds a program will be wait for before going
        to next page. This parameter should be set reasonably, not too high (
        web scraping will last too long), not too low (increasing load on IMDB
        server for a long period of time is not ethical and such requests could
        be rate limited as a result).
        * log_file: file name to write logs related to collecting IDs.
        * log_level: minimal level of log messages.
        * log_msg_format: message format in logs.
        * log_dt_format: datetime format in logs.
        Notes:
        * One of these fields "n_titles" or "pct_titles" must be set to None,
            while the other set to desired value.
        """
        self._mode = config['mode']
        self._genres = config['genres']
        self._sleep_time = config['sleep_time']
        n_titles = config['n_titles']
        pct_titles = config['pct_titles']

        if self._mode == 'cloud':
            load_dotenv()
            self._storage_options = {
                'key': os.getenv('AWS_ACCESS_KEY'),
                'secret': os.getenv('AWS_SECRET_ACCESS_KEY')
            }
            if (not self._storage_options['key'])\
                    or (not self._storage_options['secret']):
                raise ValueError(
                    'AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY'
                    + ' must be specified in environment variables'
                )

            self._bucket = os.getenv('AWS_S3_BUCKET')
            if not self._bucket:
                raise ValueError(
                    'AWS_S3_BUCKET must be specified in environment variables'
                )

            self._metadata_file = os.path.join(
                's3://', self._bucket, config['metadata_file']
            )
        elif self._mode == 'local':
            self._storage_options = None

            root_dir = str(Path(__file__).parents[2])
            self._metadata_file = os.path.join(
                root_dir,
                'data',
                config['metadata_file']
            )
        else:
            raise ValueError('Supported modes: "local", "cloud"')

        self._logger = create_logger(
            filename=config['log_file'],
            msg_format=config['log_msg_format'],
            dt_format=config['log_dt_format'],
            level=config['log_level']
        )

        if not isinstance(self._genres, list):
            self._genres = [self._genres]

        if 'all' not in self._genres:
            use_genres = set(self._genres).intersection(GENRES)
            genre_diff = set(self._genres) - set(use_genres)
            if genre_diff:
                self._logger.warning(
                    f'No {", ".join(genre_diff)} in possible genres'
                )
            if not use_genres:
                raise ValueError('No valid genres were passed')
            self._genres = use_genres
        else:
            self._genres = GENRES

        if not (n_titles or pct_titles):
            raise ValueError(
                'Only one of these arguments needs to be set'
                ' in config file: n_titles or pct_titles'
            )
        if pct_titles:
            if not 0 <= pct_titles <= 100:
                raise ValueError(
                    'pct_titles must lie in the interval [0, 100]'
                )
            self._sample_size = {
                genre: int(pct_titles / 100 * MOVIE_COUNT_BY_GENRE[genre])
                for genre in self._genres
            }
        else:
            self._sample_size = {
                genre: min(n_titles, MOVIE_COUNT_BY_GENRE[genre])
                for genre in self._genres
            }

        self._logger.info('Successfully initialized IDCollector')

    @staticmethod
    def collect_movie_id(soup: BeautifulSoup) -> Dict[str, Dict[str, str]]:
        title_raw = soup.find_all('div', {'class': 'lister-item-content'})
        title_id = [t.a['href'] for t in title_raw]
        main_genre = [
            extract_main_genre(t.find('span', {'class': 'genre'}).text)
            for t in title_raw
        ]
        return {t: {'main_genre': g} for t, g in zip(title_id, main_genre)}

    def _collect_rank_id(self, genre, rank) -> Dict[str, Dict[str, str]]:
        url = URL_TEMPLATE.format(genre, rank)
        rank_id = {}
        try:
            response = send_request(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            if response.status_code == 200:
                old_len = len(rank_id)
                rank_id |= IDCollector.collect_movie_id(soup)
                self._logger.info(
                    f'Collected {len(rank_id) - old_len} new identifiers'
                    f' while parsing genre {genre.upper()},'
                    f' rank {rank}-{rank + STEP}'
                )
            else:
                self._logger.warning(
                    f'Bad status code in genre {genre.upper()},'
                    f' rank {rank}-{rank + STEP}'
                )
        except Exception as e:
            self._logger.warning(
                f'Exception in genre {genre.upper()},'
                f' rank {rank}-{rank + STEP}'
                f' with message: {e}'
            )
        finally:
            return rank_id

    def _collect_ids_for_genre(self, genre: str) -> Dict[str, Dict[str, str]]:
        genre_id = {}
        tqdm_params = {
            'iterable': range(1, self._sample_size[genre] + 1, STEP),
            'desc': genre,
            'unit_scale': STEP,
            'bar_format': BAR_FORMAT
        }
        for rank in tqdm(**tqdm_params):
            genre_id |= self._collect_rank_id(genre, rank)

            sleep(self._sleep_time)

        return genre_id

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on a disk or cloud.
        """
        print('Collecting identifiers...')

        id_genre = {}

        for genre in self._genres:
            old_len = len(id_genre)
            id_genre |= self._collect_ids_for_genre(genre)

            self._logger.info(
                f'Collected {len(id_genre) - old_len} new identifiers'
            )

            sleep(self._sleep_time)

        DataFrame.from_dict(id_genre, orient='index').to_json(
            self._metadata_file,
            storage_options=self._storage_options
        )


def extract_main_genre(s: str) -> str:
    return re.split(', | ', s.replace('\n', ''))[0]
