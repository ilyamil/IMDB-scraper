"""
Module contains functions to Extract, Transform, Load (ETL)
raw data collected by imdb parser.
"""
import pandas as pd
from typing import Dict, List, Any


def preprocess_metadata(config: Dict[str, Any], credentials: Dict[str, Any]):
    """Process raw metadata to extract useful entites."""
    storage_options = {
        'key': credentials['aws']['access_key'],
        'secret': credentials['aws']['secret_access_key']
    }
    s3_source_uri = f's3://{credentials["aws"]["bucket"]}/{config["source"]}'
    s3_target_uri = f's3://{credentials["aws"]["bucket"]}/{config["target"]}'

    metadata = pd.read_json(
        s3_source_uri,
        storage_options=storage_options,
        orient='index'
    )
    transformed_metadata = (
        metadata
        .pipe(split_aggregate_rating)
        .pipe(split_review_summary)
        .pipe(split_movie_genres)
        .pipe(split_movie_details)
        .pipe(split_boxoffice)
    )
    transformed_metadata.columns.name = None

    transformed_metadata.to_json(
        s3_target_uri,
        orient='index',
        storage_options=storage_options
    )
    return transformed_metadata


def normalize(df: pd.DataFrame, col: str) -> pd.DataFrame:
    norm = pd.json_normalize(df[col])
    norm.columns = norm.columns.astype(int)
    return norm


def split_helpfulness_col(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split 'helpfulness' column of input dataframe into two
    distinct columns: 'upvotes' and 'total_votes'.
    After transformation 'helpfulness' column is removed.
    """
    if 'helpfulness' not in df_raw.columns:
        raise ValueError('No "helpfulness" column in input data')

    df_ = df_raw.copy(deep=False)
    df_[['upvotes', 'total_votes']] = (
        df_['helpfulness']
        .str.replace(',', '')
        .str.extractall('(\d+)') # noqa
        .unstack('match')
        .values
        .astype('float32')
    )
    return df_.drop(columns=['helpfulness'])


def correct_review_author(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes all review author identifiers in column 'author'
    by preserving only minimum valid part in form of '/user/urXXXXXX'.
    """
    if 'author' not in df_raw.columns:
        raise ValueError('No "author" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['author'] = df_['author'].astype(str).str.split('?', expand=True)[0]
    return df_


def cut_off_review_title_newline(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Removes '\n' at the end of each review title. 'title' column required.
    """
    if 'title' not in df_raw.columns:
        raise ValueError('No "title" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['title'] = df_['title'].astype(str).str.rstrip('\n')
    return df_


def change_review_dtypes(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Changes column data types to reduce memory footprint.
    """
    type_mapping = {
        'upvotes': 'int16',
        'total_votes': 'int16',
        'rating': 'float32'
    }
    return df_raw.astype(type_mapping)


def convert_to_date(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert 'date' column of type 'object' of input data to 'review_date'
    column of type datetime64[ns]. After transformation the 'date' column
    is removed.
    """
    if 'date' not in df_raw.columns:
        raise ValueError('No "date" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['review_date'] = pd.to_datetime(df_['date'])
    return df_.drop(columns=['date'])


def split_aggregate_rating(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split column 'agg_rating' of type string into two columns:
    'movie_rating' and 'num_votes'.
    After transformation the 'agg_rating' column is removed.
    """
    if 'agg_rating' not in df_raw.columns:
        raise ValueError('No "agg_rating" column in input data')

    df_ = df_raw.copy(deep=False)
    df_ = df_[~df_['agg_rating'].isna()]

    short_forms = {
        'K': 'e+03',
        'M': 'e+06',
        'B': 'e+09',
        'T': 'e+12'
    }
    rating = pd.json_normalize(df_['agg_rating']).replace(
        short_forms, regex=True
    )
    rating['rating'] = rating['avg_rating'].str.split('/', expand=True)[0]
    df_[['rating', 'num_votes']] = (
        rating
        [['rating', 'num_votes']]
        .astype('float32')
        .values
    )

    return df_.drop('agg_rating', axis=1)


def split_review_summary(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split column 'review_summary' into 3 columns: 'user_reviews_num',
    'critic_reviews_num', 'metascore'.
    After transformation the 'review_summary' columns is removed.
    """
    if 'review_summary' not in df_raw.columns:
        raise ValueError('No "review_summary" column in input data')

    df_ = df_raw.copy(deep=False)

    short_forms = {
        'K': 'e+03',
        'M': 'e+06',
        'B': 'e+09',
        'T': 'e+12'
    }

    df_[['user_review_num', 'critic_review_num', 'metascore']] = (
        pd.json_normalize(df_['review_summary'])
        .replace(short_forms, regex=True)
        .astype('float32')
        .values
    )

    return df_.drop('review_summary', axis=1)


def format_release_date(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'release_date' not in df_raw.columns:
        raise ValueError('No "release_date" column in input data')

    df_ = df_raw.copy(deep=False)
    rd = (
        df_['release_date']
        .explode()
        .str.split('(', expand=True)
        [0]
        .str.rstrip()
    )
    df_['release_date'] = (
        pd.to_datetime(rd, errors='coerce')
        .dt.strftime('%Y-%m-%d')
    )
    return df_


def format_runtime(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'runtime' not in df_raw.columns:
        raise ValueError('No "release_date" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['runtime'] = pd.to_timedelta(df_['runtime']) / pd.Timedelta('1 minute')
    return df_


def format_aka(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'also_known_as' not in df_raw.columns:
        raise ValueError('No "also_known_as" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['also_known_as'] = df_['also_known_as'].explode()
    return df_


def split_countries_of_origin(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'countries_of_origin' not in df_raw.columns:
        raise ValueError('No "countries_of_origin" column in input data')

    df_ = df_raw.copy(deep=False)

    countries_df = df_['countries_of_origin'].apply(pd.Series)
    for num in [1, 2]:
        if num not in countries_df.columns:
            countries_df[num] = None

    countries_df = countries_df[[0, 1, 2]].rename(columns={
            0: 'country_of_origin_1',
            1: 'country_of_origin_2',
            2: 'country_of_origin_3'}
    )
    df_ = pd.concat([df_, countries_df], axis=1)
    return df_.drop('countries_of_origin', axis=1)


def split_language(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'language' not in df_raw.columns:
        raise ValueError('No "language" column in input data')

    df_ = df_raw.copy(deep=False)
    languages_df = df_['language'].apply(pd.Series)

    if 1 in languages_df.columns:
        languages_df[0] = languages_df[0].fillna(languages_df[1])

    df_['original_language'] = languages_df[0]
    return df_.drop('language', axis=1)


def split_production_companies(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'production_companies' not in df_raw.columns:
        raise ValueError('No "production_companies" column in input data')

    df_ = df_raw.copy(deep=False)

    companies_df = df_['production_companies'].apply(pd.Series)
    for num in [1, 2]:
        if num not in companies_df.columns:
            companies_df[num] = None

    companies_df = companies_df.rename(columns={
            0: 'production_company_1',
            1: 'production_company_2',
            2: 'production_company_3'}
    )
    df_ = pd.concat([df_, companies_df], axis=1)
    return df_.drop('production_companies', axis=1)


def get_filming_country(s: List) -> str:
    try:
        return s[-1]
    except Exception:
        return None


def split_filming_locations(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'filming_locations' not in df_raw.columns:
        raise ValueError('No "filming_locations" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['filming_location'] = df_['filming_locations'].apply(pd.Series)
    df_['filming_country'] = (
        df_['filming_location']
        .str.split()
        .apply(get_filming_country)
    )
    return df_.drop('filming_locations', axis=1)


def split_movie_details(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Splits a column 'details' into 7 column groups and add them to dataframe:
        * 'release_date'
        * 'country_of_origin_1', 'country_of_origin_2', 'country_of_origin_3'
        * 'original_language'
        * 'also_known_as'
        * 'production_company_1', 'production_company_2', 'production_company_3' # noqa
        * 'filming_location', 'filming_country'
        * 'runtime'
    After transformation the 'details' column is removed.
    """
    if 'details' not in df_raw.columns:
        raise ValueError('No "details" column in input data')

    df_ = df_raw.reset_index()
    df_details = (
        pd.json_normalize(df_['details'])
        .pipe(format_release_date)
        .pipe(format_aka)
        .pipe(format_runtime)
        .pipe(split_countries_of_origin)
        .pipe(split_language)
        .pipe(split_production_companies)
        .pipe(split_filming_locations)
    )
    df_out = (
        pd.concat([df_, df_details], axis=1)
        .drop(columns='details')
        .set_index('index')
    )
    df_out.columns.name = None
    return df_out


def split_movie_genres(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'genres' not in df_raw.columns:
        raise ValueError('No "genres" column in input data')

    df_ = df_raw.reset_index()

    df_genres = df_['genres'].apply(pd.Series)
    for num in [1, 2]:
        if num not in df_genres.columns:
            df_genres[num] = None

    df_genres = df_genres.rename(
        columns={0: 'genre_1', 1: 'genre_2', 2: 'genre_3'}
    )

    df_out = (
        pd.concat((df_, df_genres), axis=1)
        .drop(columns='genres')
        .set_index('index')
    )
    df_out.columns.name = None
    return df_out


def split_boxoffice(df_raw: pd.DataFrame) -> pd.DataFrame:
    if 'boxoffice' not in df_raw.columns:
        raise ValueError('No "boxoffice" column in input data')

    df_ = df_raw.reset_index()
    df_boxoffice = (
        pd.json_normalize(df_['boxoffice'])
    )
    df_out = (
        pd.concat((df_, df_boxoffice), axis=1)
        .drop('boxoffice', axis=1)
        .set_index('index')
    )
    df_out.columns.name = None
    return df_out
