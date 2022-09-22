import pytest
from bs4 import BeautifulSoup
from imdb_scrapper.utils import send_request
from imdb_scrapper.movie_metadata import (
    collect_original_title,
    collect_director,
    collect_review_summary,
    collect_aggregate_rating,
    collect_actors,
    collect_genres,
    collect_imdb_recommendations,
    collect_details_summary,
    collect_boxoffice,
    collect_single_movie_metadata,
    collect_ids_from_single_page
)


EXAMPLE_URL = 'https://www.imdb.com/title/tt7991608/'


@pytest.fixture
def page() -> BeautifulSoup:
    page = send_request(EXAMPLE_URL)
    return BeautifulSoup(page.content, 'html.parser')


def test_collect_ids_from_single_page():
    url = (
        'https://www.imdb.com/search/title?'
        'genres=comedy&explore=title_type,genres'
    )
    ids = collect_ids_from_single_page(url)
    assert len(ids) == 50


def test_collect_single_movie_metadata(page):
    movie_metadata = collect_single_movie_metadata(EXAMPLE_URL)
    assert all([m for m in movie_metadata.values()])


def test_collect_original_title(page):
    orig_title = collect_original_title(page)
    assert orig_title is not None
    assert orig_title == 'Red Notice'


def test_collect_director(page):
    director = collect_director(page)
    assert director == 'Rawson Marshall Thurber'


def test_collect_review_summary(page):
    rev_content = collect_review_summary(page)
    summary_fields = ['user_review_num', 'critic_review_num', 'metascore']
    assert all(key in rev_content for key in summary_fields)
    assert all(len(v) > 0 for v in rev_content.values())


def test_collect_aggregate_rating(page):
    rating = collect_aggregate_rating(page)
    {'avg_rating': '9.0/10', 'num_votes': '2.6M'}
    assert rating is not None
    assert len(rating) == 2
    assert '/10' in rating['avg_rating']


def test_collect_actors(page):
    actors = collect_actors(page)
    assert actors is not None
    assert len(actors) > 0
    assert '1' in actors


def test_collect_imdb_recommendations(page):
    recommendations = collect_imdb_recommendations(page)
    assert recommendations is not None
    assert len(recommendations) > 0
    assert '1' in recommendations


def test_collect_genres(page):
    genres = collect_genres(page)
    true_genres = {'Action', 'Adventure', 'Comedy'}
    assert genres is not None
    assert true_genres.intersection(genres)


def test_collect_details(page):
    details_summary = collect_details_summary(page)
    sections = [
        'release_date',
        'countries_of_origin',
        'language',
        'also_known_as',
        'production_companies',
        'filming_locations',
        'runtime'
    ]
    assert all(section in details_summary.keys() for section in sections)


def test_collect_boxoffice(page):
    boxoffice = collect_boxoffice(page)
    assert boxoffice is not None
    assert 'budget' in boxoffice.keys()
    assert boxoffice['budget'] == '$160,000,000 (estimated)'
