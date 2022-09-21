from imdb_scrapper.movie_metadata import (
    collect_single_movie_metadata,
    collect_ids_from_single_page
)


def test_collect_ids_from_single_page():
    url = (
        'https://www.imdb.com/search/title?'
        'genres=comedy&explore=title_type,genres'
    )
    ids = collect_ids_from_single_page(url)
    assert len(ids) == 50


def test_collect_single_movie_metadata():
    assert False
