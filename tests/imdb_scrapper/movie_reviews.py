from imdb_scraper.movie_reviews import (
    get_reviews_total_count,
    collect_single_movie_reviews
)


class DummyLogger:
    def info(*args):
        pass

    def warn(*args):
        pass


def test_get_reviews_total_count():
    url = 'https://www.imdb.com/title/tt0372784/reviews?ref_=tt_ov_rt'
    reviews_total_count = get_reviews_total_count(url)
    assert reviews_total_count > 3_000


def test_collect_single_movie_reviews():
    reviews = collect_single_movie_reviews(
        '/title/tt0118767/',
        100,
        DummyLogger()
    )
    assert len(reviews) > 50
    assert all(c in reviews[0].keys() for c in [
        'text', 'rating', 'date', 'title', 'author', 'helpfulness'
    ])
    assert (reviews[0]['date'] is not None)\
           & (reviews[0]['text'] is not None)\
           & (reviews[0]['helpfulness'] is not None)
