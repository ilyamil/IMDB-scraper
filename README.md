# IMDB Scraper
Python-based scraper of Internet Movie DataBase (IMDB) web-site.

# Disclaimer
*I created this script to collect data about movies for non-commercial use in my side projects. Using this data I have a goal to learn something new and interesting.
For ethical reasons it's better to not use this script too often as it loads IMDB's servers without providing any value to web-site.*

# Installation

1. Create credentials.yaml file in project's root directory with the following content:

```
aws:
    access_key_id: <YOUR ACCESS KEY>
    secret_access_key: <YOUR SECRET ACCESS KEY>
    bucket: <YOUR BUCKET>
```
2. Run in terminal ```conda create --name imdb_scraper_env python=3.9```
3. Run in terminal ```conda activate imdb_scraper_env```
4. Run in terminal ```pip install -r requirements.txt```
5. Run in terminal ```pip install -e .```

# Scraping
1. Edit (if needed) a **config.yaml** file with details related to scraping. The most important config's entries are:
    * _pct_titles_ (control percent of titles to collect in each genre) under _metadata_ heading
    * _pct_reviews_ (control percent of reviews to collect of each movie) under _reviews_ heading
    * _genres_ (control genres that movie must be assigned to) under _metadata_ heading. In most cases a movie has more than 1 genre, that is why the total number of collected movies will be less than sum of number of movies in each genre
2. Run in terminal ```python run_scraper.py -e metadata``` to extract movie metadata
3. Run in terminal ```python run_scraper.py -e reviews``` to extract movie reviews
4. Run in terminal ```python run_scraper.py -e preprocess``` to preprocess raw data

# Facts
It takes ~20 hours to execute the script from start to end with default configurations on virtual machine. After this time a dataset of size 23k movies and 3 mln text reviews about these movies will be collected.
