# IMDB Scrapper
Python-based scraper of Internet Movie Database (IMDB) web-site

# Installation

1. Create credentials.yaml file in project root directory with the following content:

```
[aws]
access_key_id=<YOUR ACCESS KEY>
secret_access_key=<YOUR SECRET ACCESS KEY>
bucket=<YOUR BUCKET>
```
2. conda create --name imdb_scraper_env python=3.9
3. conda activate imdb_scraper_env
4. pip install -r requirements.txt
4. pip install -e .
