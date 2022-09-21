import os
from pathlib import Path
from imdb_scrapper.utils import get_path, send_request


def test_get_path():
    file_structure = 'folder/in_folder/some_file.txt'
    src_path = os.path.join(
        Path(__file__).resolve().parents[2].as_posix(),
        'src'
    )
    true_path = os.path.join(
        os.path.abspath(os.path.join(src_path, '..')),
        'folder',
        'in_folder',
        'some_file.txt'
    )
    assert get_path(file_structure) == true_path


def test_send_request():
    url = 'https://google.com'
    assert send_request(url).status_code == 200
