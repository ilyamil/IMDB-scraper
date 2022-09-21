import os
import sys
from pathlib import Path

# path to package source code
src_path = os.path.join(
    Path(__file__).resolve().parents[2].as_posix(),
    'src'
)
sys.path.append(src_path)

from imdb_scrapper.utils import get_path # noqa


def test_get_path():
    file_structure = 'folder/in_folder/some_file.txt'
    true_path = os.path.join(
        os.path.abspath(os.path.join(src_path, '..')),
        'folder',
        'in_folder',
        'some_file.txt'
    )
    assert get_path(file_structure) == true_path
