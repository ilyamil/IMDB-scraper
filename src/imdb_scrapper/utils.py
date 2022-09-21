import os
import logging
from pathlib import Path
from typing import Literal
from requests import (
    ConnectionError,
    Timeout,
    Session,
    Response,
    get
)
from tenacity import (
    retry,
    wait_random,
    stop_after_attempt,
    retry_if_exception_type
)


ROOT_PATH = Path(__file__).resolve().parents[2].as_posix()
RETRY_PARAMS = {
    'retry': retry_if_exception_type((ConnectionError, Timeout)),
    'stop': stop_after_attempt(5),
    'wait': wait_random(1, 5)
}


def get_path(dirname_or_filename: str, filename: str = None) -> str:
    path_norm = os.path.normpath(dirname_or_filename)
    path_tokens = path_norm.split(os.sep)
    if not filename:
        return os.path.join(ROOT_PATH, *path_tokens)
    return os.path.join(
        ROOT_PATH, *path_tokens, filename if filename else ''
    )


def create_logger(
    filename: str,
    msg_format: str,
    dt_format: str,
    level: str
) -> logging.Logger:
    logger_params = {
        'filename': get_path(filename),
        'format': msg_format,
        'datefmt': dt_format,
        'level': level
    }
    logging.basicConfig(**logger_params)
    return logging.getLogger('')


@retry(**RETRY_PARAMS)
def send_request(url: str, session: Session = None, **params) -> Response:
    headers = {"Accept-Language": "en-US,en;q=0.5"}
    if session:
        return session.get(url, headers=headers, **params)
    return get(url, headers=headers, **params)


def check_mode_requirements(mode: Literal['local', 'cloud']):
    pass
