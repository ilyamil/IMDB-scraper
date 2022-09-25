import os
import logging
import yaml
from pathlib import Path
from typing import Literal, Optional, Dict, Any
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
    log_file: str,
    log_msg_format: str,
    log_dt_format: str,
    log_level: str
) -> logging.Logger:
    logger_params = {
        'filename': get_path(log_file),
        'format': log_msg_format,
        'datefmt': log_dt_format,
        'level': log_level
    }
    logging.basicConfig(**logger_params)
    return logging.getLogger('')


@retry(**RETRY_PARAMS)
def send_request(url: str, session: Session = None, **params) -> Response:
    headers = {"Accept-Language": "en-US,en;q=0.5"}
    if session:
        return session.get(url, headers=headers, **params)
    return get(url, headers=headers, **params)


def read_yaml(path: str) -> Optional[Dict[str, Any]]:
    y = None
    with open(path, "r") as stream:
        y = yaml.safe_load(stream)
    return y


def check_mode_requirements(mode: Literal['local', 'cloud']):
    pass
