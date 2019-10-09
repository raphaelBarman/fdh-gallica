from multiprocessing import Pool

import requests
from tqdm.autonotebook import tqdm

from .utils import request_and_parse


def parallel_process(func, items, processes=4, progress=True):
    """Process in parallel a list of item with a given function
    The function should return tuple of (success, failure).
    Each of them will be stored in a list of results and failures if not None"""
    results = []
    failures = []

    with Pool(processes) as p:
        map_result = list(tqdm(
            p.imap(func, items),
            total=len(items),
            disable=(not progress)))
    for result, failure in map_result:
        if result:
            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)
        if failure:
            failures.append(failure)
    return results, failures


def request_and_parse_urls(xml_urls, processes=4, progress=True):
    """Get and parse using xmltodict the urls of XMLs"""
    return parallel_process(_request_and_parse, xml_urls, processes, progress)


def _request_and_parse(url):
    """Wrapper of the previous function to work in parallel"""
    try:
        return request_and_parse(url), None
    except requests.exceptions.HTTPError:
        return None, url
