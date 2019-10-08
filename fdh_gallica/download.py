import os
import shutil
from multiprocessing import Pool

import requests
from lxml import etree
from PIL import Image
from tqdm.autonotebook import tqdm

from .utils import print_if_verbose
from .parallel_process import parallel_process


def download_with_retry(urls_path, num_retry=5,
                        processes=4,
                        verbose=False):
    """Given a list of urls and paths, download each url to its given path. Retry for each url up to num_retry."""
    num_retry_per_path = {path: 1 for _, path in urls_path}
    definitive_failures = []
    print_if_verbose("Downloading files", verbose)
    failures = set(download(urls_path, processes, verbose))
    print_if_verbose("Verifying files", verbose)
    failures = failures.union(set(check_downloads(urls_path, processes, verbose)))
    while len(failures) > 0:
        print("Retry failures")
        retry_list = []
        for url, path in failures:
            num_retry_per_path[path] += 1
            if num_retry_per_path[path] <= num_retry:
                retry_list.append((url, path))
            else:
                definitive_failures.append((url, path))
        print_if_verbose("Downloading files", verbose)
        failures = set(download(retry_list, processes=2, progress=verbose))
        print_if_verbose("Verifying files", verbose)
        failures = failures.union(set(check_downloads(retry_list, processes, verbose)))
    return definitive_failures


def download(urls_path, processes=4, progress=True, leave_progress=True):
    """Given a list of urls and paths, download each url to its given path."""
    return parallel_process(download_item, urls_path, processes, progress)[1]


def download_item(url_path):
    """Download an url to a given path"""
    url, path = url_path
    try:
        dirname = os.path.dirname(path)
        os.makedirs(dirname, exist_ok=True)
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(path, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
    except:
        return None, (url, path)
    else:
        return None, None


def check_downloads(urls_path, processes=4, progress=True, leave_progress=True):
    """Check that all the paths are valid files"""
    return parallel_process(check_item, urls_path, processes, progress)[1]


def check_item(url_path):
    """Check that a path is a valid file"""
    url, path = url_path
    checker = lambda x: False
    if path.endswith('.jpg'):
        checker = check_jpg
    elif path.endswith('.xml'):
        checker = check_xml
    if checker(path):
        return None, None
    else:
        return None, (url, path)


def check_jpg(jpg_path):
    """Check a jpeg file using pillow"""
    try:
        # Verifiy is faster to fail
        im = Image.open(jpg_path)
        im.verify()
        im.close()
        # Check if file is corrupted or truncated
        im = Image.open(jpg_path)
        im.transpose(Image.FLIP_LEFT_RIGHT)
        im.close()
    except:
        return False
    return True


def check_xml(xml_path):
    """Check that a file is a valid XML"""
    try:
        etree.parse(xml_path)
    except:
        return False
    return True
