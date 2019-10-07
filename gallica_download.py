import requests
import shutil
import os
import argparse
from multiprocessing import Pool
from tqdm.autonotebook import tqdm
from PIL import Image
from lxml import etree
from fdh_gallica.utils import read_tuple_list, write_tuple_list, print_if_verbose


def download_with_retry(urls_path, num_retry=5, processes=4, progress=True, verbose=False):
    num_retry_per_path = {path: 1 for _, path in urls_path}
    definitive_failures = []
    print_if_verbose("Downloading files", verbose)
    failures = set(download(urls_path, processes, progress))
    print_if_verbose("Verifying files", verbose)
    failures = failures.union(set(check_downloads(urls_path)))
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
        failures = set(download(retry_list, processes=2, progress=progress))
        print_if_verbose("Verifying files", verbose)
        failures = failures.union(set(check_downloads(retry_list)))
    return definitive_failures


def download(urls_path, processes=4, progress=True, leave_progress=True):
    failures = []
    with Pool(processes) as p:
        map_results = list(tqdm(
            p.imap(download_item, urls_path),
            total=len(urls_path),
            disable=(not progress),
            leave=leave_progress
        ))
    for res in map_results:
        if res:
            failures.append(res)
    return failures


def download_item(url_path):
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
        return (url, path)
    else:
        return None


def check_downloads(urls_path, processes=4, progress=True, leave_progress=True):
    failures = []
    with Pool(processes) as p:
        map_results = list(tqdm(
            p.imap(check_item, urls_path),
            total=len(urls_path),
            disable=(not progress),
            leave=leave_progress
        ))
    for res in map_results:
        if res:
            failures.append(res)
    return failures


def check_item(url_path):
    url, path = url_path
    checker = lambda x: False
    if path.endswith('.jpg'):
        checker = check_jpg
    elif path.endswith('.xml'):
        checker = check_xml
    if checker(path):
        return None
    else:
        return (url, path)


def check_jpg(jpg_path):
    try:
        im = Image.open(jpg_path)
        im.verify() #I perform also verify, don't know if he sees other types o defects
        im.close() #reload is necessary in my case
        im = Image.open(jpg_path)
        im.transpose(Image.FLIP_LEFT_RIGHT)
        im.close()
    except:
        return False
    return True


def check_xml(xml_path):
    try:
        etree.parse(xml_path)
    except:
        return False
    return True


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser("gallica_download.py")
    args_parser.add_argument('urls_paths',
                             metavar='urls',
                             action='store',
                             type=str,
                             help='path to the directory')
    args_parser.add_argument('-p',
                             '--processes',
                             metavar='n_processes',
                             action='store',
                             type=int,
                             default=4,
                             help='number of processes to spawn (default 4)')
    args_parser.add_argument('-r',
                             '--retry',
                             metavar='n_retries',
                             action='store',
                             type=int,
                             default=5,
                             help='number of retries (default 5)')
    args_parser.add_argument('-f',
                             '--failures',
                             metavar='failures_path',
                             action='store',
                             type=str,
                             default=None,
                             help='optionally store failures')

    args = args_parser.parse_args()
    urls_paths_path = args.urls_paths
    processes = args.processes
    num_retry = args.retry
    failures_path = args.failures

    urls_paths = read_tuple_list(urls_paths_path)
    failures = download_with_retry(urls_paths, num_retry, processes, verbose=True)
    if failures_path:
        write_tuple_list(failures, failures_path)
