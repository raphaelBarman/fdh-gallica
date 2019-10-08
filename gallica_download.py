#!/usr/bin/env python
import argparse

from fdh_gallica.download import download_with_retry
from fdh_gallica.utils import read_tuple_list, write_tuple_list

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser("gallica_download.py",
                                          description="Given a CSV containing urls and paths, download them all and check if they are valid jpegs and XMLs.")
    args_parser.add_argument('urls_paths',
                             metavar='urls_paths_csv',
                             type=str,
                             help='path to the CSV file containing urls and paths')
    args_parser.add_argument('-p',
                             '--processes',
                             metavar='n_processes',
                             type=int,
                             default=4,
                             help='number of processes to spawn (default 4)')
    args_parser.add_argument('-r',
                             '--retry',
                             metavar='n_retries',
                             type=int,
                             default=5,
                             help='number of retries (default 5)')
    args_parser.add_argument('-f',
                             '--failures',
                             metavar='failures_path',
                             type=str,
                             default=None,
                             help='optionally store failures')
    args_parser.add_argument('-q',
                             '--quiet',
                             action='store_false',
                             help="disable console output")

    args = args_parser.parse_args()
    urls_paths_path = args.urls_paths
    processes = args.processes
    num_retry = args.retry
    failures_path = args.failures
    quiet = args.quiet

    urls_paths = read_tuple_list(urls_paths_path)
    failures = download_with_retry(urls_paths, num_retry, processes, quiet)
    if failures_path:
        write_tuple_list(failures, failures_path)
