import requests
import xmltodict
from multiprocessing import Pool
from tqdm.autonotebook import tqdm


def request_and_parse_urls(xml_urls, processes=4, progress=True):
    results = []
    failures = []

    with Pool(processes) as p:
        map_result = list(tqdm(
            p.imap(_parallel_request, xml_urls),
            total=len(xml_urls),
            disable=(not progress)))
    for result, failure in map_result:
        if result:
            results.append(result)
        if failure:
            failures.append(failure)
    return results, failures


def request_and_parse(xml_url):
    result = requests.get(xml_url)
    result.raise_for_status()
    result_parsed = xmltodict.parse(result.content)
    return result_parsed


def _parallel_request(url):
    try:
        return request_and_parse(url), None
    except requests.exceptions.HTTPError:
        return None, url


def iiif_urls_for_documents(documents, processes=4, progress=True):
    results = []
    failures = []

    with Pool(processes) as p:
        map_result = list(tqdm(
            p.imap(_iiif_urls, documents),
            total=len(documents),
            disable=(not progress)))
    for result, failure in map_result:
        if result:
            results.append(result)
        if failure:
            failures.append(failure)
    return results, failures


def _iiif_urls(document):
    try:
        return (document, document.iiif_urls()), None
    except:
        return None, document

def generate_download_for_documents(documents, base_dir,
                                    export_images=True, export_ocr=True,
                                    processes=4, progress=True):
    results = []
    failures = []

    documents = [(document, base_dir, export_images, export_ocr) for
                 document in documents]

    with Pool(processes) as p:
        map_result = list(tqdm(
            p.imap(_download_urls_paths, documents),
            total=len(documents),
            disable=(not progress)))
    for result, failure in map_result:
        if result:
            results.extend(result)
        if failure:
            failures.append(failure)
    return results, failures


def _download_urls_paths(document):
    document, base_dir, export_images, export_ocr = document
    try:
        return document.generate_download(base_dir, export_images, export_ocr), None
    except:
        return None, document

def write_tuple_list(tuple_list, path):
    with open(path, 'w') as outfile:
        for tuple_ in tuple_list:
            outfile.write("%s,%s\n" % (tuple_))


def read_tuple_list(path):
    with open(path, 'r') as infile:
        lines = infile.readlines()
    return [line.strip().split(',') for line in lines]

def print_if_verbose(to_print, verbose=False):
    if verbose:
        print(to_print)


def makelist(item):
    if not isinstance(item, list):
        return [item]
    else:
        return item
