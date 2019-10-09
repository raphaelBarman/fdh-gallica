import requests
import xmltodict
from .parallel_process import parallel_process


def iiif_urls_for_documents(documents, processes=4, progress=True):
    """Generates all the iiif urls for the given Gallica document object"""
    return parallel_process(_iiif_urls, documents, processes, progress)


def _iiif_urls(document):
    """Wrapper of the Document.iiif_urls function to work in parallel"""
    try:
        return (document, document.iiif_urls()), None
    except:
        return None, document


def generate_download_for_documents(documents, base_dir,
                                    export_images=True, export_ocr=True,
                                    processes=4, progress=True):
    """Generate download list of urls and paths for a list of Gallica document objects"""
    documents = [(document, base_dir, export_images, export_ocr) for
                 document in documents]
    return parallel_process(_urls_paths, documents, processes, progress)


def _urls_paths(document):
    """Wrapper for Document.generate_download to work in parallel"""
    document, base_dir, export_images, export_ocr = document
    try:
        return document.generate_download(base_dir, export_images, export_ocr), None
    except:
        return None, document


def request_and_parse(xml_url):
    """Get an xml url and parse it into a python dict"""
    result = requests.get(xml_url)
    result.raise_for_status()
    result_parsed = xmltodict.parse(result.content)
    return result_parsed


def write_tuple_list(tuple_list, path):
    """Given a list of tuples ([(x1,x2), (x3,x4), ...]) write it as a csv in a file"""
    with open(path, 'w') as outfile:
        for tuple_ in tuple_list:
            outfile.write("%s,%s\n" % (tuple_))


def read_tuple_list(path):
    """Read a csv of a tuple list"""
    with open(path, 'r') as infile:
        lines = infile.readlines()
    return [line.strip().split(',') for line in lines]


def print_if_verbose(to_print, verbose=False):
    """Print wrapper to print only if verbose is True"""
    if verbose:
        print(to_print)


def makelist(item):
    """Given an item, make sure it is a list.
    This is necessary because xmltodict often parse items either in a single object or a list of object."""
    if not isinstance(item, list):
        return [item]
    else:
        return item
