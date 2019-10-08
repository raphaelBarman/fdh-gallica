import requests
import xmltodict


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
