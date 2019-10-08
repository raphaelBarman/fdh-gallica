from urllib.parse import quote_plus

from .document import Document
from .parallel_process import request_and_parse_urls
from .utils import makelist, request_and_parse

NUM_RESULTS_PER_QUERY = 15
SRU_BASEURL = 'https://gallica.bnf.fr/SRU?version=1.2&operation=searchRetrieve&suggest=0&query='


class Search(object):
    """Gallica search object"""
    def __init__(self, all_fields=None, dc_type=None, dc_creator=None, dc_title=None, and_query=True,
                 **kwargs):
        """Accepts all the elements of a search query as arguments.
        The kwargs is an optional additional parameter that can be specified"""

        self.base_query = build_query(all_fields, dc_type, dc_creator, dc_title, and_query, **kwargs)
        self.total_records = self.get_total_records()
        if self.total_records <= 0:
            raise ValueError("Query did not yield any record")

    def execute(self, max_records=-1, processes=16, progress=True):
        """Execute the query of the search.
        max_records can be used to choose the closest multiple of NUM_RESULTS_PER_QUERY to retrieve.
        Its default value (-1) retrieves all records.
        Store the raw results in self.records
        Store the parsed document objects in self.documents
        Store the urls of failures in self.failures
        Returns True if all the records were retrieved, False if there is some failures"""

        total_records = self.total_records
        if max_records != -1 and max_records < self.total_records:
            total_records = max_records

        # Generate the list of all urls for all records up to total_records
        urls = [self.base_query +
                "&maximumRecords=%d" % NUM_RESULTS_PER_QUERY +
                "&startRecord=%d" % offset
                for offset in range(1, total_records, NUM_RESULTS_PER_QUERY)]
        records, failures = request_and_parse_search_queries(urls,
                                                             processes,
                                                             progress)
        self.documents = list(map(generate_document_from_record, records))
        self.records = records
        self.failures = failures
        return len(self.failures) == 0

    def retry(self, processes=1, progress=True):
        """Retry to execute the query only on the failed urls.
        Otherwise behaves like self.execute.
        """
        if len(self.failures) <= 0:
            return True
        records, failures = request_and_parse_search_queries(self.failures,
                                                             processes,
                                                             progress)
        self.records += records
        self.documents += list(map(generate_document_from_record, records))
        self.failures = failures
        return len(self.failures) == 0

    def get_total_records(self):
        """Fetch in the search result the total number of records"""
        result_parsed = request_and_parse(self.base_query + "&maximumRecords=0")
        try:
            return int(result_parsed['srw:searchRetrieveResponse']['srw:numberOfRecords'])
        except KeyError:
            return 0


def generate_document_from_record(record):
    """Given the xml of a record create a gallica document object"""
    try:
        ark = list(filter(lambda x: 'ark' in x, makelist(record['dc:identifier'])))[0].replace('https://gallica.bnf.fr/ark:/', '')
    except IndexError:
        raise ValueError("Record did not contain a valid ark")
    return Document(ark)

def build_query(all_fields=None, dc_type=None, dc_creator=None, dc_title=None, and_query=True, **kwargs):
    """Given different search arguments build the url of the search query."""

    def build_param(field, query, all_fields=True):
        return '%s%%20%s%%20%s' % (field, 'all' if all_fields else 'any', quote_plus(query))

    if not all_fields and not dc_type and not dc_creator and not dc_title and not kwargs:
        raise ValueError("Search should contain at least on query")
    query = SRU_BASEURL
    params = []
    if all_fields:
        params.append(build_param('gallica', all_fields))
    if dc_type:
        params.append(build_param('dc.type', dc_type, all_fields=False))
    if dc_creator:
        params.append(build_param('dc.creator', dc_creator))
    if dc_title:
        params.append(build_param('dc.title', dc_title))
    for field, search_term in kwargs.items():
        params.append(build_param(field, search_term))
    join_field = "%20and%20" if and_query else "%20or%20"
    query += join_field.join(params)
    return query


def request_and_parse_search_queries(urls, processes, progress):
    """Given the url of the search query, get it and unwrap the records to get their dublin core."""
    records = []
    results, failures = request_and_parse_urls(urls, processes, progress)
    for result_parsed in results:
        records += unwrap_records(result_parsed)
    return records, failures


def unwrap_records(parsed_xml):
    """Given a parsed xml of search records, unwrap them to get parsed xml of their dublin core"""
    try:
        records = parsed_xml['srw:searchRetrieveResponse']['srw:records']['srw:record']
        return [record['srw:recordData']['oai_dc:dc'] for record in records]
    except KeyError:
        return []
