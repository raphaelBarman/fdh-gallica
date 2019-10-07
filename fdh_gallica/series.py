from .base import GallicaObject
from .document import Document
from .utils import request_and_parse_urls, request_and_parse, makelist
import requests
import xmltodict
import os

ISSUES_BASEURL = 'https://gallica.bnf.fr/services/Issues?ark=ark:'


class Series(GallicaObject):

    def issues(self, use_cache=True, processes=4, progress=False):
        years = self.years_of_issues()
        urls = ["/".join([ISSUES_BASEURL, self.ark, 'date&date=%s' % year])
                for year in years]

        if use_cache and hasattr(self, 'documents') and hasattr(self, 'failures'):
            if len(self.failures) == 0:
                return self.documents
            else:
                urls = self.failures

        results, failures = request_and_parse_urls(urls, processes, progress)
        issues = []
        for parsed_result in results:
            issues += self.parse_issues(parsed_result)
        self.documents = issues
        self.failures = failures
        return issues

    def years_of_issues(self):
        url = "/".join([ISSUES_BASEURL, self.ark, 'date'])
        response = requests.get(url)
        parsed_response = xmltodict.parse(response.content)
        try:
            years = makelist(parsed_response['issues']['year'])
            return years
        except KeyError:
            print("missing issues year keys", parsed_response)
            return []

    def issues_per_year(self, year):
        url = "/".join([ISSUES_BASEURL, self.ark, 'date&date=%s' % year])
        parsed_response = request_and_parse(url)
        return self.parse_issues(parsed_response)

    def parse_issues(self, parsed_response):
        try:
            issues = makelist(parsed_response['issues']['issue'])
            return [Document('/'.join([self.authority, issue['@ark']]))
                    for issue in issues]
        except KeyError:
            print("missing issues keys", parsed_response)
            return []

    def generate_download(self, base_path='', export_images=True, export_ocr=True):
        urls_path = []
        years = self.years_of_issues()
        for year, issues in zip(years, self.issues()):
            year_path = os.path.join(base_path, year)
            urls_path.extend(issues.generate_download(year_path))
        return urls_path
