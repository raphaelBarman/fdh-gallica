import os

import requests
import xmltodict

from .base import GallicaObject
from .utils import makelist

OAI_BASEURL = 'https://gallica.bnf.fr/services/OAIRecord?ark=ark:'
PAGINATION_BASEURL = 'https://gallica.bnf.fr/services/Pagination?ark='
ALTO_BASEURL = 'https://gallica.bnf.fr/RequestDigitalElement?O=%s&E=ALTO&Deb=%s'
IIIF_BASEURL = 'https://gallica.bnf.fr/iiif/ark:'


class Document(GallicaObject):
    """Gallica document object"""

    def __init__(self, ark):
        GallicaObject.__init__(self, ark)
        self.oai_dict = None

    def oai(self, parse_xml=True):
        """Retrieve the XML of the OAI information for the document"""
        url = self.oai_url()
        response = requests.get(url)
        parsed_response = xmltodict.parse(response.content)
        self.oai_dict = parsed_response
        if parse_xml:
            return parsed_response
        else:
            return response.content

    def oai_url(self):
        return "/".join([OAI_BASEURL, self.ark])

    def iiif_urls(self):
        """Give all the urls of the IIIF images related to the document"""
        numbers = self.page_numbers()
        urls = [self.iiif_url_for_page(number) for number in numbers]
        return urls

    def iiif_url_for_page(self, page):
        url = "/".join([IIIF_BASEURL, self.ark, 'f%s' % page, 'full/full/0/native.jpg'])
        return url

    def alto_urls(self):
        """Give the urls of the XML ALTO ocr if it exists"""
        if not self.has_alto():
            raise ValueError("Document does not have OCR")
        numbers = self.page_numbers()
        urls = [self.alto_url_for_page(number) for number in numbers]
        return urls

    def alto_url_for_page(self, page):
        if not self.has_alto():
            return ""
        url = ALTO_BASEURL % (self.ark_name, page)
        return url

    def has_alto(self):
        """Check if the document has OCR by checking its nqamoyen as explained in the Gallica documentation"""
        if not self.oai_dict:
            self.oai()
        try:
            return float(self.oai_dict['results']['nqamoyen']) >= 50.0
        except KeyError:
            return False
        except ValueError:
            return False

    def page_numbers(self):
        """Give a list of the page numbers"""
        pagination_info = self.pagination()
        try:
            pages = makelist(pagination_info['livre']['pages']['page'])
            return [page['ordre'] for page in pages]
        except KeyError:
            return []

    def pagination(self, use_cache=True):
        """Query the pagination API to get page numbers"""
        if hasattr(self, "pagination_response"):
            return self.pagination_response
        url = "".join([PAGINATION_BASEURL, self.ark_name])
        response = requests.get(url)
        parsed_response = xmltodict.parse(response.content)
        self.pagination_response = parsed_response
        return parsed_response

    def generate_download(self, base_path='', export_images=True, export_ocr=True):
        """Generate a list of urls for the OAI metadata, IIIF urls and ALTO urls of the document"""
        urls_paths = []
        base_path = os.path.join(base_path, self.ark_name)
        urls_paths.append((self.oai_url(), os.path.join(base_path, self.ark_name + '_oai.xml')))
        iiif_urls = self.iiif_urls()
        page_numbers = self.page_numbers()
        if export_images and len(iiif_urls) > 0:
            images_dir = os.path.join(base_path, 'images')
            for page_num, image_url in zip(page_numbers, iiif_urls):
                urls_paths.append((image_url, os.path.join(
                    images_dir,
                    "%s_%03d.jpg" % (self.ark_name, int(page_num))
                )))
        if export_ocr and self.has_alto():
            alto_dir = os.path.join(base_path, 'alto')
            for page_num, alto_url in zip(page_numbers, self.alto_urls()):
                urls_paths.append((alto_url, os.path.join(
                    alto_dir,
                    "%s_%03d.xml" % (self.ark_name, int(page_num))
                )))
        return urls_paths
