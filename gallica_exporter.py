#!/usr/bin/env python
import argparse
from fdh_gallica import Document, Periodical, Search
from fdh_gallica.utils import write_tuple_list
from fdh_gallica.parallel_process import generate_download_for_documents


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser("gallica_exporter.py",
                                          description="Generates a CSV of URLs and paths to the metadata, iiif images and alto OCR for a given document/periodical/search.")
    info_type = args_parser.add_mutually_exclusive_group(required=True)

    info_type.add_argument('-d',
                           '--document',
                           metavar='document',
                           type=str,
                           help='ark of the document')
    info_type.add_argument('-p',
                           '--periodical',
                           metavar='periodical',
                           type=str,
                           help='ark of the periodical')
    info_type.add_argument('-s',
                           '--search',
                           metavar='search_term',
                           type=str,
                           nargs='?',
                           default='',
                           help='search term in all the fields')

    args_parser.add_argument('--doc-type',
                             metavar='type',
                             type=str,
                             default='all',
                             help='document type e.g. \'image\' or \'fascicule\' (default all type of documents)')
    args_parser.add_argument('--search-field',
                             metavar='field search_term',
                             required=False,
                             type=str,
                             nargs=2,
                             help='fields to look for, c.f. gallica search API')
    args_parser.add_argument('--max-records',
                             metavar='max_records',
                             type=int,
                             default=-1,
                             help='maximum number of records to search for (closest multiple of 15)')

    args_parser.add_argument('-o',
                             '--output',
                             metavar='path',
                             type=str,
                             default='download_urls_paths.csv',
                             help='Output path of the exported file (default download_urls_paths.csv)')
    args_parser.add_argument('-b',
                             '--base-dir',
                             metavar='path',
                             type=str,
                             default='',
                             help='Base directory to download files (default \'./\')')
    args_parser.add_argument('--no-image',
                             action='store_false',
                             help="Download or not the images")
    args_parser.add_argument('--no-ocr',
                             action='store_false',
                             help="Download or not the alto OCR")
    args_parser.add_argument('-q',
                             '--quiet',
                             action='store_false',
                             help="disable console output")

    args = args_parser.parse_args()
    doc = args.document
    periodical = args.periodical
    search_term = args.search
    doc_type = args.doc_type
    search_field = args.search_field
    max_records = args.max_records
    output_path = args.output
    base_dir = args.base_dir
    export_images = args.no_image
    export_ocr = args.no_ocr
    quiet = args.quiet
    urls_paths = []

    if doc:
        document = Document(doc)
        urls_paths = document.generate_download(base_dir, export_images, export_ocr)
    elif periodical:
        series = Periodical(periodical)
        urls_paths = series.generate_download(base_dir, export_images, export_ocr, quiet)
    else:
        additional_fields = {search_field[0]: search_field[1]} if search_field else {}
        search = Search(search_term, doc_type,
                        dc_creator=None, dc_title=None,
                        and_query=True, **additional_fields)
        search.execute(max_records=max_records, processes=4, progress=quiet)
        retry = 0
        while retry < 5 and len(search.failures) > 0:
            search.retry(progress=quiet)
            retry += 1
        docs = search.documents
        urls_paths, failures = generate_download_for_documents(docs, base_dir, export_images, export_ocr,
                                                               progress=quiet)
        retry = 0
        while retry < 5 and len(failures) > 0:
            urls_paths_addon, failures = generate_download_for_documents(docs, base_dir, export_images, export_ocr,
                                                                         progress=quiet)
            retry += 1
            urls_paths.extend(urls_paths_addon)
    write_tuple_list(urls_paths, output_path)
