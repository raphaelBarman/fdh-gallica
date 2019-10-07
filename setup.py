#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='fdh_gallica',
      version='1.0',
      description='FDH gallica package',
      author='Raphael Barman',
      author_email='raphael.barman@epfl.ch',
      packages=find_packages('fdh_gallica'),
      install_requires=[
          'lxml',
          'pillow',
          'requests',
          'tqdm',
          'xmltodict'
      ])
