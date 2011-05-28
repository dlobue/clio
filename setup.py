from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='clio',
      version=version,
      description="project for interfacing with mongodb",
      long_description="""\
TODO""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Dominic LoBue',
      author_email='dominic@geodelic.com',
      url='',
      license='proprietary',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          "pymongo",
          "flask",
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
