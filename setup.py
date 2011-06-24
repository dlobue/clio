from setuptools import setup, find_packages
import sys, os

version = '0.2.3'

setup(name='clio',
      version=version,
      description="project for interfacing with mongodb",
      #long_description="""\TODO""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Dominic LoBue',
      author_email='dominic@geodelic.com',
      url='',
      license='proprietary',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      scripts=['bin/clio'],
      data_files=[('/etc/init', ['init_scripts/clio.conf']),
                  ('/etc/clio', ['conf/daemon.conf', 'conf/app.conf']),
                 ],
      zip_safe=False,
      install_requires=[
          "pymongo>=1.10.0",
          "flask>=0.6.1",
          "simpledaemon>=1.0.1",
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
