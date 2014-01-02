#!/usr/bin/env python

'''
Created on Aug 11, 2013

@author: Nicklas Boerjesson
'''


from setuptools import setup
from qal import __release__
if __name__ == "__main__":
    setup(
        name='qal',
        version=__release__,
        description='QAL is a Python library for mixing data sources into SQL statements.',
        author='Nicklas Boerjesson',
        author_email='nicklas_attheold_optimalbpm.se',
        maintainer='Nicklas Boerjesson',
        maintainer_email='nicklas_attheold_optimalbpm.se',
        long_description="""Query Abstraction Layer is a Python library for mixing data from different data sources into SQL statements.\
          It supports several database backends and file formats.
          """,
        url='https://sourceforge.net/projects/qal/',
        packages=['qal', 'qal.dal', 'qal.dal.tests', 'qal.sql', 'qal.sql.tests', 'qal.sql.tests.resources', 
                  'qal.common', 'qal.dataset', 'qal.tools', 'qal.tools.tests'],
        package_data = {
            # If any package contains *.txt or *.xml files, include them:
            '': ['*.txt', '*.xml', '*.sql']
        },
        license='BSD')
