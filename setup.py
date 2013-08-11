#!/usr/bin/env python

'''
Created on Aug 11, 2013

@author: Nicklas Boerjesson
'''


from setuptools import setup

setup(
    name='qal',
    version='0.1',
    description='QAL is a Python library for mixing data sources into SQL statements.',
    author='Nicklas Boerjesson',
    author_email='nicklasb_attheold_gmaildotcom',
    long_description="""\
      Query Abstraction Layer is a Python library for mixing data from different data sources into SQL statements.
      It supports several database backends and file formats.
      """,
    url='https://sourceforge.net/projects/qal/',
    packages=['qal', 'qal.dal', 'qal.dal.tests', 'qal.sql', 'qal.sql.tests', 'qal.common', 'qal.dataset'],
    license='BSD',
    install_requires=['setuptools'])