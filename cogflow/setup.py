#!/usr/bin/env python
import setuptools

requirements = ['apache-airflow']

setuptools.setup(
    name='cogflow',
    version='1.0.0',
    description='Airflow operators for the Cognite Open Industrial Data API.',
    author='Denis Gontcharov',
    author_email='denis@gontcharov.dev',

    install_requires=requirements,
    packages=setuptools.find_packages('src'),
    package_dir={'': 'src'},
    url='https://github.com/gontcharovd/cogflow',
    license='MIT license'
)
