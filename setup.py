# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'kafka-scanner',
    version      = '0.3.1',
    description   = 'High Level Kafka Scanner, supporting inverse consuming and deduplication. Based on kafka-python library.',
    keywords = 'kafka',
    author = 'Scrapinghub',
    author_email = 'info@scrapinghub',
    license = 'BSD',
    url = 'https://github.com/scrapinghub/kafka-scanner',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    packages     = find_packages(),
    install_requires = [
        'kafka-python',
        'six',
        'retrying',
        'msgpack-python',
        'sqlitedict'
    ]
)
