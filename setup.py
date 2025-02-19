
#
# Copyright 2015-2023 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from setuptools import setup

setup(
    name="hatrac",
    description="simple object storage service",
    version="1.2",
    packages=["hatrac", "hatrac.model", "hatrac.model.directory", "hatrac.model.storage", "hatrac.rest"],
    package_data={'hatrac': ["*.wsgi"]},
    scripts=["bin/hatrac-deploy", "bin/hatrac-migrate", "bin/hatrac-utils"],
    install_requires=[
        "flask>=2.3.3",
        "psycopg2",
        "webauthn2",
        "boto3<=1.34.157",
        "botocore<=1.34.157",
    ],
    maintainer_email="isrd-support@isi.edu",
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ])
