
#
# Copyright 2015 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

from distutils.core import setup

setup(
    name="hatrac",
    description="simple object storage service",
    version="0.1-prerelease",
    packages=["hatrac"],
    scripts=["bin/hatrac-deploy"],
    requires=["web", "psycopg2", "webauthn2"],
    maintainer_email="support@misd.isi.edu",
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ])
