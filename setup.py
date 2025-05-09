#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-asana",
    version="2.3.0",
    description="Singer.io tap for extracting Asana data",
    author="Stitch",
    url="http://github.com/singer-io/tap-asana",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_asana"],
    install_requires=[
        "asana==5.1.0",
        "singer-python==6.1.1"
    ],
    extras_require={
        "test": [
            "pylint",
            "requests==2.32.3",
            'nose'
        ],
        "dev": [
            "ipdb"
        ]
    },
    entry_points="""
    [console_scripts]
    tap-asana=tap_asana:main
    """,
    packages=["tap_asana"],
    package_data={
        "schemas": ["tap_asana/schemas/*.json"]
    },
    include_package_data=True,
)
