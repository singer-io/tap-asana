#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-asana",
    version="2.6.0",
    description="Singer.io tap for extracting Asana data",
    author="Stitch",
    url="http://github.com/singer-io/tap-asana",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_asana"],
    install_requires=[
        "asana==5.2.4",
        "requests==2.33.1",
        "singer-python==6.8.0"
    ],
    extras_require={
        "test": [
            "parameterized",
            "pylint"
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
