#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-asana",
    version="2.1.2",
    description="Singer.io tap for extracting Asana data",
    author="Stitch",
    url="http://github.com/singer-io/tap-asana",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_asana"],
    install_requires=["asana==0.10.2", "singer-python==5.12.2"],
    extras_require={"test": ["pylint", "requests==2.20.0", "nose"], "dev": ["ipdb"]},
    entry_points="""
    [console_scripts]
    tap-asana=tap_asana:main
    """,
    packages=["tap_asana"],
    package_data={"schemas": ["tap_asana/schemas/*.json"]},
    include_package_data=True,
)
