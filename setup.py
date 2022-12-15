#!/usr/bin/env python
# coding: utf-8
from setuptools import setup, find_packages


setup(
    name="athena_timeseries",
    version="1.0.0",
    description="AWS Athena helper for time series operation",
    license="MIT",
    author="yoshiso",
    author_email="nya060@gmail.com",
    url="https://github.com/yoshiso/athena-timeseries.git",
    keywords="",
    packages=find_packages(exclude=("tests")),
    python_requires=">=3.7",
    install_requires=["pandas", "numpy", "awswrangler"],
    tests_require=[],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
