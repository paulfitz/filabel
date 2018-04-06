#!/usr/bin/env python

import sys
from setuptools import setup

install_requires = [
    "dataset >= 1.0.6"
]

setup(name="filabel",
      version="0.1.1",
      author="Paul Fitzpatrick",
      author_email="paulfitz@alum.mit.edu",
      description="Label files quickly for image classification",
      packages=['filabel'],
      entry_points={
          "console_scripts": [
              "filabel=filabel.main:main"
          ]
      },
      install_requires=install_requires,
      test_suite="nose.collector",
      url="https://github.com/paulfitz/filabel"
)
