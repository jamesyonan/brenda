#!/bin/bash
# build paracurl library to allow for testing during development
set -e
rm -rf build
python setup.py build
cp build/*/paracurl.so .
