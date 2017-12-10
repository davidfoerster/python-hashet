#!/bin/sh
# Polyglot invocation of the default Python 3 interpreter with -OO flags
# (source: https://stackoverflow.com/a/9051580/2461638).
''''exec python3 -OO -- "$0" "$@" # '''

import runpy
from os.path import basename, splitext

runpy.run_module(splitext(basename(__file__))[0])
