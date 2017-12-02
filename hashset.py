#!/usr/bin/env python3
import runpy
from os.path import basename, splitext

runpy.run_module(splitext(basename(__file__))[0])
