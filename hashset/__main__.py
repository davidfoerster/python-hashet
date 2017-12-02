#!/usr/bin/env python3
import sys, os
from . import hashset

import pickle
pickle.DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL


def _open(path, mode='r'):
	if path == '-':
		if mode == 'r':
			return sys.stdin
		if mode == 'w':
			return sys.stdout

		ValueError(
			'Cannot use the special path {!r} with file mode {!r}. '
			'Please use \'/dev/std*\' etc. instead.'
				.format(path, mode))

	return open(path, mode)


args = sys.argv[1:]
if len(args) == 3 and args[0] == '--build':
	from .picklers import string_pickler
	with _open(args[1]) as f_in, open(args[2], 'wb') as f_out:
		hashset.build(
			(line.rstrip('\n') for line in f_in), f_out, pickler=string_pickler())

elif len(args) == 2 and args[0] == '--read':
	with hashset(args[1]) as _set:
		for item in _set:
			print(item)

elif len(args) >= 2 and args[0] == '--test':
	with hashset(args[1]) as _set:
		item = None
		for item in filter(_set.__contains__, args[2:]):
			print(item)
		if item is None:
			sys.exit(1)

else:
	# TODO
	raise Exception('Usage error')
