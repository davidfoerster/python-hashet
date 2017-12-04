#!/usr/bin/env python3
import sys, os
from . import hashset

import pickle
pickle.DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL


def _open(path, mode='r'):
	if path != '-':
		return open(path, mode)

	mode = set(mode)
	mode.discard('t')

	if 'r' in mode:
		f_src = (sys, 'stdin')
	elif 'w' in mode or 'a' in mode:
		f_src = (sys, 'stdout')
	else:
		f_src = None

	if f_src is not None:
		f = getattr(*f_src)
		if 'b' in mode and 'b' not in f.mode:
			wrapper = f
			f = f.buffer
		else:
			wrapper = None
		if len(mode) == len(f.mode) and mode.issubset(f.mode):
			if wrapper is not None:
				f = wrapper.detach()
			setattr(*(f_src + (None,)))
			return f

	ValueError(
		'Cannot use the special path {!r} with file mode {!r}. '
		'Please use \'/dev/std*\' etc. instead.'
			.format(path, mode))


args = sys.argv[1:]
if len(args) == 3 and args[0] == '--build':
	from .picklers import string_pickler
	with _open(args[1]) as f_in, _open(args[2], 'wb') as f_out:
		hashset.build(
			(line.rstrip('\n') for line in f_in), f_out, pickler=string_pickler())

elif len(args) == 2 and args[0] == '--read':
	with hashset(args[1]) as _set:
		for item in _set:
			print(item)

elif len(args) >= 2 and args[0] == '--test':
	with hashset(args[1]) as _set:
		needles = args[2:]
		if needles:
			fneedles = None
		else:
			fneedles = sys.stdin
			sys.stdin = None
			needles = (s.rstrip('\n') for s in fneedles)

		item = None
		try:
			for item in filter(_set.__contains__, needles):
				print(item)
		finally:
			if fneedles is not None:
				fneedles.close()

		if item is None:
			sys.exit(1)

else:
	# TODO
	raise Exception('Usage error')
