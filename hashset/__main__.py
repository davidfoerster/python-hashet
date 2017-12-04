#!/usr/bin/env python3
import sys, os
import hashset
from .util.functional import comp, methodcaller

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
			.format(path, ''.join(mode)))


def _build( in_path, out_path ):
	from .picklers import string_pickler
	with _open(in_path) as f_in, _open(out_path, 'wb') as f_out:
		hashset.hashset.build(
			map(methodcaller(str.rstrip, '\n'), f_in), f_out,
			pickler=string_pickler())
	return 0


def _read( in_path ):
	with hashset.hashset(in_path) as _set:
		for item in _set:
			print(item)
	return 0


def _test( in_path, *needles ):
	with hashset.hashset(in_path) as _set:
		if needles:
			fneedles = None
		else:
			fneedles = sys.stdin
			sys.stdin = None
			needles = map(methodcaller(str.rstrip, '\n'), fneedles)

		found_any = True
		try:
			for item in needles:
				if item in _set:
					print(item)
				else:
					found_any = False
		finally:
			if fneedles is not None:
				fneedles.close()

		return int(not found_any)


def _main( args ):
	if len(args) >= 1 and args[0] in ('--build', '--read', '--test'):
		rv = globals()['_' + args[0].lstrip('-')](*args[1:])
		if rv != 0:
			sys.exit(rv)
	else:
		# TODO
		raise RuntimeError('Usage error')


_main(sys.argv[1:])
