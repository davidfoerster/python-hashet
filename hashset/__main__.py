#!/usr/bin/env python3
import sys, os
import itertools, functools, collections
import argparse, codecs
import hashset
from .hashers import hashlib_proxy, pyhash_proxy, default_hasher
from .picklers import string_pickler, pickle_proxy
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


def build( in_path, out_path ):
	from .picklers import string_pickler
	with _open(in_path) as f_in, _open(out_path, 'wb') as f_out:
		hashset.hashset.build(
			map(methodcaller(str.rstrip, '\n'), f_in), f_out,
			pickler=string_pickler())
	return 0


def dump( in_path ):
	with hashset.hashset(in_path) as _set:
		for item in _set:
			print(item)
	return 0


def probe( in_path, *needles ):
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




def make_argparse():
	ap = argparse.ArgumentParser(description=hashset.__doc__, add_help=False)

	actions = (ap
		.add_argument_group('Actions',
			'Available modes of operations. Select exactly one of these!')
		.add_mutually_exclusive_group(required=True))
	actions.add_argument('-b', '--build',
		nargs=2, metavar=('ITEM-FILE', 'HASHSET-FILE'),
		help='Build a new hash set from the lines of a file. '
			'The special value \'-\' as a stand-in for standard input and output '
			'respectively.')
	actions.add_argument('-d', '--dump',
		nargs=1, metavar='HASHSET-FILE',
		help='Write out all items from a given hash set.')
	actions.add_argument('-p', '--probe',
		nargs='+', metavar=('HASHSET-FILE', 'ITEM'),
		help='Probe the existence of a list of items in a hash set. '
			'The item list is either the list of positional command-line arguments '
			'or, in their absence, read from standard input one item per line.')

	opt = ap.add_argument_group('Optional Arguments')
	opt.add_argument('-h', '--help', action='help',
		help='Show this help message and exit.')

	return ap


def main( args ):
	args = make_argparse().parse_args(args)
	action = ('build', 'dump', 'probe')
	action = tuple(filter(lambda a: getattr(args, a) is not None, action))
	assert len(action) == 1
	action = action[0]

	rv = globals()[action](*getattr(args, action))
	if rv != 0:
		sys.exit(rv)


main(sys.argv[1:])
