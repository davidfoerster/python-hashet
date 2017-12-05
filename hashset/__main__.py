#!/usr/bin/env python3
import sys, os
import itertools, functools, collections, contextlib
import hashset
import hashset.util.io as util_io
import hashset.util.iter as util_iter
from .hashers import hashlib_proxy, pyhash_proxy, default_hasher
from .picklers import codec_pickler, pickle_proxy

import pickle
pickle.DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL


def build( in_path, out_path, **kwargs ):
	with contextlib.ExitStack() as es:
		f_in = es.enter_context(util_io.open(in_path,
			encoding=kwargs.get('external_encoding')))
		f_out = es.enter_context(util_io.open(out_path, 'wb'))

		hashset.hashset.build(
			map(util_io.strip_line_terminator, f_in), f_out,
			pickler=codec_pickler.string_instance())

	return 0


def dump( in_path, **kwargs ):
	with hashset.hashset(in_path) as _set:
		for item in _set:
			print(item)
	return 0


def probe( in_path, *needles, **kwargs ):
	with hashset.hashset(in_path) as _set:
		if needles:
			fneedles = None
		else:
			fneedles = util_io.open('-', encoding=kwargs.get('external_encoding'))
			needles = map(util_io.strip_line_terminator, fneedles)

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
	import argparse, locale
	preferred_encoding = locale.getpreferredencoding()
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
	opt.add_argument('--encoding', '--external-encoding', metavar='CHARSET',
		dest='external_encoding', default=preferred_encoding,
		help='The external encoding when reading or writing text.')
	opt.add_argument('-h', '--help', action='help',
		help='Show this help message and exit.')

	return ap


def main( args ):
	kwargs = vars(make_argparse().parse_args(args))
	#print(args); return

	actions = ['build', 'dump', 'probe']
	action_args = None
	while actions and action_args is None:
		action = actions.pop()
		action_args = kwargs.pop(action)
	util_iter.each(kwargs.__delitem__, actions)
	del actions

	rv = globals()[action](*action_args, **kwargs)
	if rv != 0:
		sys.exit(rv)


main(sys.argv[1:])
