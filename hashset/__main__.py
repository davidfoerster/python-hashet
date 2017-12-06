#!/usr/bin/env python3
import sys, os
import collections
import contextlib
import hashset
import hashset.util as util
import hashset.util.io as util_io
import hashset.util.iter as util_iter
from functools import partial as fpartial


def build( in_path, out_path, **kwargs ):
	with contextlib.ExitStack() as es:
		f_in = es.enter_context(util_io.open(
			in_path, encoding=kwargs['external_encoding']))
		f_out = es.enter_context(util_io.open(out_path, 'wb'))

		hashset.hashset.build(
			map(util_io.strip_line_terminator, f_in), f_out,
			pickler=codec_pickler.string_instance(
				kwargs.get('internal_encoding')),
			load_factor=kwargs['load_factor'])

	return 0


def dump( in_path, **kwargs ):
	with contextlib.ExitStack() as es:
		util_iter.each(
			fpartial(print, file=es.enter_context(
				util_io.open_stdstream('stdout', kwargs['external_encoding']))),
			es.enter_context(hashset.hashset(in_path)))

	return 0


def probe( in_path, *needles, **kwargs ):
	with contextlib.ExitStack() as es:
		if not needles:
			needles = map(util_io.strip_line_terminator,
				es.enter_context(util_io.open(
					'-', encoding=kwargs['external_encoding'])))

		f = es.enter_context(
			util_io.open_stdstream('stdout', kwargs['external_encoding']))
		_set = es.enter_context(hashset.hashset(in_path))

		found_any = True
		for item in needles:
			if item in _set:
				print(item, file=f)
			else:
				found_any = False

	return int(not found_any)


def _parse_fraction( s, verifier=None ):
	split = min(filter((0).__le__, map(s.find, '/÷')), default=-1)
	if split < 0:
		x = float(s)
	else:
		x = float(s[:split]) / float(s[split+1:])
	if verifier is not None and not verifier(x):
		raise ValueError('Illegal value {:f}, derived from {!r}'.format(x, s))
	return x


class NamedMethod(collections.UserString):
	def __init__( self, name, func ):
		super().__init__(name)
		self.func = func

	def __call__( self, *args, **kwargs ):
		return self.func(*args, **kwargs)


def make_argparse():
	import argparse, locale, codecs
	from .hashers import hashlib_proxy, pyhash_proxy, default_hasher
	from .picklers import codec_pickler, pickle_proxy

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
		help='The external encoding when reading or writing text. (default: {})'
			.format(preferred_encoding))
	opt.add_argument('-h', '--help', action='help',
		help='Show this help message and exit.')

	p = ap.add_argument_group('Hashset Parameters',
		'Parameters that influence hash set creation.')
	p.add_argument('--internal-encoding', metavar='CHARSET',
		type=codecs.lookup, default=codecs.lookup(preferred_encoding),
		help='The internal encoding of the entries of the hash set file to build. '
		'(default: {})'.format(preferred_encoding))
	default_load_factor = 0.75
	p.add_argument('--load-factor', metavar='FRACTION',
		type=NamedMethod('float or fraction',
			fpartial(_parse_fraction, verifier=(0).__lt__)),
		default=default_load_factor,
		help='The load factor of the resulting hash set, a positive decimal or '
			'fraction. (default: {:f})'
				.format(default_load_factor))

	return ap


def main( args ):
	import pickle
	pickle.DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

	kwargs = vars(make_argparse().parse_args(args))

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
