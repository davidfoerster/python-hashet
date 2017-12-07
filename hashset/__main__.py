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
			kwargs['hash'].get_instance(),
			kwargs['pickler'].get_instance(
				codec=kwargs['internal_encoding'], int_size=kwargs['item_int_size']),
			kwargs['load_factor'], int_size=kwargs['index_int_size'])

	return 0


def dump( in_path, **kwargs ):
	with contextlib.ExitStack() as es:
		util_iter.each(
			fpartial(print, file=es.enter_context(
				util_io.open_stdstream('stdout', kwargs['external_encoding']))),
			es.enter_context(hashset.hashset(in_path)))

	return 0


def probe( in_path, *needles, quiet=False, **kwargs ):
	with contextlib.ExitStack() as es:
		if not needles:
			needles = map(util_io.strip_line_terminator,
				es.enter_context(util_io.open_stdstream(
					'stdin', encoding=kwargs['external_encoding'])))

		_set = es.enter_context(hashset.hashset(in_path))

		if quiet:
			found_any = any(map(_set.__contains__, needles))
		else:
			f = es.enter_context(
				util_io.open_stdstream('stdout', kwargs['external_encoding']))
			found_any = False
			for item in filter(_set.__contains__, needles):
				found_any = True
				print(item, file=f)

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


class ArgumentChoice(collections.UserString):
	def __init__( self, name, _type ):
		super().__init__(name)
		self.type = _type


	def __eq__( self, other ):
		return self is other


	def get_instance( self, *args, **kwargs ):
		return self.type(*args, **kwargs)


	@classmethod
	def update_choices( cls, value_func, default=None ):
		cls.choices.update(
			(item[0], cls(*value_func(*item))) for item in cls.choices.items())
		if default is not None:
			cls.default = cls.choices[default]


def make_argparse():
	import argparse, locale, codecs
	from .hashers import hashlib_proxy, pyhash_proxy, default_hasher
	from .picklers import codec_pickler, pickle_proxy

	preferred_encoding = locale.getpreferredencoding()
	ap = argparse.ArgumentParser(
		description=hashset.__doc__, add_help=False,
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog='Author and copyright: David Foerster, 2017\n\n'
			'Source code repository and issue tracker:\n'
			'https://github.com/davidfoerster/python-hashset')

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
	opt.add_argument('-q', '--quiet',
		action='store_true', default=False,
		help="Don't print matched items; only report success through the exit "
			'status.')
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
	p.add_argument('--index-int-size',
		type=int, metavar='N', default=0,
		help='The size (in bytes) of the integer, a power of 2, used to store '
			'offsets in the bucket index. This may save some time and memory during '
			'hash set construction. (default: determine optimal value)')
	p.add_argument('--item-int-size',
		type=int, metavar='N', default=0,
		help='The size (in bytes) of the integers used to store the length of the '
			'(encoded) hash set items. (default: determine optimal value)')
	default_load_factor = 0.75
	p.add_argument('--load-factor', metavar='FRACTION',
		type=NamedMethod('float or fraction',
			fpartial(_parse_fraction, verifier=(0).__lt__)),
		default=default_load_factor,
		help='The load factor of the resulting hash set, a positive decimal or '
			'fraction. (default: {:.2f})'
				.format(default_load_factor))


	class PicklerChoice(ArgumentChoice):
		choices = {
			'string': codec_pickler.string_instance,
			'pickle': lambda **kwargs: pickle_proxy(pickle)
		}
	PicklerChoice.update_choices(util.as_tuple, 'string')
	p.add_argument('--pickler',
		type=fpartial(dict.get, PicklerChoice.choices),
		choices=PicklerChoice.choices.values(),
		default=PicklerChoice.default,
		help='''The "pickler" used to encode hash set items; either 'string'
			encoding for strings (default) or the 'pickle' encoding working on a
			wide array of Python objects.''')


	class HashChoice(ArgumentChoice):
		def get_instance( self ):
			return super().get_instance(self.data)

		choices = { a: hashlib_proxy for a in hashlib_proxy.algorithms_available }
		choices.update(
			(a, pyhash_proxy) for a in pyhash_proxy.algorithms_available)
	HashChoice.update_choices(util.as_tuple, default_hasher.name)
	p.add_argument('--hash', metavar='ALGORITHM',
		type=fpartial(dict.get, HashChoice.choices),
		choices=HashChoice.choices.values(), default=HashChoice.default,
		help='The hash algorithm used to assign items to buckets. (default: {})'
			.format(HashChoice.default))

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
