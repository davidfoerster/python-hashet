import sys, math, itertools
import struct, pickle
import hashset.util as util
import hashset.util.iter as util_iter
import hashset.util.functional as functional
from functools import partial as fpartial
from .util.math import ceil_div, is_pow2, ceil_pow2


class _vardata_hook:
	def __init__( self, name, doc=None ):
		self.name = '_' + name
		self.__doc__ = doc
		self.fset = functional.attrsetter(self.name)


	def setter( self, fset ):
		self.fset = fset
		if self.__doc__ is None:
			doc = getattr(fset, '__doc__', None)
			if isinstance(doc, str):
				self.__doc__ = doc
		return self


	def __get__( self, instance, owner ):
		return self if instance is None else getattr(instance, self.name)


	def __set__( self, instance, value ):
		old_value = self.__get__(instance, None)
		if type(value) is not type(old_value) or value != old_value:
			self.fset(instance, value)
			instance.reevaluate()


class header:
	"""Manages and represents the necessary header data of a stored hash set and provides some ancilliary services."""

	byteorder = sys.byteorder
	_magic = b'hashset '
	_version = 1

	_struct = struct.Struct('=BB 2x I')
	_struct_keys = ('version', 'int_size', 'index_offset')
	_vardata_keys = {'element_count', 'bucket_count', 'hasher', 'pickler'}
	vars().update({ k: _vardata_hook(k) for k in _vardata_keys })

	hasher.__doc__ = """The hasher to use for this hash set. (See 'hashset.build' for a description.)"""

	pickler.__doc__ = """The pickler to use for this hash set. (See 'hashset.build' for a description.)"""


	def __init__( self, hasher, pickler, int_size=0 ):
		"""
		Initializes a header instance with a hasher, a pickler and a size (in
		bytes) used to represent section offsets.
		"""

		self.int_size = int_size
		self.index_offset = None

		self._vardata = None
		self._hasher = hasher
		self._pickler = pickler
		self._element_count = None
		self._bucket_count = None


	@util.property_setter
	def int_size( self, n ):
		"""A size (in bytes) used to represent section offsets."""
		if not (0 <= n <= 128 and is_pow2(n)):
			raise ValueError(
			'int_size must be a power of 2 between 0 and 128, not {:d}'.format(n))

		self._int_size = n


	def reevaluate( self ):
		"""Resets cached derived attributes in case their source changed."""
		self._vardata = None


	def vardata( self, force=False ):
		"""Returns the “variable” part of the header data.

		The variable header part contains the bulk of its data.
		"""

		if force or self._vardata is None:
			self_getattr = fpartial(getattr, self)

			if any(map(
				functional.comp(functional.is_none, self_getattr), self._vardata_keys)
			):
				raise RuntimeError(
					'One or more of \'{}\' were never assigned'
						.format('\', \''.join(self._vardata_keys)))

			self._vardata = pickle.dumps(dict(map(
				functional.project_out(functional.identity, self_getattr),
				self._vardata_keys)))

		return self._vardata


	def hash( self, obj ):
		return self.hasher(obj, self.pickler.dump_single)


	def value_offset( self ):
		"""Returns the offset of the content section of the buffer prefixed by this header."""
		return self.index_offset + self.bucket_count * self.int_size


	def int_to_bytes( self, n ):
		"""Convert an integer to its byte representation based on the parameters int his header."""
		return n.to_bytes(self.int_size, self.byteorder)


	def run_estimates( self, items ):
		"""Estimate the optimal parameters for a hash set based on the given items."""
		est = getattr(self.pickler, 'run_estimates', None)
		if est is not None: est(items)


	@classmethod
	def get_magic( cls ):
		if cls.byteorder == 'little':
			return cls._magic
		if cls.byteorder == 'big':
			return cls._magic[::-1]
		raise RuntimeError('Unknown byte order: {!r}'.format(cls.byteorder))


	def calculate_sizes( self, buckets=None, force=False ):
		"""Performs some internal calculations before writing this header to a buffer.

		If given a list of buckets, some paramters may be set to more suitable
		values toa void later issues.
		"""

		# Calculate int_size
		if self.int_size <= 0 and buckets is not None:
			max_int = sum(map(len, buckets))
			self.int_size = ceil_pow2(ceil_div(max_int.bit_length(), 8))
			assert 0 <= self.int_size <= 0xFF

		# Calculate index offset
		self.index_offset = util.pad_multiple_of(
			len(self._magic) + self._struct.size + len(self.vardata(force)),
			self.int_size)


	def to_bytes( self, buf=None, buckets=None ):
		"""Writes this header to a newly created or the given buffer and returns it.

		'buckets' is handed to 'calculate_sizes' if given.
		"""

		self.calculate_sizes(buckets)

		if buf is None:
			buf = bytearray(self.index_offset)

		magic = self.get_magic()
		buf[:len(magic)] = magic
		self._struct.pack_into(buf, len(magic),
			self._version, self.int_size, self.index_offset)

		vardata = self.vardata()
		vardata_offset = len(magic) + self._struct.size
		buf[vardata_offset : vardata_offset + len(vardata)] = vardata

		return buf


	@classmethod
	def from_bytes( cls, b ):
		"""Constructs a new header instance and initializes its paramaters based on the data encoded into a buffer."""

		expected_magic = cls.get_magic()
		magic = bytes(b[:len(expected_magic)])
		if magic != expected_magic:
			raise ValueError(
				'Unknown magic {!r}, expected {!r}'.format(magic, expected_magic))

		s = cls._struct.unpack_from(b, len(magic))
		assert len(s) == len(cls._struct_keys)
		s = dict(zip(cls._struct_keys, s))

		version = s.pop('version')
		if version != cls._version:
			raise ValueError(
				'Unsupported version {:d}, expected {:d}'.format(
					version, cls._version))

		var = pickle.loads(
			b[ len(magic) + cls._struct.size : s['index_offset'] ])
		if cls._vardata_keys != var.keys():
			raise ValueError('Header field mismatch: {}'
				.format(', '.join(cls._vardata_keys ^ var.keys())))

		h = cls(None, None)
		util_iter.stareach(fpartial(setattr, h),
			itertools.chain(s.items(), var.items()))
		return h
