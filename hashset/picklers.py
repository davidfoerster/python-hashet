import sys
import locale, codecs
from .header import header
from .util.math import ceil_div


def _slice( buf, offset=0, length=None ):
	end = None if length is None else offset + length
	if offset or (end is not None and end != len(buf)):
		buf = buf[offset:end]
	return buf


class PickleError(RuntimeError):
	def __init__( self, msg=None, cause=None, can_resume=False ):
		super().__init__(*((msg,) if msg else (cause.args if cause else ())))
		self.cause = cause
		self.can_resume = can_resume


class bytes_pickler:
	"""Encodes and decodes byte sequences…

	as well as sequences (“buckets”) of such sequences for use with
	'hashset.build'.
	"""

	def __init__( self, list_ctor=list, int_size=1, byteorder=header.byteorder,
		**kwargs
	):
		"""Initializes a new instance …

		with 'list_ctor' the constructor to build new buckets when decoding,
		'int_size' and 'byteorder' the size in bytes and byte order of integers
		used to encode the length of byte sequences as accepted by 'int.to_bytes'.
		"""
		self.list_ctor = list_ctor
		self.int_size = int_size
		self.byteorder = byteorder


	def dump_single( self, obj ):
		obj = self.dump_single_convert(obj)
		return self._to_bytes(len(obj)) + obj

	def dump_single_convert( self, obj ):
		return obj


	def dump_bucket( self, obj ):
		return b''.join(map(self.dump_single, obj))


	def load_single( self, buf, offset=0 ):
		return self.load_single_convert(
			buf, offset + self.int_size, self._get_length(buf, offset))

	def load_single_convert( self, buf, offset, length=None ):
		return _slice(buf, offset, length)


	def load_bucket( self, buf, offset=0, length=None ):
		return self.list_ctor(self._load_list_gen(buf, offset, length))

	def _load_list_gen( self, buf, offset, length=None ):
		end = len(buf) if length is None else offset + length
		while offset < end:
			length = self._get_length(buf, offset)
			offset += self.int_size
			yield self.load_single_convert(buf, offset, length)
			offset += length


	def run_estimates( self, items ):
		longest = max(items, key=len, default=None)
		if longest is not None:
			self.int_size = max(
				self.get_int_size_for_val(len(self.dump_single_convert(longest))),
				1)


	def _get_length( self, buf, offset=0 ):
		return int.from_bytes(_slice(buf, offset, self.int_size), self.byteorder)


	def _to_bytes( self, n ):
		try:
			return n.to_bytes(self.int_size, self.byteorder)
		except OverflowError as err:
			err.value = n
			err = PickleError(
				'{:d} is too big to be represented in {:d} bytes'
					.format(n, self.int_size),
				err, True)
			self.int_size = self.get_int_size_for_val(n)
			raise err


	@staticmethod
	def get_int_size_for_val( n ):
		return ceil_div(n.bit_length(), 8)


#####################################################################

class codec_pickler(bytes_pickler):
	"""Like its parent, but encodes objects to byte sequences using a codec."""

	def __init__( self, codec, *args, **kwargs ):
		"""Initializes a new instance with a codec name or a CodecInfo instance…

		(from the codecs module). Other arguments are forwarded are forwarded
		to the parent constructor.
		"""

		super().__init__(*args, **kwargs)

		if isinstance(codec, str):
			codec = codecs.lookup(codec)
		self._encode = codec.encode
		self._decode = codec.decode


	def dump_single_convert( self, obj ):
		return self._encode(obj)[0]


	def load_single_convert( self, buf, offset, length=None ):
		return self._decode(super().load_single_convert(buf, offset, length))[0]


	@classmethod
	def string_instance( cls, codec=None, *args, **kwargs ):
		"""Like the initializer but with a suitable default codec name…

		taken from locale.getpreferredencoding."""

		if codec is None:
			codec = locale.getpreferredencoding(False)

		return cls(codec, *args, **kwargs)


#####################################################################

class pickle_proxy:
	""" Wraps two callables, one that “dumps” objects to and one that loads them from byte sequences, for use with 'hashset.build'."""


	def __init__( self, *args, **kwargs ):
		"""Requires either one or two arguments.

		With two arguments, use the first as a callable to “dump“ objects to and
		the second to load them from byte sequences.

		With one arguments use its attributes 'dumps' and 'loads' instead. This is
		convenient to take such callables from a single object or module, e. g. the
		'pickle' module.
		"""

		if len(args) == 1:
			p = args[0]
			self.dump_single = p.dumps
			self.load_single = p.loads
		else:
			self.dump_single, self.load_single = args


	def dump_bucket( self, obj ):
		return self.dump_single(obj)


	def load_bucket( self, buf, offset=0, length=None ):
		return self.load_single(_slice(buf, offset, length))
