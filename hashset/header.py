import sys, math
import struct, pickle
import hashset.util as util


class header:
	byteorder = sys.byteorder
	_magic = b'hashset '
	_version = 0xff
	_struct = struct.Struct('=BB 6x QQQ')
	_struct_keys = (
		'version', 'int_size', 'index_offset', 'element_count', 'bucket_count')
	_vardata_keys = ('hasher', 'pickler')

	def __init__( self, hasher, pickler, int_size=8 ):
		self.hasher = hasher
		self.pickler = pickler
		self._vardata = None

		self.int_size = int_size
		self.index_offset = None
		self.element_count = None
		self.bucket_count = None
		self._bucket_mask = None


	def set_element_count( self, n, load_factor=1 ):
		if n > 0:
			self.element_count = n
			bc1 = math.ceil(n / load_factor)
			bc2 = 1 << (bc1.bit_length() - 1)
			bc2 <<= bc1 != bc2
			self._set_bucket_count(bc2)
		else:
			self.element_count = 0
			self._set_bucket_count(0)


	def _set_bucket_count( self, n ):
		if n < 0 or not util.is_pow2(n):
			raise ValueError(
				'Bucket count must be non-negative a power of 2, not {:d}'.format(n))

		self.bucket_count = n
		self._bucket_mask = max(n - 1, 0)


	def get_bucket_idx( self, _bytes ):
		return self.hasher(_bytes, self.pickler.dump_single) & self._bucket_mask


	def value_offset( self ):
		return self.index_offset + self.bucket_count * self.int_size


	def int_to_bytes( self, n ):
		return n.to_bytes(self.int_size, self.byteorder)


	@classmethod
	def get_magic( cls ):
		if cls.byteorder == 'little':
			return cls._magic
		if cls.byteorder == 'big':
			return cls._magic[::-1]
		raise 'Unknown byte order: {!r}'.format(cls.byteorder)


	def calculate_sizes( self, buckets=None, force=False ):
		if force or self._vardata is None:
			# Pickle vardata
			self._vardata = pickle.dumps(
				{ k: getattr(self, k) for k in self._vardata_keys })
			if len(self._vardata) % 8:
				self._vardata += b'\x00' * ((8 - len(self._vardata)) % 8)

		# Calculate index offset
		assert len(self._magic) % 8 == 0
		self.index_offset = (
			len(self._magic) + self._struct.size + len(self._vardata))

		# Calculate int_size
		if buckets is not None:
			max_int = sum(map(len, buckets))
			self.int_size = util.ceil_pow2((max_int.bit_length() + 7) // 8)

		self.int_size = max(self.int_size, 1)


	def to_bytes( self, buf=None, buckets=None ):
		self.calculate_sizes(buckets)

		if buf is None:
			buf = bytearray(self.index_offset)

		magic = self.get_magic()
		buf[:len(magic)] = magic
		self._struct.pack_into(buf, len(magic), self._version, self.int_size,
			self.index_offset, self.element_count, self.bucket_count)
		buf[len(magic) + self._struct.size:] = self._vardata
		return buf


	def to_file( self, file, buckets=None ):
		file.write(self.to_bytes(None, buckets))
		if buckets:
			util.iter.each(file.write, map(self.int_to_bytes,
				util.iter.saccumulate(0, map(len, buckets), slice(len(buckets) - 1))))
			util.iter.each(file.write, buckets)


	@classmethod
	def from_bytes( cls, b ):
		expected_magic = cls.get_magic()
		magic = bytes(b[:len(expected_magic)])
		if magic != expected_magic:
			raise ValueError(
				'Unknown magic {!r}, expected {!r}'.format(magic, expected_magic))

		s = cls._struct.unpack_from(b, len(magic))
		assert len(s) == len(cls._struct_keys)
		s = dict(zip(cls._struct_keys, s))

		if s['version'] != cls._version:
			raise ValueError(
				'Unsupported version {}, expected {}'.format(
					s['version'], cls._version))

		int_size = s['int_size']
		if not (0 < int_size <= 8 and util.is_pow2(int_size)):
			raise ValueError(
				'Integer size must be a power of 2 between 1 and 8, not {:d}'
					.format(int_size))

		vardata_offset = len(magic) + cls._struct.size
		h = cls(**pickle.loads(b[vardata_offset:s['index_offset']]))
		h._set_bucket_count(s.pop('bucket_count'))
		for k, v in s.items():
			if hasattr(h, k):
				setattr(h, k, v)
		return h
