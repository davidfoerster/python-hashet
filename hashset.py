import sys, os, math
import itertools, functools
import collections, collections.abc
import struct, pickle, hashlib, mmap


class pickle_proxy:
	def __init__( self, *args ):
		if len(args) == 1:
			p = args[0]
			self.dump = p.dumps
			self.load = p.loads
		else:
			self.dump, self.load = args


class hashlib_proxy:
	def __init__( self, hash_name ):
		self.name = hash_name
		self.hash_ctor = self._get_ctor(hash_name)


	@staticmethod
	def _get_ctor( hash_name ):
		if hash_name in hashlib.algorithms_guaranteed:
			return getattr(hashlib, hash_name)
		else:
			return functools.partial(hashlib.new, hash_name)


	def __call__( self, data, pickler=None ):
		if pickler is not None:
			data = pickler.dump(data)

		_hash = self.hash_ctor()
		_hash.update(data)
		return int.from_bytes(_hash.digest(), sys.byteorder)


	def __getstate__( self ):
		return (self.name,)


	def __setstate__( self, state ):
		self.__init__(*state)


class hashset:
	def __init__( self, buf ):
		if isinstance(buf, str):
			fd = os.open(buf, os.O_RDONLY | getattr(os, 'O_BINARY', 0))
			try:
				buf = self._open_mmap(fd)
			finally:
				os.close(fd)
		if isinstance(buf, int):
			if 0 <= buf < 1<<31:
				buf = self._open_mmap(buf)
			else:
				raise ValueError('Invalid file descriptor: {:d}'.format(buf))

		self._mmap = buf if isinstance(buf, mmap.mmap) else None
		self.buf = memoryview(buf)
		self.header = _header.from_bytes(self.buf)
		self.buckets = []
		self.buckets_data = self.buf[self.header.value_offset():]
		with self.buf[self.header.index_offset:self.header.value_offset()] as buckets_idx_raw:
			self.buckets_idx = (
				buckets_idx_raw.cast('BHILQ'[self.header.int_size.bit_length() - 1]))


	def __iter__( self ):
		return itertools.chain.from_iterable(
			map(self.get_bucket, range(self.header.bucket_count)))


	def __contains__( self, obj ):
		return obj in self.get_bucket(self.header.get_bucket(obj))


	def get_bucket( self, n ):
		if n >= len(self.buckets):
			self.buckets.extend(itertools.repeat(None, n + 1))

		bucket = self.buckets[n]
		if bucket is None:
			bucket = self._get_bucket2(self.buckets_idx[n],
				_get_default(self.buckets_idx, n + 1, len(self.buckets_data)))
			self.buckets[n] = bucket

		return bucket


	def _get_bucket2( self, start, stop ):
		if start >= stop:
			assert start == stop
			return ()
		with self.buckets_data[start:stop] as bucket_data:
			return self.header.pickler.load(bucket_data)


	@staticmethod
	def _open_mmap( fd ):
		return mmap.mmap(fd, 0, access=mmap.ACCESS_READ)


	def release( self ):
		for v in vars(self).values():
			if isinstance(v, memoryview):
				v.release()
		if self._mmap is not None:
			self._mmap.close()


	def __enter__( self ):
		return self

	def __exit__( self, exc_type, exc, traceback ):
		self.release()
		return False


	@staticmethod
	def build( iterable, file=None, hasher=hashlib_proxy('md5'),
		pickler=pickle_proxy(pickle), load_factor=2/3
	):
		if isinstance(iterable, collections.abc.Set):
			_set = iterable
		else:
			_set = frozenset(iterable)
		del iterable
		#print(*_set, sep='\n', end='\n\n')

		header = _header(hasher, pickler, 1)
		header.set_element_count(len(_set), load_factor)
		#print(header.element_count, header.bucket_count)

		buckets = [()] * header.bucket_count
		for obj in _set:
			i = header.get_bucket(obj)
			#print(obj, '=>', i)
			bucket = buckets[i] or []
			if not bucket:
				buckets[i] = bucket
			bucket.append(obj)
		del _set
		#print(*buckets, sep='\n', end='\n\n')

		buckets = [pickler.dump(b) if b else b'' for b in buckets]
		#print(*buckets, sep='\n', end='\n\n')
		#header.calculate_sizes(buckets); print(*('{}={:#x}'.format(k, getattr(header, k)) for k in header._struct_keys if hasattr(header, k)), sep=', ', file=sys.stderr)
		header.to_file(file, buckets)


class _header:
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
		if n < 0 or not _is_pow2(n):
			raise ValueError(
				'Bucket count must be non-negative a power of 2, not {:d}'.format(n))

		self.bucket_count = n
		self._bucket_mask = max(n - 1, 0)


	def get_bucket( self, _bytes ):
		return self.hasher(_bytes, self.pickler) & self._bucket_mask


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

		if buckets is None:
			self.int_size = _ceil_pow2(max(self.int_size, 1))
		else:
			# Calculate int_size
			buckets_length = sum(map(len, buckets))
			_max = max(self.value_offset(), buckets_length)
			while _max.bit_length() > self.int_size * 8:
				self.int_size = _ceil_pow2((_max.bit_length() + 7) // 8)
				_max = max(self.value_offset(), buckets_length)


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
			_each(file.write, map(self.int_to_bytes,
				_saccumulate(0, map(len, buckets), slice(len(buckets) - 1))))
			_each(file.write, buckets)


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
		if not (0 < int_size <= 8 and _is_pow2(int_size)):
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


def _saccumulate( start, iterable, _slice=None ):
	if _slice is not None:
		iterable = itertools.islice(
			iterable, _slice.start, _slice.stop, _slice.step)

	return itertools.accumulate(itertools.chain((start,), iterable))


def _each( func, iterable ):
	for item in iterable:
		func(item)


def _is_pow2( n ):
	return not n or not (n & (n - 1))


def _ceil_pow2( n ):
	return n if _is_pow2(n) else 1 << n.bit_length()


def _get_default( seq, idx, default=None ):
	return seq[idx] if 0 <= idx < len(seq) else default


def _iskip( iterable, skip ):
	it = iter(iterable)
	try:
		for ignored in range(skip):
			next(it)
	except StopIteration:
		pass
	return it


def _ichain( iterable, *suffix ):
	return itertools.chain(iterable, suffix)
