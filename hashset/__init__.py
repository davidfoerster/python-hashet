"""Build, read, and probe hashets to/from files."""

import sys, os, mmap
import math, itertools, collections.abc
import pickle
import hashset.util, hashset.util.iter
import hashset.util.functional as functional
from .header import header as hashset_header
from .picklers import pickle_proxy, PickleError
from .hashers import default_hasher
from .util.math import is_pow2, ceil_pow2


class hashset:
	"""Manages previously constructed hash sets that are stored in a buffer.

	Such a buff is typically backed by a memory-mapped file.
	"""

	_default_header_args = dict(
		hasher=default_hasher, pickler=pickle_proxy(pickle))


	def __init__( self, _from=None, load_factor=2/3 ):
		"""Initialize a new hashset instance.

		If '_from' is a buffer the hash set is built based on its content.
		The buffer may be a path name or a file descriptor that serves as a
		reference to construct a memory-mapping of the referenced file.

		If '_from' is a mapping instance its 'hasher' and 'pickler' entries are
		used to construct an empty hash set in-memory. The other mapping entries
		are forwarded to the header constructor.

		If '_from' is None (the default) the constructor chooses suitable default
		values to build a new in-memory hash set.
		"""

		self.load_factor = load_factor

		if _from is None or isinstance(_from, collections.abc.Mapping):
			kwargs = self._default_header_args.copy()
			if _from is not None:
				kwargs.update(_from)
			pargs = (kwargs.pop('hasher'), kwargs.pop('pickler'))
			self._header = hashset_header(*pargs, **kwargs)
			self._buckets = []
			self._size = 0
			self._buckets_complete = True
			self._hash_mask = 0

			self._mmap = None
			self.buf = None
			self._value_offset = None
			self.buckets_idx = None

		else:
			if isinstance(_from, str):
				fd = os.open(_from, os.O_RDONLY | getattr(os, 'O_BINARY', 0))
				try:
					_from = self._open_mmap(fd)
				finally:
					os.close(fd)
			if isinstance(_from, int):
				if 0 <= _from < 1<<31:
					_from = self._open_mmap(_from)
				else:
					raise ValueError('Invalid file descriptor: {:d}'.format(_from))

			self._mmap = _from if isinstance(_from, mmap.mmap) else None
			self.buf = _from if isinstance(_from, memoryview) else memoryview(_from)
			self._header = hashset_header.from_bytes(self.buf)
			self._size = self._header.element_count
			self._buckets = [None] * self._header.bucket_count
			self._hash_mask = self._to_hash_mask(len(self._buckets))
			self._buckets_complete = False
			self._value_offset = self._header.value_offset()
			self.buckets_idx = (
				self.buf[self._header.index_offset : self._value_offset]
					.cast('BHILQ'[self._header.int_size.bit_length() - 1]))


	@staticmethod
	def _open_mmap( fd ):
		return mmap.mmap(fd, 0, access=mmap.ACCESS_READ)


	@staticmethod
	def _to_hash_mask( bucket_count ):
		if bucket_count >= 0 and is_pow2(bucket_count):
			return bucket_count - 1
		else:
			raise ValueError('Illegal bucket_count: {:d}'.format(bucket_count))


	def __len__( self ):
		return self._size

	def __bool__( self ):
		return bool(self._size)


	def __iter__( self ):
		"""Returns an iterator over the entries of this hash set."""
		for b in map(self.get_bucket, range(len(self._buckets))):
			yield from b
		self._buckets_complete = True


	def __contains__( self, obj ):
		"""Tests if this hash set contains the given object."""
		return obj in self.get_bucket_for(obj)


	def get_bucket( self, n ):
		"""Returns the bucket at a given index.

		A bucket is either a sequence or a set of entries.
		"""

		bucket = self._buckets[n]
		if bucket is None:
			if self.buf is None:
				bucket = []
			else:
				offset = self.buckets_idx[n]
				length = (
					util.getitem(self.buckets_idx, n + 1,
						len(self.buf) - self._value_offset) - offset)
				if length > 0:
					bucket = (
						self.header.pickler.load_bucket(
							self.buf, self._value_offset + offset, length))
				else:
					assert length == 0
					bucket = ()

			self._buckets[n] = bucket

		return bucket


	def get_bucket_for( self, obj ):
		"""Returns the bucket for the given object."""
		return self.get_bucket(self.get_bucket_idx_for(obj))


	def get_bucket_idx_for( self, obj ):
		"""Returns the bucket index for the given object."""
		return self.header.hash(obj) & self._hash_mask


	@property
	def buckets( self ):
		"""The list of buckets backing this hash set."""
		if not self._buckets_complete:
			util.iter.each(self.get_bucket, range(self.header.bucket_count))
			self._buckets_complete = True
		return self._buckets


	def add( self, obj ):
		self.reserve(self._size + 1)
		return self._add_impl(obj)


	def _add_impl( self, obj ):
		bucket = self.get_bucket_for(obj)
		if obj in bucket:
			return False
		else:
			bucket.append(obj)
			self._size += 1
			return True


	def discard( self, obj ):
		bucket = self.get_bucket_for(obj)
		try:
			bucket.remove(obj)
			self._size -= 1
			return True
		except ValueError:
			return False


	def remove( self, obj ):
		discarded = self.discard(obj)
		if not discarded:
			raise KeyError(obj)


	def pop( self ):
		if self._size:
			self._size -= 1
			return next(iter(filter(bool,
				map(self.get_bucket, range(self.header.bucket_count))))).pop()
		else:
			raise KeyError('empty set')


	def update( self, *iterable ):
		if not iterable:
			return
		iterable = (
			iterable[0] if len(iterable) == 1 else itertools.chain(*iterable))

		iterable_len = util.getlength(iterable)
		if iterable_len is None:
			for item in iterable:
				self.add(item)
		else:
			self.reserve(self._size + iterable_len)
			util.iter.each(self._add_impl, iterable)


	def reserve( self, size=None, load_factor=None ):
		"""Re-allocates an amount of buckets suitable for the given size and load factor.

		If necessary the entire hash set is re-hashed to accomodate the new size.

		The hash set will use the given load factor from hereon. If none is given,
		the current load factor is used instead.

		If no size is given, the current hash set site is used. This allows one to
		change the load factor only.
		"""

		if size is None:
			size = self._size
		else:
			assert size >= 0
		if load_factor is not None:
			assert load_factor > 0
			self.load_factor = load_factor

		required_buckets = math.ceil(max(size, 0) / self.load_factor)
		if required_buckets > len(self._buckets):
			self._rehash(required_buckets)


	def _rehash( self, bucket_count, force=False ):
		if bucket_count > 0:
			bucket_count = ceil_pow2(bucket_count)
		elif bucket_count < 0:
			raise ValueError('Negative bucket count')
		elif self._size:
			raise ValueError('Zero bucket count for non-empty element set')
		if not force and bucket_count == self.header.bucket_count:
			return

		hash_mask = self._to_hash_mask(bucket_count)
		buckets = [None] * bucket_count
		for item in self:
			i = self.header.hash(item) & hash_mask
			bucket = buckets[i]
			if bucket is None:
				bucket = []
				buckets[i] = bucket
			bucket.append(item)

		self.release()
		self._mmap = None
		self.buf = None
		self.value_offset = None
		self.buckets_idx = None
		self._buckets = buckets
		self._hash_mask = hash_mask


	@property
	def header( self ):
		"""Returns the header object used to build the file header for this hash set."""
		self._header.element_count = self._size
		self._header.bucket_count = len(self._buckets)
		return self._header


	def release( self ):
		"""Releases the resources associated with this hash set, e. g. a memory-mapping.

		hashset implements the resource manager interface which calls this method
		upon exit.
		"""

		util.iter.each(memoryview.release,
			filter(functional.instance_tester(memoryview), itertools.chain(
				itertools.chain.from_iterable(filter(bool, self._buckets)),
				(self.buckets_idx, self.buf))))
		if self._mmap is not None:
			self._mmap.close()


	def __enter__( self ):
		return self

	def __exit__( self, exc_type, exc, traceback ):
		self.release()
		return False


	def to_file( self, file ):
		"""Writes this hash set to a file or buffer-like object

		in a way that allows later retrieval from the same buffer through the
		class constructor."""

		self.header.run_estimates(self)

		while True:
			try:
				buckets = list(util.iter.iconditional(
					self.buckets, bool, self.header.pickler.dump_bucket, b''))
				break
			except PickleError as err:
				if err.can_resume:
					header.reevaluate()
				else:
					raise err

		file.write(self.header.to_bytes(None, buckets))
		if buckets:
			util.iter.each(file.write, itertools.chain(
				map(self.header.int_to_bytes,
					util.iter.accumulate(map(len, util.iter.islice(buckets, -1)), 0)),
				buckets))
