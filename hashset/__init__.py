"""Build, read, and probe hashets to/from files."""

import sys, os, mmap
import itertools, collections.abc
import pickle
import hashset.util, hashset.util.iter
from .header import header as hashset_header
from .picklers import pickle_proxy, PickleError
from .hashers import default_hasher


class hashset:
	"""Manages previously constructed hash sets that are stored in a buffer.

	Such a buff is typically backed by a memory-mapped file.
	"""


	def __init__( self, buf ):
		"""Initialize a new hashset instance with a backing buffer.

		The buffer may be a path name or a file descriptor that serves as a
		reference to construct a memory-mapping of the referenced file.
		"""

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
		self.header = hashset_header.from_bytes(self.buf)
		self.value_offset = self.header.value_offset()
		self.buckets = []
		self.buckets_idx = (
			self.buf[self.header.index_offset : self.value_offset]
				.cast('BHILQ'[self.header.int_size.bit_length() - 1]))


	def _extend_buckets( self, size=None ):
		if size is None:
			size = self.header.bucket_count
		if len(self.buckets) < size:
			self.buckets.extend(
				itertools.repeat(None, size - len(self.buckets)))


	def __iter__( self ):
		"""Returns an iterator over the entries of this hash set."""
		self._extend_buckets
		return itertools.chain.from_iterable(
			map(self.get_bucket, range(self.header.bucket_count)))


	def __contains__( self, obj ):
		"""Tests if this hash set contains the given object."""
		return obj in self.get_bucket(self.header.get_bucket_idx(obj))


	def get_bucket( self, n ):
		"""Returns the bucket at a given index.

		A bucket is either a sequence or a set of entries.
		"""

		self._extend_buckets(n + 1)
		bucket = self.buckets[n]
		if bucket is None:
			offset = self.buckets_idx[n]
			length = (
				util.getitem(self.buckets_idx, n + 1,
					len(self.buf) - self.value_offset) - offset)
			if length > 0:
				bucket = (
					self.header.pickler.load_bucket(
						self.buf, self.value_offset + offset, length))
			else:
				assert length == 0
				bucket = ()

			self.buckets[n] = bucket

		return bucket


	@staticmethod
	def _open_mmap( fd ):
		return mmap.mmap(fd, 0, access=mmap.ACCESS_READ)


	def release( self ):
		"""Releases the resources associated with this hash set, e. g. a memory-mapping.

		hashset implements the resource manager interface which calls this method
		upon exit.
		"""

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
	def build( iterable, file=None, hasher=default_hasher,
		pickler=pickle_proxy(pickle), load_factor=2/3, **header_kwargs
	):
		"""Builds a new hash set based on the items of a given iterable and saves the resulting data set to a buffer, typically backed by a file.

		'hasher' is a callable that accepts an object to hash and optional 'pickler'
		callable to convert the object to a byte sequence consumable by most hash
		algorithm implementations.

		'pickler' is an object with the 4 function interfaces 'dump_single',
		'dump_bucket', 'load_single', and 'load_bucket' that “dump” single objects
		or buckets (i. e. sequences) to or load them from byte sequences.

		'load_factor' is the ratio of buckets to items used in this hash set.
		"""

		if isinstance(iterable, collections.abc.Set):
			_set = iterable
		else:
			_set = frozenset(iterable)
		del iterable

		header = hashset_header(hasher, pickler, **header_kwargs)
		header.set_element_count(len(_set), load_factor)
		header.run_estimates(_set)

		while True:
			buckets = [()] * header.bucket_count
			try:
				for obj in _set:
					i = header.get_bucket_idx(obj)
					bucket = buckets[i] or []
					if not bucket:
						buckets[i] = bucket
					bucket.append(obj)

				break
			except PickleError as err:
				if err.can_resume:
					header.reevaluate()
				else:
					raise err
		del _set

		buckets = list(
			util.iter.iconditional(buckets, bool, pickler.dump_bucket, b''))

		if file is not None:
			header.to_file(file, buckets)
			return file
		else:
			return buckets
