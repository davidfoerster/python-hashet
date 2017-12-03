import sys, os, mmap
import itertools, collections.abc
import pickle
import hashset.util, hashset.util.iter
from .header import header as hashset_header
from .picklers import pickle_proxy, PickleError
from .hashers import hashlib_proxy


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
		self._extend_buckets
		return itertools.chain.from_iterable(
			map(self.get_bucket, range(self.header.bucket_count)))


	def __contains__( self, obj ):
		return obj in self.get_bucket(self.header.get_bucket_idx(obj))


	def get_bucket( self, n ):
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

		header = hashset_header(hasher, pickler, 1)
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
		header.to_file(file, buckets)
