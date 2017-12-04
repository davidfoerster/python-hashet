import hashlib
from .header import header


class hashlib_proxy:
	"""Wraps the hash algorithms of 'hashlib' for use with 'hashset.build'."""

	algorithms_guaranteed = hashlib.algorithms_guaranteed
	algorithms_available = hashlib.algorithms_available


	def __init__( self, hash_name ):
		"""Wraps the named 'hashlib' algorithm."""
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
			data = pickler(data)

		_hash = self.hash_ctor()
		_hash.update(data)
		return int.from_bytes(_hash.digest(), header.byteorder)


	def __getstate__( self ):
		return (self.name,)


	def __setstate__( self, state ):
		self.__init__(*state)


class pyhash_proxy:
	"""Wraps the hash algorithms of 'pyhash' for use with 'hashset.build'."""

	accepted_types = (bytes, str)
	algorithms_available = frozenset()
	algorithms_preferred = (
		'xx_64', 'murmur3_x64_128', 'murmur2_x64_64a' )


	def __init__( self, hash_name ):
		"""Wraps the named 'pyhash' algorithm."""
		self.name = hash_name
		self.hasher = getattr(pyhash, hash_name)()


	def __call__( self, data, pickler=None ):
		if pickler is not None and not isinstance(data, self.accepted_types):
			data = pickler(data)

		return self.hasher(data)


	def __getstate__( self ):
		return (self.name,)


	def __setstate__( self, state ):
		self.__init__(*state)


try:
	import pyhash
	pyhash_proxy.algorithms_available = frozenset(
		k for k in dir(pyhash)
		if not k.startswith('_') and
			not k.startswith('Test') and
			isinstance(getattr(pyhash, k), type)
	)
	default_hasher = pyhash_proxy(next(iter(filter(
		pyhash_proxy.algorithms_available.__contains__,
		pyhash_proxy.algorithms_preferred))))

except (ImportError, StopIteration):
	default_hasher = hashlib_proxy('md5')
