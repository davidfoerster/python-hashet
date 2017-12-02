import hashlib
from .header import header


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
			data = pickler(data)

		_hash = self.hash_ctor()
		_hash.update(data)
		return int.from_bytes(_hash.digest(), header.byteorder)


	def __getstate__( self ):
		return (self.name,)


	def __setstate__( self, state ):
		self.__init__(*state)
