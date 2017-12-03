import sys
from .header import header


def _slice( buf, offset=0, length=None ):
	end = None if length is None else offset + length
	if offset or (end is not None and end != len(buf)):
		buf = buf[offset:end]
	return buf


class bytes_pickler:
	# TODO: Scale int_size automatically
	def __init__( self, list_ctor=list, int_size=4, byteorder=header.byteorder ):
		self.list_ctor = list_ctor
		self.int_size = int_size
		self.byteorder = byteorder


	def dump_single( self, obj ):
		return len(obj).to_bytes(self.int_size, self.byteorder) + obj

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


	def _get_length( self, buf, offset=0 ):
		return int.from_bytes(_slice(buf, offset, self.int_size), self.byteorder)


#####################################################################

class string_pickler(bytes_pickler):
	def __init__( self, encoding='utf-8', *args, **kwargs ):
		super().__init__(*args, **kwargs)
		self.encoding = encoding

	def dump_single( self, obj ):
		return super().dump_single(obj.encode(self.encoding))

	def load_single_convert( self, buf, offset, length=None ):
		return str(super().load_single_convert(buf, offset, length), self.encoding)


#####################################################################

class pickle_proxy:
	def __init__( self, *args ):
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
