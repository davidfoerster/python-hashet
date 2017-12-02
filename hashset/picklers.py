import sys
from .header import header


class bytes_pickler:
	# TODO: Scale int_size automatically
	def __init__( self, list_ctor=list, int_size=4, byteorder=header.byteorder ):
		self.list_ctor = list_ctor
		self.int_size = int_size
		self.byteorder = byteorder


	def dump_single( self, obj ):
		return len(obj).to_bytes(self.int_size, self.byteorder) + obj

	def dump_bucket( self, obj ):
		return (
			len(obj).to_bytes(self.int_size, self.byteorder) +
			b''.join(map(self.dump_single, obj)))


	def load_single( self, buf ):
		return self.load_single_convert(
			buf[self.int_size : self.int_size + self._get_length(buf)])

	def load_single_convert( self, buf ):
		return buf


	def load_bucket( self, buf ):
		return self.list_ctor(self._load_list_gen(buf))

	def _load_list_gen( self, buf ):
		offset = self.int_size
		for i in range(self._get_length(buf)):
			length = self._get_length(buf[offset : offset + self.int_size])
			offset += self.int_size
			yield self.load_single_convert(buf[offset : offset + length])
			offset += length


	def _get_length( self, buf ):
		return int.from_bytes(buf[:self.int_size], self.byteorder)


#####################################################################

class string_pickler(bytes_pickler):
	def __init__( self, encoding='utf-8', *args, **kwargs ):
		super().__init__(*args, **kwargs)
		self.encoding = encoding

	def dump_single( self, obj ):
		return super().dump_single(obj.encode(self.encoding))

	def load_single_convert( self, buf ):
		return str(buf, self.encoding)


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

	def load_bucket( self, buf ):
		return self.load_single(buf)
