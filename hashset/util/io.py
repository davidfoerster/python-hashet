import sys, os, io, codecs
from .functional import comp, project_out


open_flags_map = {
	'r': os.O_RDONLY,
	'w': os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
	'a': os.O_WRONLY | os.O_CREAT | os.O_APPEND,
	'x': os.O_EXCL,
	'b': getattr(os, 'O_BINARY', 0),
	't': getattr(os, 'O_TEXT', 0),
}
open_flags_map.update(map(
	project_out(frozenset, open_flags_map.__getitem__), 'rwa'))
open_flags_map.update(map(
	project_out(
		comp(frozenset, '+'.__add__),
		comp(os.O_RDWR.__or__, (~(os.O_RDONLY | os.O_WRONLY)).__and__,
			open_flags_map.__getitem__)),
	'rwa'))


def open( path, mode='r', buffering=-1, encoding=None, errors=None,
	newline=None
):
	"""Similar to the eponymous built-in method but treats the path '-' specially.

	If '-' is specified as path, use the underlying raw file object of sys.stdin
	or sys.stdout depending on the selected access mode. The raw file object is
	wrapped in a file buffer and text file wrapper according to the other
	options. The original file objects are detached and their entry in the sys
	module set to None.

	Not all combinations of access modes and stdin/stdout are guaranteed to be
	compatible in which case a ValueError is raised and the original value of
	stdin/stdout in the sys module is _not_ restored.
	"""

	smode = set(mode)
	if 'b' not in smode:
		smode.add('t')
		if newline is None:
			newline = os.linesep
	elif 't' in smode:
		raise ValueError(
			"Conflicting options 'b' and 't' in mode {!r}".format(mode))

	if path != '-':
		return io.open(path, mode, buffering, encoding, errors, newline)

	amode = frozenset(smode.intersection('rwa+'))
	if amode not in open_flags_map:
		raise ValueError(
			'Conflicting or missing access mode flags: {!r}'
				.format(''.join(amode)))

	f = None
	if '+' not in amode:
		if 'r' in amode:
			f = sys.stdin
			sys.stdin = None
			buftype = io.BufferedReader
		elif 'w' in amode:
			f = sys.stdout
			sys.stdout = None
			buftype = io.BufferedWriter
	if f is None:
		ValueError(
			'Cannot use the special path {!r} with file mode {!r}. '
			'Please use \'/dev/std*\' etc. instead.'
				.format(path, mode))

	while isinstance(f, io.TextIOBase):
		f = f.detach()
	while isinstance(f, io.BufferedIOBase):
		f = f.detach()

	if buffering == 0 or buffering > 1:
		bufsize = buffering
	elif buffering == 1 and 'b' in smode:
		raise ValueError('Line buffering is incompatible with binary mode.')
	else:
		bufsize = max(
			getattr(os.fstat(f.fileno()), 'st_blksize', 0),
			io.DEFAULT_BUFFER_SIZE)
	f = buftype(f, bufsize)

	if 't' in smode:
		if buffering == 0:
			raise ValueError('Unbuffered access is incompatible with text mode.')
		f = io.TextIOWrapper(f, encoding, errors, newline, buffering == 1)

	return f


def open_stdstream( name, encoding=None, errors='strict', newlines=None,
	line_buffering=False
):
	"""Use either stdin or stdout of the 'sys' module with the given options.

	If the given options don't match of the selected existing stream, its
	underlying buffer object is wrapped into a new text file wrapper object.
	The original stream is detached from its buffer and the respective attribute
	in the sys module is set to None.
	"""

	if name != 'stdin' and name != 'stdout':
		raise ValueError('Unsupported stream name: {!r}'.format(name))

	f = getattr(sys, name)
	assert isinstance(f, io.TextIOBase), \
		"{0} has type {1.__module__}.{1.__qualname__} which doesn't derive from {2.__module__}.{2.__qualname__}".format(name, type(f), io.TextIOBase)
	setattr(sys, name, None)

	matches = (
		errors == f.errors and newlines == f.newlines and
		line_buffering == f.line_buffering)
	if matches and encoding is not None and encoding != f.encoding:
		encoding = codecs.lookup(encoding).name
		f_encoding = codecs.lookup(f.encoding).name
		matches = encoding == f_encoding
	if not matches:
		f.flush()
		f = io.TextIOWrapper(f.detach(), encoding, errors, newlines, line_buffering)

	return f


def strip_line_terminator( s, linesep=os.linesep ):
	"""Removes the given suffix from a string."""
	return s[0:len(s)-len(linesep)] if s.endswith(linesep) else s
