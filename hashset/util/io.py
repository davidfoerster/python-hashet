import sys, os, io
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


def strip_line_terminator( s, linesep=os.linesep ):
	return s[0:len(s)-len(linesep)] if s.endswith(linesep) else s