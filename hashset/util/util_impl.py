import operator


def getitem( seq, idx, default ):
	return seq[idx] if 0 <= idx < len(seq) else default


def identity( x ):
	return x


def pad_multiple_of( n, b, fill=b'\0' ):
	b += fill * -(len(b) % -n)
	return b


def property_setter( fset, fget='_{}', doc=None, docref='fset' ):
	if doc is None and docref is not None:
		if docref == 'fget' or docref == 'fset':
			docref = locals()[docref]
			if callable(docref):
				doc = getattr(docref, '__doc__', None)
		else:
			raise ValueError(
				'Invalid reference name in \'docref\': {!r}'.format(docref))

	if isinstance(fget, str):
		if '{}' in fget:
			fget = fget.replace('{}', fset.__name__, 1)
		fget = operator.attrgetter(fget)

	return property(fget, fset, doc)
