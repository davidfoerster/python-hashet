import operator


def getitem( seq, idx, default ):
	"""
	Returns the item at the given index from a sequence if the index lies within
	the acceptable range, or 'default'.
	"""
	return seq[idx] if 0 <= idx < len(seq) else default


def identity( x ):
	"""Returns its only argument."""
	return x


def pad_multiple_of( n, b, fill=b'\0' ):
	"""Pad the length of 'b' to a multiple of 'n' using 'fill'."""

	l = len(b)
	return b.ljust(l - (l % -n), fill)


def property_setter( fset, fget='_{}', doc=None, docref='fset' ):
	"""Returns a property that, by default, only overrides the setter method and uses a default getter method.

	The default getter method is based on a backing attribute name which maybe be derived from the name of the given setter method in the follwing way:
	The first instance of the infix '{}' is replaced with the setter method name.

	If no doc string is specified via 'doc', it may be taken from either the getter
	('fget') or the setter ('fset') name via 'docref'.
	"""

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
