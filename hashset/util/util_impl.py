import operator, collections.abc


def as_tuple( *args ):
	"""Returns the argument list as a tuple."""
	return args


def getitem( seq, idx, default ):
	"""
	Returns the item at the given index from a sequence if the index lies within
	the acceptable range, or 'default'.
	"""
	return seq[idx] if 0 <= idx < len(seq) else default


def getlength( obj, default=None ):
	return len(obj) if isinstance(obj, collections.abc.Sized) else default


def pad_multiple_of( b, n, fill=b'\0' ):
	"""Pad the length of 'b' to a multiple of 'n' using 'fill'."""

	l = b if isinstance(b, int) else len(b)
	l -= (l % -n)
	return l if isinstance(b, int) else b.ljust(l, fill)


def attrdeleter( name ):
	"""Returns a function that calls 'delattr' with the given name."""
	return lambda obj: delattr(obj, name)


def property_setter( fset, fget='_{}', fdel=None, doc=None, docref='fset' ):
	"""Returns a property that, by default, only overrides the setter method and uses a default getter method.

	The default getter method is based on a backing attribute name which maybe be derived from the name of the given setter method in the follwing way:
	The first instance of the infix '{}' is replaced with the setter method name.

	If no doc string is specified via 'doc', it may be taken from either the getter
	('fget'), the setter ('fset') or the deleter ('fdel') name via 'docref'.
	"""

	if isinstance(fget, str):
		fget = operator.attrgetter(fget.replace('{}', fset.__name__, 1))

	if isinstance(fdel, str):
		fdel = attrdeleter(fdel.replace('{}', fset.__name__, 1))

	if doc is None and docref is not None:
		if docref in ('fget', 'fset', 'fdel'):
			docref = locals()[docref]
			if callable(docref):
				doc = getattr(docref, '__doc__', None)
		else:
			raise ValueError(
				'Invalid reference name in \'docref\': {!r}'.format(docref))

	return property(fget, fset, fdel, doc)
