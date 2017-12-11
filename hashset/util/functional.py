import itertools, functools, operator


def identity( x ):
	"""Returns its only argument."""
	return x


def comp( *funcs, rev=True ):
	"""Returns a function object that concatenates the given function calls.

	The concatenation is performed from right to left unless the 'rev' argument
	is False.
	"""

	if not funcs:
		return identity
	if len(funcs) == 1:
		return funcs[0]

	if rev:
		funcs = funcs[::-1]
	return functools.partial(functools.reduce, rapply, funcs)


def rapply( x, func ):
	return func(x)


def call_as_first( first_idx, func, *args ):
	if first_idx >= len(args):
		raise TypeError(
			'Expected at least {0:d} arguments to invoke '
			'"{2.__module__}.{2.__qualname__}" through '
			'"{3.__module__}.{3.__qualname__}", got {1:d}'
				.format(first_idx + 1, len(args), func, call_as_first))

	args = list(args)
	first = args.pop(first_idx)
	return func(first, *args)


def methodcaller( func, *args ):
	"""Retuns a function object that invokes a given method on its first argument.

	The method may be either a callable, in which case it is invoked directly, or
	a method names, in which case this method defers to operator.methodcaller
	instead.

	Any additional arguments are appended after the first argument.
	"""

	if callable(func):
		return functools.partial(call_as_first, len(args), func, *args)
	else:
		return operator.methodcaller(func, *args)


is_none = functools.partial(operator.is_, None)
is_not_none = functools.partial(operator.is_not, None)


def attrsetter( name ):
	return functools.partial(call_as_first, 1, setattr, name)


def attrdeleter( name ):
	"""Returns a function that calls 'delattr' with the given name."""
	return functools.partial(call_as_first, 1, delattr, name)


def instance_tester( _type ):
	return functools.partial(call_as_first, 1, isinstance, _type)


def project_out( *funcs, mapping=None, mapping_type=operator.itemgetter ):
	"""Returns a function object that "projects" its arguments to tuple elements based on the given function."""

	if mapping is not None:
		mapping = map(mapping_type, mapping)
		funcs = tuple(
			itertools.starmap(comp, zip(funcs, mapping)) if funcs else mapping)

	return lambda x: tuple(map(functools.partial(rapply, x), funcs))
