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


def call_last_as_first( func, *args ):
	return func(args[-1], *args[:-1])


def methodcaller( func, *args ):
	"""Retuns a function object that invokes a given method on its first argument.

	The method may be either a callable, in which case it is invoked directly, or
	a method names, in which case this method defers to operator.methodcaller
	instead.

	Any additional arguments are appended after the first argument.
	"""

	if callable(func):
		return functools.partial(call_last_as_first, func, *args)
	else:
		return operator.methodcaller(func, *args)


is_none = functools.partial(operator.is_, None)
is_not_none = functools.partial(operator.is_not, None)

itemgetter = tuple(map(operator.itemgetter, range(2)))


def instance_tester( _type ):
	return functools.partial(call_last_as_first, isinstance, _type)


def project_out( *funcs, mapping=None ):
	"""Returns a function object that "projects" its arguments to tuple elements based on the given function."""

	if mapping is not None:
		mapping = map(operator.itemgetter, mapping)
		funcs = tuple(
			itertools.starmap(comp, zip(funcs, mapping)) if funcs else mapping)

	return lambda x: tuple(map(functools.partial(rapply, x), funcs))
