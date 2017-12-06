import functools, operator


def identity( x ):
	"""Returns its only argument."""
	return x


def comp( *funcs, rev=True ):
	if not funcs:
		return identity
	if len(funcs) == 1:
		return funcs[0]

	if rev:
		funcs = funcs[::-1]
	return functools.partial(functools.reduce, _comp_reducer, funcs)


def _comp_reducer( x, func ):
	return func(x)


def methodcaller( func, *args ):
	if callable(func):
		return lambda obj: func(obj, *args)
	else:
		return operator.methodcaller(func, *args)


is_not_none = functools.partial(operator.is_not, None)

itemgetter = tuple(map(operator.itemgetter, range(2)))


def project_out( *funcs ):
	return lambda x: tuple(f(x) for f in funcs)
