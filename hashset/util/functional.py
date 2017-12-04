import itertools, functools, operator


def identity( x ):
	"""Returns its only argument."""
	return x


def methodcaller( func, *args ):
	if callable(func):
		return lambda obj: func(obj, *args)
	else:
		return operator.methodcaller(func, *args)
