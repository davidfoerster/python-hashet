import itertools
from .util_impl import getlength
from .functional import identity, starcall


def islice( iterable, start, *args ):
	"""Very similar to 'itertools.islice' but supports negative start and stop values if the underlying iterable has a length."""

	if len(args) == 0:
		stop = start
		start = None
		step = None
	elif len(args) == 1:
		stop = args[0]
		step = None
	elif len(args) == 2:
		stop, step = args
	else:
		raise TypeError(
			'Expected 2 to 4 arguments, got {:d}'.format(len(args) + 2))

	if step is None:
		step = 1

	if stop is None:
		stop = getlength(iterable)
	else:
		if stop < 0:
			stop = max(stop + len(iterable), 0)
		stop = min(stop, getlength(iterable, stop))

	if start is None:
		start = 0
	elif start < 0:
		start = max(start + len(iterable), 0)

	if stop is not None and start >= stop:
		iterable = ()
	elif (start > 0 or step != 1 or
		(stop is not None and stop < getlength(iterable, stop))
	):
		iterable = itertools.islice(iterable, start, stop, step)

	return iterable


def accumulate( iterable, *start ):
	"""Accumulates the values of an iterable (as with 'itertools.accumulate'), prefixed with the additional arguments."""

	if start:
		iterable = itertools.chain(start, iterable)
	return itertools.accumulate(iterable)


def each( func, iterable ):
	"""Calls the given function on each iterable item."""
	iterable = iter(iterable)
	try:
		item = next(iterable)
	except StopIteration:
		return False
	func(item)
	for item in iterable:
		func(item)
	return True


def stareach( func, iterable ):
	"""Calls the given function with argument expansion on each iterable item."""
	return each(starcall(func), iterable)


def ichain( iterable, *suffix ):
	"""Returns an iterator that lists all items of 'iterable' followed by the remainder of the given arguments."""
	return itertools.chain(iterable, suffix)


def iconditional( iterable, pred=bool, func_true=identity,
	func_false=identity
):
	"""Returns an iterator that maps the items of 'iterable' using 'func_true' or 'func_false' based on the return value of the given predicate."""

	if not callable(func_true):
		val_true = func_true
		func_true = lambda x: val_true
	if not callable(func_false):
		val_false = func_false
		func_false = lambda x: val_false

	return (func_true(x) if pred(x) else func_false(x) for x in iterable)
