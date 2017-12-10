import itertools
from .functional import identity


def saccumulate( start, iterable, _slice=None ):
	"""Slices an iterator (as with 'itertools.islice') and accumulates its values
	(as with 'itertools.accumulate'), starting with the additional value 'start'."""

	if _slice is not None:
		iterable = itertools.islice(
			iterable, _slice.start, _slice.stop, _slice.step)

	return itertools.accumulate(itertools.chain((start,), iterable))


def each( func, iterable ):
	"""Calls the given function on each iterable item."""
	for item in iterable:
		func(item)


def stareach( func, iterable ):
	"""Calls the given function with argument expansion on each iterable item."""
	for item in iterable:
		func(*item)


def iskip( iterable, skip ):
	"""Returns an iterator that skips the the first 'skip' items of the iterator created from 'iterable'."""

	it = iter(iterable)
	try:
		for ignored in range(skip):
			next(it)
	except StopIteration:
		pass
	return it


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
