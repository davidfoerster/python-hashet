import itertools
from .util_impl import identity


def saccumulate( start, iterable, _slice=None ):
	if _slice is not None:
		iterable = itertools.islice(
			iterable, _slice.start, _slice.stop, _slice.step)

	return itertools.accumulate(itertools.chain((start,), iterable))


def each( func, iterable ):
	for item in iterable:
		func(item)


def stareach( func, iterable ):
	for item in iterable:
		func(*item)


def iskip( iterable, skip ):
	it = iter(iterable)
	try:
		for ignored in range(skip):
			next(it)
	except StopIteration:
		pass
	return it


def ichain( iterable, *suffix ):
	return itertools.chain(iterable, suffix)


def iconditional( iterable, pred=bool, func_true=identity, func_false=identity ):
	if not callable(func_true):
		val_true = func_true
		func_true = lambda x: val_true
	if not callable(func_false):
		val_false = func_false
		func_false = lambda x: val_false

	return (func_true(x) if pred(x) else func_false(x) for x in iterable)
