def is_pow2( n ):
	"""Tests if an integer is a power of 2, or 0."""
	return not n or not (n & (n - 1))


def ceil_pow2( n ):
	"""Returns the smallest power of 2 that is greater or equal than the given integer."""
	return n if is_pow2(n) else 1 << n.bit_length()


def ceil_div( numerator, denominator ):
	"""Returns the “ceil” quotient of the numerator and the denominator."""
	return -(-numerator // denominator)
