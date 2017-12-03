def is_pow2( n ):
	return not n or not (n & (n - 1))


def ceil_pow2( n ):
	return n if is_pow2(n) else 1 << n.bit_length()


def getitem( seq, idx, default ):
	return seq[idx] if 0 <= idx < len(seq) else default


def identity( x ):
	return x
