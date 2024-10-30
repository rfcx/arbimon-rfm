def _polarize_zero_region(X, region):
    (r1i, r1x), (r2i, r2x) = region
    midpt = (r1i + r2i)/2
    if r1x is None:
        if r2x is None:
            r1x, r2x = -1, 1
        else:
            r1x = -r2x
    elif r2x is None:
        r2x = -r1x

    X[(r1i + 1):(midpt)] = r1x
    X[(midpt + 1):r2i] = r2x


def polarize_zero_regions(X):
    zero_region = None
    X2 = X.copy()
    last_sgn=0

    for i in xrange(len(X)):
        x = X[i]
        if i == 0:
            starti = x
        elif x == 0:
            if zero_region is None:
                zero_region = [(i-1, X[i-1] if i > 0 else None)]
        else:
            if zero_region is not None:
                zero_region.append((i, X[i] if i > 0 else None))
                _polarize_zero_region(X2, zero_region)
                zero_region = None
    if zero_region is not None:
        zero_region.append((len(X), None))
        _polarize_zero_region(X2, zero_region)

    return X2
