import numpy


def histogramize(X, hist=None, bins=100):
    Xbins = numpy.ones_like(X)
    if hist is None:
        hist = numpy.histogram(X, bins=bins)
    counts, borders = hist
    for i in xrange(len(counts)):
        Xbins[(borders[i] < X)*(borders[i+1] < X)] = i
    Xbins[borders[-1] <= X] = len(counts)-1
    return Xbins
