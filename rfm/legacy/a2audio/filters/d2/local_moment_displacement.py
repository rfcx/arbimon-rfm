import numpy
import scipy.ndimage

def local_moment_displacement(X, moment=1, r=1, mode='constant'):
    s = X.shape
    zr = numpy.array([range(-r, r+1) for i in range(-r, r+1)]) ** moment
    diam = 2*r+1
    area = diam * diam
    oner = numpy.ones([diam, diam])
    mu = numpy.zeros([s[0], s[1], 2])
    X = X + 1 - numpy.min(X)
    N = scipy.ndimage.filters.convolve(X, oner, mode=mode)
    N[N==0] = 1
    mu[:, :, 0] = scipy.ndimage.filters.convolve(X, zr.T, mode=mode) / N
    mu[:, :, 1] = scipy.ndimage.filters.convolve(X, zr, mode=mode) / N

    return mu
