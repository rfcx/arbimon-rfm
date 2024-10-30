import numpy
import scipy.ndimage


def local_moment_displacement(X, moment=1, r=5, axis=-1, mode="constant"):
    "Computes the local moment displacement in one direction"
    d = 2*r+1
    k = numpy.arange(r, -r-1, -1) ** moment
    Xz = scipy.ndimage.convolve1d(X, k, axis, mode=mode)
    X1 = scipy.ndimage.convolve1d(X, numpy.ones([d]), axis, mode=mode)
    X1[X1 == 0] = 1
    return Xz / X1
