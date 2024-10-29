import math
import colorsys

spaces = ["rgb", "yiq", "hls", "hsv"]


def convert_spaces(cl, s1, s2):
    cl2 = [min(max(0, c), 1) for c in cl]
    if s1 == s2:
        return cl2
    elif s1 == "rgb":
        return getattr(colorsys, "rgb_to_%s" % s2)(*cl2)
    elif s2 == "rgb":
        return getattr(colorsys, "%s_to_rgb" % s1)(*cl2)
    else:
        rgb = getattr(colorsys, "%s_to_rgb" % s1)(*cl2)
        return getattr(colorsys, "rgb_to_%s" % s2)(*rgb)


def rgb2hsv(cl):
    return convert_spaces(cl, 'rgb', 'hsv')


def hsv2rgb(cl):
    return convert_spaces(cl, 'hsv', 'rgb')


def normalize(cl, scale=255.0):
    if type(scale) not in (list, tuple):
        scale = [scale] * len(cl)
    return [c / float(s) for c, s in zip(cl, scale)]


def quantize(cl, scale=255.0):
    return [int(c * scale) for c in cl]


def grad(p, colors):
    i = max(0, p * (len(colors)-1))
    f, c = map(int, [math.floor(i), math.ceil(i)])
    a = i - f
    return [(c1*(1-a) + c2*a) for (c1, c2) in zip(colors[f], colors[c])]


class AbstractGradient(object):
    def __init__(self):
        raise NotImplementedError()

    def __call__(self, i):
        raise NotImplementedError()

    def get_palette(self, color_count=256):
        d = color_count - 1.0
        return [self(i / d) for i in range(color_count)]


class MatPlotLibGradient(AbstractGradient):
    def __init__(self, name, quant_scale=None):
        import pylab
        self.cmap = getattr(pylab.cm, name)
        self.quant_scale = quant_scale

    def __call__(self, i):
        cl = self.cmap(i)[:3]
        return quantize(cl, self.quant_scale) if self.quant_scale else cl


class LinearGradient(AbstractGradient):
    def __init__(self, colors, norm_scale=1.0, quant_scale=None, spacetx=None):
        self.colors = colors
        self.norm_scale = norm_scale
        self.quant_scale = quant_scale
        self.spacetx = spacetx if callable(spacetx) else (lambda x: x)
        self.grad = [normalize(cl, norm_scale) for cl in colors]

    def _grad_(self, p):
        colors = self.grad
        i = max(0, p * (len(colors)-1))
        f, c = map(int, [math.floor(i), math.ceil(i)])
        a = i - f
        cl = [(c1*(1-a) + c2*a) for (c1, c2) in zip(colors[f], colors[c])]
        return cl

    def __call__(self, i):
        cl = self.spacetx(self._grad_(i))
        return quantize(cl, self.quant_scale) if self.quant_scale else cl


class MultiGradient(AbstractGradient):
    def __init__(self, gradients, quant_scale=None):
        self.gradients = gradients
        self.quant_scale = quant_scale

    def __call__(self, i):
        c = len(self.gradients)
        j = i * c
        q = min(math.floor(j), c-1)
        cl = self.gradients[int(q)](j-q)
        return quantize(cl, self.quant_scale) if self.quant_scale else cl


if __name__ == "__main__":
    for r in range(256):
        for g in range(256):
            for b in range(256):
                rgb = (r, g, b)
                hsv = rgb2hsv(rgb)
                rgb2 = hsv2rgb(hsv)
                print("rgb : %r, hsv : %r, rgb2: %r" % (rgb, hsv, rgb2))
