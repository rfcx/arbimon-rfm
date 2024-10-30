from a2pyutils import colors
import pylab


palette = [
    colors.MultiGradient([
        colors.LinearGradient(
            [[43/60.0, 1.0, .9], [31/60.0, 1.0, .9]], spacetx=colors.hsv2rgb
        ),
        colors.LinearGradient(
            [[23/60.0, 1.0, .9], [11/60.0, 1.0, .9]], spacetx=colors.hsv2rgb
        ),
        colors.LinearGradient(
            [[10/60.0, 1.0, .9], [6/60.00, 0.3, .9]], spacetx=colors.hsv2rgb
        ),
        colors.LinearGradient(
            [[0x90, 0x61, 0x24], [0xea, 0xca, 0xb9]], norm_scale=255.0
        )
    ], 255).get_palette(256),
    [[255-i, 255-i, 255-i] for i in range(256)],
]

for x in pylab.cm.datad:
    if (x in ['hot','jet'] or 'gist_' in x) and '_r' not in x:
        palette.append(colors.MatPlotLibGradient(x, 255).get_palette(256))


def get_palette(id=1):
    return palette[id % len(palette)]


def export_palette(palette, fileout):
    if fileout[:4] == ".png":
        import png
        W = png.Writer(
            width=width, height=height, bitdepth=bpp, palette=palette
        )
    else:
        import bmpio
        W = bmpio.Writer

    h = len(palette)
    w = W(width=1, height=h, bitdepth=8, palette=palette)
    with file(fileout, "wb") as fout:
        w.write(fout, [[h - i - 1] for i in range(h)])


def print_hex(palette):
    def h(c):
        hs = hex(int(c))[2:]
        return ('0'*(2-len(hs))) + hs

    def cl(x):
        return '#%s%s%s' % (h(x[0]),h(x[1]),h(x[2]))

    print [cl(x) for x in palette]
