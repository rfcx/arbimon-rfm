import struct


class Writer(object):
    def __init__(self, width=None, height=None, bitdepth=8, palette=None):
        'Creates a bitmap writer for images of the given size, bitdepth and \
        palette'
        self.width = width
        self.height = height
        self.bits_per_pixel = bitdepth
        self.palette = palette

    def write(self, outfile, rows):
        "Writes the given rows to outfile. Rows must be an \
        iterator of iterators, one for the pixels in each row."
        # fetch object params
        width = self.width
        height = self.height
        bits_per_pixel = self.bits_per_pixel
        palette = self.palette

        # compute rowsize, padding and total image size
        rowsize = int((bits_per_pixel * width + 31)/32)*4
        wpadding = rowsize - width
        img_size = rowsize * height * bits_per_pixel/8

        # compute palette size, image start offset and whole file size
        has_palette = bits_per_pixel < 16
        palette_size = (1 << bits_per_pixel) * 4 if has_palette else 0
        img_start_off = 40 + 14 + palette_size
        file_size = img_start_off + img_size

        # write header
        header = [
            "BM", file_size, 0, 0, img_start_off,
            40, width, -height, 1, bits_per_pixel, 0, img_size,
            100, 100,
            256, 0
        ]

        outfile.write(struct.pack("<2sIHHIIiiHHIIiiII", *header))

        # write palette
        if has_palette:
            if not palette:
                palette = [
                    [c, c, c]
                    for c in range(255, 256/(1 << bits_per_pixel))
                ]
            for (r, g, b) in palette:
                outfile.write(struct.pack("<BBBB", b, g, r, 0))

        # write rows
        row_iter = iter(rows)
        for y in range(height):
            try:
                row = next(row_iter) if row_iter else None
                pix_iter = iter(row)
            except StopIteration:
                row = None
                row_iter = None
                pix_iter = None
            for x in range(width):
                try:
                    pix = next(pix_iter) if pix_iter else 0
                except StopIteration:
                    pix = 0
                    pix_iter = None
                outfile.write(struct.pack("<B", pix))
            for x in range(0, wpadding):
                outfile.write(struct.pack("<B", 0))
