from typing import Collection

from kts.ui.components import HTMLRepr


class Line(HTMLRepr):
    def __init__(self, x: Collection, y: Collection, color: str, width: int = 3):
        self.xs = x
        self.ys = y
        self.color = color
        self.width = width

    def calc_svg_pts(self, height, width, margin, limits):
        min_x, max_x, min_y, max_y = limits
        res = ""
        for i, (x, y) in enumerate(zip(self.xs, self.ys)):
            new_x = ((x - min_x) / (max_x - min_x)) * width + margin
            new_y = height - ((y - min_y) / (max_y - min_y)) * height + margin
            if i == 0:
                res += f" M {new_x} {new_y} "
            res += f" L {new_x} {new_y} "
        return res

    def html(self, height, width, margin, limits):
        return f'<path d="{self.calc_svg_pts(height, width, margin, limits)}" stroke="{self.color}" fill="transparent" stroke-width="{self.width}"/>'


class Plot(HTMLRepr):
    def __init__(self, lines, height=300, width=600, margin=0):
        self.lines = lines
        self.margin = margin
        self.height = height
        self.width = width
        self.limits = self.compute_limits()

    def compute_limits(self):
        min_x = min([min(l.xs) for l in self.lines])
        max_x = max([max(l.xs) for l in self.lines])
        min_y = min([min(l.ys) for l in self.lines])
        max_y = max([max(l.ys) for l in self.lines])
        return min_x, max_x, min_y, max_y

    @property
    def html(self):
        return f'<svg width="{self.width + self.margin * 2}" height="{self.height + self.margin * 2}">' + \
        ''.join([line.html(self.height, self.width, self.margin, self.limits) for line in self.lines]) + \
        '</svg>'
