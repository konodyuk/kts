from typing import Collection, List, Dict, Union

import pygments.token as pt
from IPython.display import display
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name
from pygments.style import Style

from kts.settings import cfg
from kts.util.formatting import format_value


class KTSStyle(Style):
    default_style = "bg: #000000"
    styles = {
        pt.Comment: 'italic #408080',
        pt.Keyword: 'bold #4caf50',
        pt.Name: '#90caf9',
        pt.Name.Function: '#64b5f6',
        pt.Name.Class: '#64b5f6',
        pt.Name.Variable.Instance: '#ffcc80',
        pt.Name.Decorator: 'bold #81d4f4',
        pt.String: '#4dff4d',
        pt.Number: '#ce93d8',
        pt.Operator: 'bold #fffd54',
    }

custom_styles = {'kts': KTSStyle}

class Highlighter:
    def __init__(self):
        self.lexer = get_lexer_by_name("python", stripall=True)
        self.set_style('kts')

    def highlight(self, code):
        return highlight(code, self.lexer, self.formatter)
    
    @property
    def css(self):
        return f'<style>\n{self.formatter.get_style_defs([".kts .code"])}\n</style>\n'
    
    def set_style(self, style):
        if style in custom_styles:
            style = custom_styles[style]
        self.formatter = HtmlFormatter(style=style, nowrap=True, nobackground=True)

CSS_STYLE = """
.kts .wrapper {{
  display: inline-flex;
  flex-direction: column;
  background-color: {first};
  padding: 10px;
  border-radius: 20px;
}}
.kts .wrapper-border {{
  border: 0px solid {second};
}}
.kts .pool {{
  display: flex;
  flex-wrap: wrap;
  background-color: {second};
  padding: 10px;
  border-radius: 20px;
  margin: 5px;
}}
.kts .field {{
  text-align: left;
  border-radius: 15px;
  padding: 5px 15px;
  margin: 5px;
  display: inline-block;
}}
.kts .field-bg {{
  background-color: {second};
}}
.kts .field-bold {{
  font-weight: bold;
}}
.kts .field-third {{
  color: {third};
}}
.kts .field-accent {{
  color: {accent};
}}
.kts .field-bg:hover {{
  background-color: {fourth};
}}
.kts .annotation {{
  text-align: left;
  margin-left: 20px;
  margin-bottom: -5px;
  display: inline-block;
  color: {third};
}}
.kts .title {{
  text-align: center;
  display: inline-block;
  font-weight: bold;
  color: {third};
}}
.kts .code {{
  background-color: {second};
  text-align: left;
  border-radius: 15px;
  padding: 0.5rem 15px;
  margin: 5px;
  color: white;
  display: inline-block;
}}
.kts .code:hover {{
  background-color: {fourth};
}}
.kts .code pre {{
  background-color: {second};
}}
.kts .code:hover pre {{
  background-color: {fourth};
}}
.kts .output {{
  background-color: {second};
  text-align: left;
  border-radius: 15px;
  padding: 5px 15px;
  margin: 5px;
  font-weight: bold;
  color: {accent};
  overflow: auto;
  max-height: 3rem;
  display: flex;
  flex-direction: column-reverse;
}}

.kts .df {{
  background-color: {second};
  text-align: left;
  border-radius: 15px;
  padding: 5px 15px;
  margin: 5px;
  display: inline-block;
  color: {accent};
}}

.kts .title-with-cross {{
  display: grid;
  grid-template-columns: 1rem auto 1rem;
  margin-left: 5px;
  margin-right: 5px;
}}
.kts .cross-circle {{
  background-color: {second};
  width: 1rem;
  height: 1rem;
  position: relative;
  border-radius: 50%;
  cursor: pointer;
  z-index: 2;
  margin-top: 2px;
}}
.kts .cross-before,
.kts .cross-after {{
  background-color: {first};
  content: '';
  position: absolute;
  width: 0.75rem;
  height: 2px;
  border-radius: 0;
  top: calc((1rem - 2px) / 2);
  z-index: 0;
}}
.kts .cross-before {{
  -webkit-transform: rotate(-45deg);
  -moz-transform: rotate(-45deg);
  transform: rotate(-45deg);
  left: calc(1rem / 8);
}}
.kts .cross-after {{
  -webkit-transform: rotate(-135deg);
  -moz-transform: rotate(-135deg);
  transform: rotate(-135deg);
  right: calc(1rem / 8);
}}

.kts #hidden {{
  display: none
}}
.kts .thumbnail {{
  margin: 0;
  cursor: pointer;
}}
.kts .thumbnail-first {{
  background-color: {first};
}}
.kts .thumbnail-second {{
  background-color: {second};
}}
.kts #collapsible {{
  -webkit-transition: max-height {anim_height}, padding {anim_padding}; 
  -moz-transition: max-height {anim_height}, padding {anim_padding}; 
  -ms-transition: max-height {anim_height}, padding {anim_padding}; 
  -o-transition: max-height {anim_height}, padding {anim_padding}; 
  transition: max-height {anim_height}, padding {anim_padding};  
  
  padding: 0;
  border: 2px solid {second};
  align-self: flex-start;
  max-height: 100px;
  overflow: hidden;
}}
.kts .check {{
  display: none;
}}
.kts .check:checked + #collapsible {{
  padding: 10px;
  max-height: {max_height_expanded};
}}
.kts .check:checked + #collapsible > #hidden {{
  display: inline-flex;
}}
.kts .check:checked + #collapsible > .thumbnail {{
  display: none;
}}
.kts .check:checked + .wrapper-border {{
  border: 2px solid {second};
}}
.kts .check-outer {{
  display: none;
}}
.kts .check-outer:checked + #collapsible {{
  padding: 10px;
  max-height: {max_height_expanded};
}}
.kts .check-outer:checked + #collapsible > #hidden {{
  display: inline-flex;
}}
.kts .check-outer:checked + #collapsible > .thumbnail {{
  display: none;
}}
.kts .check-outer:checked + .wrapper-border {{
  border: 2px solid {second};
}}
.kts .inner-wrapper {{
  flex-direction: column;
}}

.kts progress[value], .kts progress:not([value]) {{
  -webkit-appearance: none;
  appearance: none;
  
  padding: 3px;
  width: calc(100% - 10px);
  height: 1rem;
}}

.kts progress[value]::-webkit-progress-bar {{
  background-color: {second};
  border-radius: 15px;
  padding: 3px;
}}

.kts progress[value]::-webkit-progress-value {{
  background-color: {third};
  border-radius: 15px; 
}}

.kts .hbar-container {{
  display: block;
  position: relative;
  height: min(calc(100% - 3px), 1.5rem);
  margin: 2px;
}}
.kts .hbar {{
  position: absolute;
  display: inline-block;
  background-color: {second};
  text-align: left;
  height: 100%;
  border-radius: 15px;
}}
.kts .hbar-line {{
  position: absolute;
  display: inline-block;
  background-color: {accent};
  text-align: left;
  height: 1px;
  top: 50%;
}}

.kts .inner-column {{
  display: flex;
  flex-direction: column;
  padding: auto;
}}
.kts .row {{
  display: flex;
  flex-direction: row;
}}
"""

class Theme:
    def __init__(self, 
                 first: str, 
                 second: str, 
                 third: str, 
                 fourth: str, 
                 accent: str, 
                 second_accent: str = "#000",
                 anim_height: str = "1.0s",
                 anim_padding: str = "0.7s",
                 max_height_expanded: str = "3000px",
                ):
        self.first = first
        self.second = second
        self.third = third
        self.fourth = fourth
        self.accent = accent
        self.second_accent = second_accent
        self.anim_height = anim_height
        self.anim_padding = anim_padding
        self.max_height_expanded = max_height_expanded

    @property
    def css(self):
        kw = {
            "first": self.first,
            "second": self.second,
            "third": self.third,
            "fourth": self.fourth,
            "accent": self.accent,
            "second_accent": self.second_accent,
            "anim_height": self.anim_height,
            "anim_padding": self.anim_padding,
            "max_height_expanded": self.max_height_expanded,
        }
        return CSS_STYLE.format(**kw)

themes = {
    'blue': Theme("#2e3047", "#43455c", "#707793", "#000", "#3bba9c", ""),
    'kts':  Theme("#2e3047", "#43455c", "#707793", "#000", "#3bba9c", ""), # red and white and black
    'light':  Theme("#e7ecee", "#d0d8e3", "#a8b7cc", "#8492ab", "#f5d8d5", "#f5ece6"),
    'grey':  Theme("#333", "#444", "#888", "#000", "#ddd"),
    'violet': Theme("#392338", "#523957", "#ff947f", "#000", "#ffcfa4"),
    'dark':  Theme("#050b28", "#262f58", "#2e4a7a", "#1e96a2", "#4ebbd9d", "#dab236"),
    'unknown_1': Theme("#333b3e", "#3e555e", "#888b8e", "", "#fff"),
    'unknown_2': Theme("#a0aec3", "#7c879f", "#cbd3de", "#e5e9dc", "#efd3d1"),
}

class HTMLRepr:
    def _repr_html_(self):
        return f'<div class="kts">{self.html}</div>'


# ========== core blocks ==========


class CSS(HTMLRepr):
    def __init__(self, style):
        self.style = style
    
    @property
    def html(self):
        return f'<div><style scoped>\n{self.style}\n</style></div>\n'
    
class CurrentTheme(HTMLRepr):
    def __init__(self, hl, theme):
        self.sample_code = """@decorator\ndef func(arg):\n    return arg + 1"""
        self.hl = hl
        self.theme = theme
        
    def set_highlighting(self, hl):
        self.hl = hl
        
    def set_theme(self, theme):
        self.theme = theme
        
    @property
    def html(self):
        return Column([
            CSS(self.hl.css),
            CSS(self.theme.css),
            Title('theme'),
            Annotation('annotation'),
            Field('field'),
            Annotation('code'),
            Code(self.sample_code),
        ]).html
    
class Column(HTMLRepr):
    def __init__(self, elements, border=False, style=""):
        self.elements = elements
        self.style = style
        self.border = border

    @property
    def html(self):
        css_class = ""
        if self.border:
            css_class += 'wrapper-border'
        stacked = "\n".join([e.html for e in self.elements])
        return f"""<div class="{css_class} wrapper" style="{self.style}">{stacked}</div>"""
    
class Pool(HTMLRepr):
    def __init__(self, elements):
        self.elements = elements
        
    @property
    def html(self):
        stacked = "\n".join([e.html for e in self.elements])
        return f"""<div class="pool">{stacked}</div>"""
    
class CollapsibleColumn(HTMLRepr):
    def __init__(self, elements, thumbnail, cssid, outer=False, border=False):
        self.elements = elements
        self.thumbnail = thumbnail
        self.cssid = cssid
        self.outer = outer
        self.border = border
    
    @property
    def html(self):
        stacked = "\n".join([e.html for e in self.elements])
        if self.outer:
            check_class = "check-outer"
        else:
            check_class = "check"
        column_class = ""
        if self.border:
            column_class += 'wrapper-border'
        return f"""<input type="checkbox" class="{check_class}" id="{self.cssid}"/>
            <div class="wrapper {column_class}" id="collapsible">{self.thumbnail.html}<div class="inner-wrapper" id="hidden">{stacked}</div>
        </div>"""

class InnerColumn(HTMLRepr):
    def __init__(self, elements):
        self.elements = elements

    @property
    def html(self):
        stacked = "\n".join([e.html for e in self.elements])
        return f"""<div class="inner-column">{stacked}</div>"""
    
class Row(HTMLRepr):
    def __init__(self, elements):
        self.elements = elements

    @property
    def html(self):
        stacked = "\n".join([e.html for e in self.elements])
        return f"""<div class="row">{stacked}</div>"""

class AlignedColumns(HTMLRepr):
    def __init__(self, columns, title=None, bg=True, style=None):
        self.columns = columns
        self.title = title
        self.bg = bg
        self.style = style
        
    @property
    def html(self):
        n_cols = len(self.columns)
        res = f"""<div {'class="wrapper"' if self.bg and not self.title else ''} style="display: inline-grid; grid-template-columns: {'auto ' * n_cols if not self.style else self.style};">"""
        for row in zip(*self.columns):
            res += ''.join(row) + '\n'
        res += "</div>"
        if self.title is not None:
            res = f"""<div {'class="wrapper"' if self.bg and self.title else ''} style="display: inline-grid; grid-template-rows: auto auto;">{Title(self.title, style="margin-bottom: 5px;").html}{res}</div>"""
        return res
    
class Field(HTMLRepr):
    def __init__(self, string, bold=True, accent=True, bg=True, style=""):
        self.string = string
        self.bold = bold
        self.accent = accent
        self.bg = bg
        self.style = style
    
    @property
    def html(self):
        css_class = "field"
        if self.bold:
            css_class += " field-bold"
        if self.bg:
            css_class += " field-bg"
        if self.accent:
            css_class += " field-accent"
        else:
            css_class += " field-third"
        return f"""<div class="{css_class}" style="{self.style}">{self.string}</div>"""
    
class Output(HTMLRepr):
    def __init__(self, string):
        self.string = string
    
    @property
    def html(self):
        lines = self.string.split('\n')[::-1]
        inner_css = '\n'.join([f"<div>{line}</div>" for line in lines])
        return f"""<div class="output">{inner_css}</div>"""
    
class Annotation(HTMLRepr):
    def __init__(self, string, style=""):
        self.string = string
        self.style = style
    
    @property
    def html(self):
        return f"""<div class="annotation" style="{self.style}">{self.string}</div>"""
    
class Title(HTMLRepr):
    def __init__(self, string, style=""):
        self.string = string
        self.style = style
    
    @property
    def html(self):
        return f"""<div class="title" style="{self.style}">{self.string.upper()}</div>"""
    
class TitleWithCross(HTMLRepr):
    def __init__(self, string, cssid):
        self.string = string
        self.cssid = cssid
    
    @property
    def html(self):
        return f"""<div class="title-with-cross">
            <div></div>
            <div class="title">{self.string.upper()}</div>
            <label class="cross-circle" for="{self.cssid}">
                <div class="cross-before"></div>
                <div class="cross-after"></div>
            </label>
        </div>"""
    
class ThumbnailField(HTMLRepr):
    def __init__(self, string, cssid, bold=True, accent=True, first=True, bg=True, style=""):
        self.string = string
        self.cssid = cssid
        self.bold = bold
        self.accent = accent
        self.bg = bg
        self.first = first
        self.style = style
    
    @property
    def html(self):
        css_class = "field"
        if self.bold:
            css_class += " field-bold"
        if self.bg:
            css_class += " field-bg"
        if self.accent:
            css_class += " field-accent"
        else:
            css_class += " field-third"
        if self.first:
            css_class += " thumbnail-first"
        else:
            css_class += " thumbnail-second"
        return f"""<label class="{css_class} thumbnail" style="{self.style}" for="{self.cssid}">{self.string}</label>"""
    
class Progress(HTMLRepr):
    def __init__(self, value=None, total=None,  style=""):
        self.value = value
        self.total = total
        self.style = style
    
    @property
    def html(self):
        if isinstance(self.value, int) and isinstance(self.total, int):
            return f"""<progress value="{self.value}" max="{self.total}" style="{self.style}"></progress>"""
        else:
            return f"""<progress max="1" style="{self.style}"></progress>"""
    
class Code(HTMLRepr):
    def __init__(self, code):
        self.code = code
        self.html_code = ct.hl.highlight(code)

    @property
    def html(self):
        return f"""<div class="code"><pre>{self.html_code}</pre></div>"""
    
class DF(HTMLRepr):
    def __init__(self, df):
        self.df = df
        
    @property
    def html(self):
        return f"""<div class="df">{self.df._repr_html_()}</div>"""
    
class Raw(HTMLRepr):
    def __init__(self, html):
        self.html = html

# ========== compound blocks ==========

class Hbar(HTMLRepr):
    def __init__(self, width):
        self.width = width
        
    @property
    def html(self):
        return f'<div class="hbar" style="width: {self.width}px"></div>'
    
class Importance(HTMLRepr):
    def __init__(self, value, vmin, vmax, zero):
        self.value = value
        self.vmin = vmin - 1
        self.vmax = vmax - 1
        self.zero = zero
        
    @property
    def html(self):
        if self.value >= self.zero:
            hbar_style = f'width: {self.value - self.zero}px; left: {self.zero}px;'
        else:
            hbar_style = f'width: {self.zero - self.value}px; left: {self.value}px;'
        return f'''<div class="hbar-container" style="width: {self.vmax}px">
        <div class="hbar" style="{hbar_style}"></div>
        {f'<div class="hbar-line" style="width: 1px; left: {self.zero}px; height: calc(100% + 4px); top: -2px;"></div>' if self.zero else ''}
        <div class="hbar-line" style="width: {self.vmax - self.vmin}px; left: {self.vmin}px"></div>
        <div class="hbar-line" style="width: 1px; height: 5px; top: calc(50% - 2px); left: {self.vmin}px"></div>
        <div class="hbar-line" style="width: 1px; height: 5px; top: calc(50% - 2px); left: {self.vmax}px"></div>
        </div>
        '''

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

class SingleModelFittingReport:
    def __init__(self, n_steps=None, metric_name=None):
        self.n_steps = n_steps
        self.step = 0
        self.train_scores = []
        self.valid_scores = []
        self.current_train_score = None
        self.current_valid_score = None
        self.metric_name = metric_name
        self.metric_value = None
        self.start = None
        self.took = None
        self.eta = None

    def update(self, step, train_score=None, valid_score=None, timestamp=None):
        self.step = max(step, self.step)
        if self.start is None:
            self.start = timestamp
        else:
            self.took = timestamp - self.start
            self.eta = (self.n_steps - self.step) / (self.step) * self.took
        if train_score is not None:
            self.train_scores.append((step, train_score))
            self.train_scores = sorted(self.train_scores, key=lambda x: x[0])
            self.current_train_score = self.train_scores[-1][1]
        if valid_score is not None:
            self.valid_scores.append((step, valid_score))
            self.valid_scores = sorted(self.valid_scores, key=lambda x: x[0])
            self.current_valid_score = self.valid_scores[-1][1]

    def finish(self):
        self.step = self.n_steps
        
    def set_metric_value(self, value):
        self.metric_value = value
        
    def clip_outliers(self, lines):
        res_lower = np.inf
        res_upper = -np.inf
        for a in lines:
            if len(a) < 20:
                continue
            x = list(zip(*a))[0]
            y = list(zip(*a))[1]
            spl = np.array_split(y, 20)
            maximums = list(map(np.max, spl))
            minimums = list(map(np.min, spl))
            if np.mean(spl[0]) < np.mean(spl[1]):
                lower = min(np.min(spl[-1]), np.mean(y))
                upper = np.max(y)
            else:
                lower = np.min(y)
                upper = max(np.max(spl[-1]), np.mean(y))
            res_lower = min(lower, res_lower)
            res_upper = max(upper, res_upper)
        if res_lower == np.inf:
            return lines
        res = []
        for a in lines:
            x = list(zip(*a))[0]
            y = list(zip(*a))[1]
            new_x = x
            new_y = np.clip(y, res_lower, res_upper)
            if len(new_y) > 100:
                spl_x = np.array_split(new_x, 100)
                spl_y = np.array_split(new_y, 100)
                new_x = list(map(np.mean, spl_x))
                new_y = list(map(np.mean, spl_y))
            res.append(list(zip(new_x, new_y)))
        return res
    
    def get_first(self, a):
        return [i[0] for i in a]
    
    def get_second(self, a):
        return [i[1] for i in a]

    def html(self, active=False, annotations=False):
        ind_kw = dict(style='width: 4.5rem; padding-top: 0px; padding-bottom: 0px;', bg=False, bold=True)
        indicators = Row([
            InnerColumn([Annotation('progress')] * (annotations) + [Progress(self.step, self.n_steps, style='width: 400px; margin-top: 7px;')]), # calc(600px - 12rem - 150px)
            InnerColumn([Annotation('train')] * (annotations) + [Field(format_value(self.current_train_score), **ind_kw, accent=False)]),
            InnerColumn([Annotation('valid')] * (annotations) + [Field(format_value(self.current_valid_score), **ind_kw)]),
            InnerColumn([Annotation(self.metric_name)] * (annotations) + [Field(format_value(self.metric_value), **ind_kw)]),
            InnerColumn([Annotation('took')] * (annotations) + [Field(format_value(self.took, True), **ind_kw)]),
            InnerColumn([Annotation('eta')] * (annotations) + [Field(format_value(self.eta, True), **ind_kw)]),
        ])
        if active:
            lines = []
            colors = []
            if len(self.train_scores) >= 5:
                lines.append(self.train_scores)
                colors.append(ct.theme.third)
            if len(self.valid_scores) >= 5:
                lines.append(self.valid_scores)
                colors.append(ct.theme.accent)
            if len(lines) >= 1:
                lines = self.clip_outliers(lines)
                for i in range(len(lines)):
                    steps = list(zip(*lines[i]))[0]
                    scores = list(zip(*lines[i]))[1]
                    lines[i] = Line(steps, scores, colors[i])
                indicators.elements[0].elements.append(Field(Plot(lines, height=200, width=385).html, bg=False, style='padding-right: 5px; padding-left: 5px;'))
                return indicators.html
            else:
                return indicators.html
        else:
            return indicators.html

class CVFittingReport(HTMLRepr):
    def __init__(self, n_folds=5, n_steps=None, metric_name='metric'):
        self.n_folds = n_folds
        self.reports = [SingleModelFittingReport(n_steps, metric_name) for i in range(n_folds)]
        self.active = set()
        self.current_fold = None
        
    def set_fold(self, value):
        self.current_fold = value
        self.active.clear()
        self.active.add(value)
        
    def update(self, step, train_score=None, valid_score=None, timestamp=None):
        self.reports[self.current_fold].update(step, train_score, valid_score, timestamp)
        
    def set_metric(self, value):
        self.reports[self.current_fold].set_metric_value(value)
        self.reports[self.current_fold].finish()
        
    @property
    def html(self):
        return Column([Title('fitting')] + [Raw(report.html(active=(i in self.active), annotations=(i == 0))) for i, report in enumerate(self.reports)]).html

class FeatureImportances(HTMLRepr):
    def __init__(self, features: List[Dict[str, Union[str, 'FeatureConstructor', float]]], width: int = 600):
        self.features = features
        self.width = width
        self.max_importance = max([i['max'] for i in self.features])
        self.min_importance = min([i['min'] for i in self.features])
        self.zero_position = 0
        if self.min_importance < 0:
            self.zero_position = self.to_px(0)
        
    def to_px(self, value):
        return self.width * ((value - self.min_importance) / (self.max_importance - self.min_importance))
        
    @property
    def html(self):
        name_annotation = Annotation('feature', style="text-align: right; margin-bottom: 3px; margin-right: 5px;")
        mean_annotation = Annotation('mean', style="margin-left: 7px; margin-bottom: 3px;")
        hbar_annotation = Annotation('importance', style="margin-left: 5px; margin-bottom: 3px;")
        names = [i['feature_constructor'].html_collapsable(name=i['name'], style='padding: 0px 5px; text-align: right;', bg=False, border=True) for i in self.features]
        means = [Field(i['mean'], style='max-height: 1.5rem; margin: 2px;', bg=False).html for i in self.features]
        hbars = [Importance(self.to_px(i['mean']), self.to_px(i['min']), self.to_px(i['max']), self.zero_position).html for i in self.features]
        return AlignedColumns([
            [name_annotation.html] + names,
            [mean_annotation.html] + means,
            [hbar_annotation.html] + hbars,
        ], title='feature importances').html


class Leaderboard(HTMLRepr):
    """Needs refactoring, very sketchy"""
    def __init__(self, experiments):
        self.experiments = experiments
        self.col_widths = [1, 6, 5, 10, 6, 4, 8]
        self.col_names = ['#', 'id', 'score', 'model', '# features', "date", "took"]
        self.data = [
            (
                i, 
                e.id, 
                format_value(e.score), 
                e.model.__class__, 
                e.n_features,
                format_value(e.date, time=True), 
                format_value(e.took, time=True)
            )
            for i, e in enumerate(experiments)
        ]
    
    def head_style(self, i):
        return dict(bg=False, accent=False, bold=False, 
                    style=f"padding: 0px 5px; margin: 0px; width: {i}rem; border: 0px;")

    def cell_style(self, i):
        return dict(bg=False, style=f"padding: 0px 5px; margin: 0px; width: {i}rem; border: 0px;")

    def concat(self, row):
        return '\n'.join(cell.html if not isinstance(cell, str) else cell for cell in row)

    @property
    def html(self):
        head_cells = [Field(self.col_names[0], **self.head_style(self.col_widths[0]))]
        for i in range(1, len(self.col_widths)):
            head_cells.append(Field(self.col_names[i], **self.head_style(self.col_widths[i])))

        rows = [[Field(self.data[i][j],  **self.cell_style(col_widths[j]))
            for j in range(len(self.data[0]))
        ] for i in range(len(self.data))]
        rows = [Raw(e.html_collapsable(ThumbnailField(self.concat(rows[i]), cssid=-1, first=False), border=True)) for i, e in enumerate(self.experiments)]

        res = Column([Title('leaderboard'), Field(self.concat(head_cells), bg=False, bold=False, style="padding-bottom: 0px; margin: 0px 2px 0px 2px;")] + rows)
        return res.html


class SingleFoldReport(HTMLRepr):
    def __init__(self, value, total, took, eta, output=None):
        self.value = value
        self.total = total
        self.took = took
        self.eta = eta
        self.output = output

    @property
    def html(self):
        tmp = [Progress(self.value, self.total, **self.progress_formatting)]
        if self.output is not None:
            tmp += [Output(self.output)]
        return Row([
            InnerColumn(tmp), 
            Field(format_value(self.took, time=True), **self.indicator_formatting), 
            Field(format_value(self.eta, time=True), **self.indicator_formatting)]).html
    
    @property
    def indicator_formatting(self):
        return dict(bg=False, style=f"padding: 0px 5px; margin: 2px; width: 7rem;")
    
    @property
    def progress_formatting(self):
        return dict(style="margin-top: 3px; width: 300px;")


class FeatureComputingReport(HTMLRepr):
    def __init__(self, feature_constructors):
        self.entries = defaultdict(lambda: dict(value=0, total=0, took=None, eta=None))
        self.outputs = defaultdict(lambda: list())
        self.feature_constructors = feature_constructors

    @property
    def html(self):
        thumbnails = [i.html_collapsable(**self.thumbnail_formatting) for i in self.active_features]
        blocks = self.progress_blocks
        return AlignedColumns([
            [Annotation('feature', **self.thumbnail_annotation_formatting).html] + thumbnails,
            [Annotation('progress', **self.progress_annotation_formatting).html] + blocks
        ], title='computing features').html
    
    @property
    def active_features(self):
        return [self.feature_constructors[name] for name in self.active_feature_names]
    
    @property
    def active_feature_names(self):
        result = list()
        for key in self.entries.keys():
            name = key.function_name
            if name not in result:
                result.append(name)
        return result
    
    @property
    def progress_blocks(self):
        blocks = []
        for name in self.active_feature_names:
            entries = self.entries_by_name(name)
            single_fold_reports = []
            for k, v in entries.items():
                output = self.outputs.get(k, None)
                if output is not None:
                    output = self.format_output(output)
                sfr = SingleFoldReport(v['value'], v['total'], v['took'], v['eta'], output)
                single_fold_reports.append(sfr)
            blocks.append(InnerColumn(single_fold_reports).html)
        return blocks
                
    def format_output(self, output):
        return '\n'.join([f"[{self.format_timestamp(timestamp)}] {text}" for timestamp, text in output])
    
    def entries_by_name(self, name):
        return {key: value for key, value in self.entries.items() if key.function_name == name}
    
    @property
    def thumbnail_formatting(self):
        return dict(style='padding: 0px 5px; text-align: right;', bg=False)
    
    @property
    def thumbnail_annotation_formatting(self):
        return dict(style="text-align: right; margin-bottom: 3px; margin-right: 5px;")
    
    @property
    def progress_annotation_formatting(self):
        return dict(style="margin-bottom: 3px; margin-left: 5px;")
    
    def format_timestamp(self, timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S.%f")[:-3]
    
    def update(self, run_id, value, total, took=None, eta=None):
        if self.entries[run_id]['value'] <= value:
            entry = self.entries[run_id]
            entry['value'] = value
            entry['total'] = total
            if took is not None:
                entry['took'] = took
            if eta is not None:
                entry['eta'] = eta
    
    def update_text(self, run_id, text=None, timestamp=None):
        output = self.outputs[run_id]
        if (timestamp, text) not in output:
            output.append((timestamp, text))

# ========== end of block definition ==========

ct = CurrentTheme(Highlighter(), themes['blue'])

def init():
    update_handles(ct)

def update_handles(new: CurrentTheme):
    handles = cfg._theme_displays
    for h in handles:
        h.update(new)
    handles.append(display(new, display_id=True))

def set_highlighting(name: str):
    ct.hl.set_style(name)
    update_handles(ct)

def set_theme(name: str):
    if name in themes:
        ct.set_theme(themes[name])
        update_handles(ct)
    else:
        raise UserWarning(f"Theme should be one of [{', '.join(themes.keys())}]")

def set_animation(value: bool):
    if value:
        ct.theme.anim_height, ct.theme.anim_padding = "1.0s", "0.7s"
    else:
        ct.theme.anim_height, ct.theme.anim_padding = "0s", "0s"
