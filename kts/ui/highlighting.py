from pygments import token as pt, highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name
from pygments.style import Style


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
        pt.Punctuation: 'bold #ffffff',
    }


custom_styles = {'kts': KTSStyle}


class Highlighter:
    def __init__(self, style='tango'):
        self.lexer = get_lexer_by_name("python")
        self.style = style
        self.set_style(style)

    def highlight(self, code):
        return highlight(code, self.lexer, self.formatter)

    @property
    def css(self):
        return f'<style>\n{self.formatter.get_style_defs([".kts .kts-code"])}\n</style>\n'

    def set_style(self, style):
        if style in custom_styles:
            style = custom_styles[style]
        self.formatter = HtmlFormatter(style=style, nowrap=True, nobackground=True)
        self.style = style

highlighter = Highlighter()
