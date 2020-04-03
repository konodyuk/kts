import inspect

from docstring_parser import parse, DocstringMeta

from kts.ui.components import Field, Column, AlignedColumns, Title, Code, Annotation
from kts.util.misc import adaptivemethod


def parse_to_html(doc, title=None, signature=None):
    doc = parse(doc)
    elements = [Title(title or 'docs')]

    if signature:
        elements += [Annotation('signature'), Code(signature)]

    description = doc.short_description
    if doc.long_description:
        description += '\n\n' + doc.long_description
    elements += [Annotation('description'), Field(description, bg=False)]

    param_kw = dict(style="padding-bottom: 0px; padding-top: 0px; margin-bottom: 0px;")
    if doc.params:
        elements += [Annotation('params')]
        params = []
        descriptions = []
        for param in doc.params:
            params.append(Field(param.arg_name, bold=True, bg=False, accent=False, **param_kw).html)
            descriptions.append(Field(param.description, bold=True, bg=False, accent=True, **param_kw).html)

        container_kw = dict(style="auto auto; padding-left: 0px; padding-right: 0px; justify-content: start;")
        elements += [AlignedColumns([params, descriptions], bg=True, **container_kw)]

    if doc.returns:
        elements += [Annotation('returns')]
        elements += [Field(f"{doc.returns.description}", bg=False)]

    if doc.raises:
        elements += [Annotation('raises')]
        for exception in doc.raises:
            elements += [Field(f"{exception.type_name}: {exception.description}", bg=False)]

    examples = [i for i in doc.meta if isinstance(i, DocstringMeta) and i.args[0] == 'examples']
    if examples:
        elements += [Annotation('examples')]
        elements += [Code(examples[0].description)]

    return "<div class='kts'>" + Column(elements).html + "</div>"


def html_docstring(f):
    sig = inspect.signature(f)
    params = ', '.join(p.name for p in sig.parameters.values())
    sig = f"{f.__qualname__}({params})"
    name = f"{f.__qualname__} docs"
    html = parse_to_html(f.__doc__, name, sig)
    f._repr_html_ = lambda *a: html
    return f


class HTMLReprWithDocstring:
    @adaptivemethod
    def _repr_html_(self):
        if isinstance(self, type):
            if 'html_doc' in dir(self):
                html = self.html_doc
            elif self.__doc__:
                name = f"{self.__name__} docs"
                sig = inspect.signature(self.__init__)
                params = ', '.join(p.name for p in sig.parameters.values() if p.name != 'self')
                sig = f"{self.__name__}({params})"
                html = self.html_doc = parse_to_html(self.__doc__, name, sig)
        else:
            html = self.html
        return f'<div class="kts">{html}</div>'
