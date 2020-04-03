from docstring_parser import parse, DocstringMeta

from kts.ui.components import Field, Column, AlignedColumns, Title, Code, Annotation


def parse_to_html(doc, title=None, signature=None):
    doc = parse(doc)
    elements = [Title(title or 'docs')]

    if signature:
        elements += [Annotation('signature'), Code(signature)]

    description = doc.short_description
    if doc.long_description:
        description += '\n' + doc.long_description
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
    # signatures are temporarily removed from docstrings
    # may later add them back, with annotations moved to argument descriptions
    # to make signature more compact
    # sig = inspect.signature(f)
    # sig = f"{f.__qualname__}{sig}"
    name = f"{f.__qualname__} docs"
    html = parse_to_html(f.__doc__, name, None)
    f._repr_html_ = lambda *a: html
    return f
