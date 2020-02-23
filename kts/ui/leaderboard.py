from kts.ui.components import HTMLRepr, Column, Field, Title, ThumbnailField, Raw
from kts.util.formatting import format_value


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
