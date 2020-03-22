from IPython.core.display import display

from kts.settings import cfg
from kts.ui.components import CurrentTheme
from kts.ui.highlighting import Highlighter
from kts.ui.theme import themes, default_highlightings
from kts.ui.dashboard import Dashboard


def init():
    cfg._highlighter = Highlighter('tango')
    cfg._theme = themes['light-orange']
    cfg._dashboard_handle = display(Dashboard(), display_id=True)


def update_dashboard():
    cfg._dashboard_handle.update(Dashboard())


def update_handles():
    update_dashboard()
    handles = cfg._theme_handles
    for h in handles:
        h.update(CurrentTheme())
    handles.append(display(CurrentTheme(), display_id=True))


def set_highlighting(name: str):
    cfg._highlighter.set_style(name)
    update_handles()


theme_names = ', '.join(themes.keys())

def set_theme(name: str = 'dark'):
    """One of: %s"""
    if name not in themes:
        raise UserWarning(f"Theme should be one of [{theme_names}]")
    cfg._theme = themes[name]
    cfg._theme_name = name
    cfg._highlighter.set_style(default_highlightings[name])
    update_handles()

set_theme.__doc__ %= theme_names


def set_animation(value: bool):
    if value:
        cfg._theme.anim_height, cfg._theme.anim_padding = "1.0s", "0.7s"
    else:
        cfg._theme.anim_height, cfg._theme.anim_padding = "0s", "0s"
    update_handles()
