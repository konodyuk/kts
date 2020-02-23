from IPython.core.display import display

from kts.settings import cfg
from kts.ui.components import CurrentTheme
from kts.ui.highlighting import Highlighter
from kts.ui.theme import themes, default_highlightings

ct = CurrentTheme(Highlighter('tango'), themes['light'])


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


theme_names = ', '.join(themes.keys())

def set_theme(name: str = 'dark'):
    """One of: %s"""
    if name in themes:
        ct.set_theme(themes[name])
        ct.hl.set_style(default_highlightings[name])
        update_handles(ct)
    else:
        raise UserWarning(f"Theme should be one of [{theme_names}]")

set_theme.__doc__ %= theme_names


def set_animation(value: bool):
    if value:
        ct.theme.anim_height, ct.theme.anim_padding = "1.0s", "0.7s"
    else:
        ct.theme.anim_height, ct.theme.anim_padding = "0s", "0s"
