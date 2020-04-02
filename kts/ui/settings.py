from IPython.core.display import display

from kts.settings import cfg, ConfigError
from kts.ui.components import CurrentTheme
from kts.ui.highlighting import Highlighter
from kts.ui.theme import themes, default_highlightings
from kts.ui.dashboard import Dashboard


def init():
    try:
        cfg._theme = themes[cfg._theme_name]
    except:
        raise ConfigError(f"Invalid theme: {cfg._theme_name}")
    try:
        cfg._highlighter = Highlighter(cfg._highlighter_name)
    except:
        raise ConfigError(f"Invalid highlighting: {cfg._highlighter_name}")
    cfg._dashboard_handles.append(display(Dashboard(), display_id=True))


def update_dashboard():
    for handle in cfg._dashboard_handles:
        handle.update(Dashboard())


def update_handles():
    update_dashboard()
    if cfg._theme_handle is None:
        cfg._theme_handle = display(CurrentTheme(), display_id=True)
    else:
        cfg._theme_handle.display(CurrentTheme())


def set_highlighting(name: str):
    cfg._highlighter.set_style(name)
    cfg._highlighter_name = name
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
