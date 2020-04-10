from kts.ui.static import CSS_STYLE


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
    'dark': Theme("#2e3047", "#43455c", "#707793", "#000", "#3bba9c", "#f00"),
    'dark-neon': Theme("#392338", "#523957", "#ff947f", "#000", "#ffcfa4", "#ff0"),
    'light': Theme("#edf1fb", "#fafafa", "#ec4e3a", "#fff", "#000", "#ff0"),
    'light-blue': Theme("#edf1fb", "#f7f8fc", "#5e40d8aa", "#fff", "#000", "#ff0"),
    'light-orange': Theme("#edf1fb", "#f7f8fc", "#FF7500", "#fff", "#000", "#ff0"),
    'light-green': Theme("#edf1fb", "#f7f8fc", "#009900", "#fff", "#000", "#ff0"),
    'light-banana': Theme("#ffcb48", "#f7f8fc", "#000000", "#fff", "#000", "#fff"),
    'light-gray': Theme("#f7f7f8", "#fff", "#000000", "#fff", "#000", "#999"),
}

default_highlightings = {
    'dark': 'kts',
    'dark-neon': 'kts',
    'light': 'tango',
    'light-blue': 'tango',
    'light-orange': 'tango',
    'light-green': 'tango',
    'light-banana': 'tango',
    'light-gray': 'tango',
}
