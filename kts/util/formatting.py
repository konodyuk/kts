from time import strftime, localtime

import numpy as np


def format_value(value, time=False):
    if value is None:
        return ''
    if time and value < 1e9:
        value = int(value)
        days, r = divmod(value, 24 * 60 * 60)
        hours, r = divmod(r, 60 * 60)
        minutes, seconds = divmod(r, 60)
        res = []
        if days:
            res.append(f"{days}d")
        if hours:
            res.append(f"{hours}h")
        if minutes:
            res.append(f"{minutes}m")
        if seconds and not hours:
            res.append(f"{seconds}s")
        if not res:
            res = ['0s']
        return ' '.join(res)
    elif time:
        res = strftime("%d %b", localtime(value)).lower()
        if res[0] == '0':
            res = res[1:]
        return res
    else:
        if value < 1e-2:
            return np.format_float_scientific(value, 2)
        else:
            return np.format_float_positional(value, 3)