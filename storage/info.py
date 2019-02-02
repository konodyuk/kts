
class Info:
    def __init__(self):
        self.attributes = dict()
        for name in glob(config.info_path + '*'):
            self.attributes