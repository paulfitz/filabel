from filabel.main import get_database, get_list

class FileLabel(object):
    def __init__(self, url=None):
        self.db = get_database(url)
        self.data = get_list(self.db)

    def get_split(self, name):
        return SplitLabel(self.data['splits'][name])

class SplitLabel(object):
    def __init__(self, data):
        self.data = data
        self.id2label = dict(enumerate(self.labels))
        self.label2id = dict([[y, x] for x, y in enumerate(self.labels)])

    @property
    def labels(self):
        return self.data['labels']

    def samplesForLabel(self, name):
        return self.data['samples'][self.label2id[name]]

