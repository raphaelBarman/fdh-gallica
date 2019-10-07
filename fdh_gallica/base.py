class GallicaObject(object):
    def __init__(self, ark):
        ark = ark.strip('/')
        self.ark = ark
        self.authority = ark.split('/')[0]
        self.ark_name = ark.split('/')[1]

    def __repr__(self):
        return self.ark
