from abc import ABC


class GallicaObject(ABC):
    """Abstract class for gallica objects"""
    def __init__(self, ark):
        """A gallica object is identified by its ARK"""
        ark = ark.lstrip('ark:')
        ark = ark.strip('/')
        self.ark = ark
        self.authority = ark.split('/')[0]
        self.ark_name = ark.split('/')[1]

    def __repr__(self):
        return self.ark
