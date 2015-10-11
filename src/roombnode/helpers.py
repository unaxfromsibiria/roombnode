'''
@author: Michael Vorotyntsev
@email: linkofwise@gmail.com
@github: unaxfromsibiria
'''


class MetaOnceObject(type):
    """
    Once object.
    """

    _classes = dict()

    def __call__(self, *args, **kwargs):
        cls = str(self)
        if cls not in self._classes:
            this = super().__call__(*args, **kwargs)
            self._classes[cls] = this
        else:
            this = self._classes[cls]
        return this
