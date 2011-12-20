from ssbench.constants import *

# XXX -- really, you'd use subclasses for each size_str with a factory

SIZE_STRS = ['tiny', 'small', 'medium', 'large', 'huge']
ATTRS_BY_SIZE = {
    'tiny': dict(container='Picture', type_char='P', size=99 * 10**3), # 99 KB
    'small': dict(container='Audio', type_char='A', size=4.9 * 10**6), # 4.9 MB
    'medium': dict(container='Document', type_char='D', size=9.9 * 10**6), # 9.9 MB
    'large': dict(container='Video', type_char='V', size=101 * 10**6), # 101 MB
    'huge': dict(container='Application', type_char='L', size=1.1 * 10**9), # 1.1 GB
}

class ScenarioFile(object):
    """A file which may be stored in a Swift cluster as part of the execution of a
    benchmark scenario.  Properties include purpose (eg. "Stock" vs. "Population"),
    name, type (which implies size, container name, and part of the file's name),
    etc."""

    def __init__(self, purpose, size_str, i):
        """Initializes with a size string and an index (unique within a size string)
        
        :purpose: Either 'S' for Stock or 'P' for Population
        :size_str: A string like 'tiny', 'small', etc.
        :i: An index saying this is the 'i'th file of this size
        """

        if purpose not in ['S', 'P']:
            raise ValueError('Invalid purpose %r' % purpose)
        self._purpose = purpose

        if size_str not in SIZE_STRS:
            raise ValueError('Invalid size_str %r' % size_str)
        self._size_str = size_str

        self._i = i

    @property
    def container(self):
        return ATTRS_BY_SIZE[self._size_str]['container']

    @property
    def name(self):
        attrs = ATTRS_BY_SIZE[self._size_str]
        return '%s%s%06d' % (self._purpose, attrs['type_char'], self._i)

    @property
    def size(self):
        return ATTRS_BY_SIZE[self._size_str]['size']

        

