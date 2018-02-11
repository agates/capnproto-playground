# THIS FILE HAS BEEN GENERATED AUTOMATICALLY BY capnpy
# do not edit by hand
# generated on 2018-02-11 11:55

from capnpy import ptr as _ptr
from capnpy.struct_ import Struct as _Struct
from capnpy.struct_ import check_tag as _check_tag
from capnpy.struct_ import undefined as _undefined
from capnpy.enum import enum as _enum, fill_enum as _fill_enum
from capnpy.enum import BaseEnum as _BaseEnum
from capnpy.type import Types as _Types
from capnpy.segment.builder import SegmentBuilder as _SegmentBuilder
from capnpy.list import List as _List
from capnpy.list import PrimitiveItemType as _PrimitiveItemType
from capnpy.list import BoolItemType as _BoolItemType
from capnpy.list import TextItemType as _TextItemType
from capnpy.list import StructItemType as _StructItemType
from capnpy.list import EnumItemType as _EnumItemType
from capnpy.list import VoidItemType as _VoidItemType
from capnpy.list import ListItemType as _ListItemType
from capnpy.util import text_repr as _text_repr
from capnpy.util import float32_repr as _float32_repr
from capnpy.util import float64_repr as _float64_repr
from capnpy.util import extend_module_maybe as _extend_module_maybe
from capnpy.util import check_version as _check_version
__capnpy_version__ = '0.4.3'
_check_version(__capnpy_version__)

#### FORWARD DECLARATIONS ####

class PhEvent(_Struct): pass
PhEvent.__name__ = 'PhEvent'


#### DEFINITIONS ####

@PhEvent.__extend__
class PhEvent(_Struct):
    __static_data_size__ = 1
    __static_ptrs_size__ = 2
    
    
    @property
    def ph(self):
        # no union check
        return self._read_str_data(0)
    
    def get_ph(self):
        return self._read_str_data(0, default_=b"")
    
    def has_ph(self):
        ptr = self._read_fast_ptr(0)
        return ptr != 0
    
    @property
    def timestamp(self):
        # no union check
        value = self._read_data(0, ord(b'Q'))
        if 0 != 0:
            value = value ^ 0
        return value
    
    @property
    def group_name(self):
        # no union check
        return self._read_str_text(8)
    
    def get_group_name(self):
        return self._read_str_text(8, default_=b"")
    
    def has_group_name(self):
        ptr = self._read_fast_ptr(8)
        return ptr != 0
    
    @staticmethod
    def __new(ph=None, timestamp=0, group_name=None):
        builder = _SegmentBuilder()
        pos = builder.allocate(24)
        builder.alloc_data(pos + 8, ph)
        builder.write_uint64(pos + 0, timestamp)
        builder.alloc_text(pos + 16, group_name)
        return builder.as_string()
    
    def __init__(self, ph=None, timestamp=0, group_name=None):
        _buf = PhEvent.__new(ph, timestamp, group_name)
        self._init_from_buffer(_buf, 0, 1, 2)
    
    def shortrepr(self):
        parts = []
        if self.has_ph(): parts.append("ph = %s" % _text_repr(self.get_ph()))
        parts.append("timestamp = %s" % self.timestamp)
        if self.has_group_name(): parts.append("group_name = %s" % _text_repr(self.get_group_name()))
        return "(%s)" % ", ".join(parts)

_PhEvent_list_item_type = _StructItemType(PhEvent)


_extend_module_maybe(globals(), modname=__name__)