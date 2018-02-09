# THIS FILE HAS BEEN GENERATED AUTOMATICALLY BY capnpy
# do not edit by hand
# generated on 2018-02-08 23:38

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
    __static_data_size__ = 2
    __static_ptrs_size__ = 0
    
    
    @property
    def ph(self):
        # no union check
        value = self._read_data(0, ord(b'f'))
        if 0.0 != 0:
            value = value ^ 0.0
        return value
    
    @property
    def timestamp(self):
        # no union check
        value = self._read_data(8, ord(b'Q'))
        if 0 != 0:
            value = value ^ 0
        return value
    
    @staticmethod
    def __new(ph=0.0, timestamp=0):
        builder = _SegmentBuilder()
        pos = builder.allocate(16)
        builder.write_float32(pos + 0, ph)
        builder.write_uint64(pos + 8, timestamp)
        return builder.as_string()
    
    def __init__(self, ph=0.0, timestamp=0):
        _buf = PhEvent.__new(ph, timestamp)
        self._init_from_buffer(_buf, 0, 2, 0)
    
    def shortrepr(self):
        parts = []
        parts.append("ph = %s" % _float32_repr(self.ph))
        parts.append("timestamp = %s" % self.timestamp)
        return "(%s)" % ", ".join(parts)

_PhEvent_list_item_type = _StructItemType(PhEvent)


_extend_module_maybe(globals(), modname=__name__)