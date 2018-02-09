# THIS FILE HAS BEEN GENERATED AUTOMATICALLY BY capnpy
# do not edit by hand
# generated on 2018-02-09 01:33

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

class StructHandlerInfo(_Struct): pass
StructHandlerInfo.__name__ = 'StructHandlerInfo'


#### DEFINITIONS ####

@StructHandlerInfo.__extend__
class StructHandlerInfo(_Struct):
    __static_data_size__ = 0
    __static_ptrs_size__ = 2
    
    
    @property
    def struct_name(self):
        # no union check
        return self._read_str_text(0)
    
    def get_struct_name(self):
        return self._read_str_text(0, default_=b"")
    
    def has_struct_name(self):
        ptr = self._read_fast_ptr(0)
        return ptr != 0
    
    @property
    def handlers(self):
        # no union check
        return self._read_list(8, _TextItemType(_Types.text))
    
    def get_handlers(self):
        res = self.handlers
        if res is None:
            return _List.from_buffer(b'', 0, 0, 0, _TextItemType(_Types.text))
        return res
    
    def has_handlers(self):
        ptr = self._read_fast_ptr(8)
        return ptr != 0
    
    @staticmethod
    def __new(struct_name=None, handlers=None):
        builder = _SegmentBuilder()
        pos = builder.allocate(16)
        builder.alloc_text(pos + 0, struct_name)
        builder.copy_from_list(pos + 8, _TextItemType(_Types.text), handlers)
        return builder.as_string()
    
    def __init__(self, struct_name=None, handlers=None):
        _buf = StructHandlerInfo.__new(struct_name, handlers)
        self._init_from_buffer(_buf, 0, 0, 2)
    
    def shortrepr(self):
        parts = []
        if self.has_struct_name(): parts.append("struct_name = %s" % _text_repr(self.get_struct_name()))
        if self.has_handlers(): parts.append("handlers = %s" % self.get_handlers().shortrepr())
        return "(%s)" % ", ".join(parts)

_StructHandlerInfo_list_item_type = _StructItemType(StructHandlerInfo)


_extend_module_maybe(globals(), modname=__name__)