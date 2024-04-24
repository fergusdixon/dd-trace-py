# #!/usr/bin/env python3
# flake8: noqa
from typing import TYPE_CHECKING

from ..._constants import IAST
from .._metrics import _set_iast_error_metric
from .._metrics import _set_metric_iast_executed_source
from .._utils import _is_python_version_supported

if _is_python_version_supported():
    from .. import oce
    from ._native import ops
    from ._native.aspect_helpers import _convert_escaped_text_to_tainted_text
    from ._native.aspect_helpers import as_formatted_evidence
    from ._native.aspect_helpers import common_replace
    from ._native.aspect_format import _format_aspect
    from ._native.aspect_helpers import parse_params
    from ._native.initializer import active_map_addreses_size
    from ._native.initializer import create_context
    from ._native.initializer import debug_taint_map
    from ._native.initializer import destroy_context
    from ._native.initializer import initializer_size
    from ._native.initializer import num_objects_tainted
    from ._native.initializer import reset_context
    from ._native.taint_tracking import OriginType
    from ._native.taint_tracking import Source
    from ._native.taint_tracking import TagMappingMode
    from ._native.taint_tracking import are_all_text_all_ranges
    from ._native.taint_tracking import get_range_by_hash
    from ._native.taint_tracking import get_ranges
    from ._native.taint_tracking import is_notinterned_notfasttainted_unicode
    from ._native.taint_tracking import is_tainted
    from ._native.taint_tracking import origin_to_str
    from ._native.taint_tracking import set_fast_tainted_if_notinterned_unicode
    from ._native.taint_tracking import set_ranges
    from ._native.taint_tracking import copy_ranges_from_strings
    from ._native.taint_tracking import copy_and_shift_ranges_from_strings
    from ._native.taint_tracking import shift_taint_range
    from ._native.taint_tracking import shift_taint_ranges
    from ._native.taint_tracking import str_to_origin
    from ._native.taint_tracking import taint_range as TaintRange

    new_pyobject_id = ops.new_pyobject_id
    set_ranges_from_values = ops.set_ranges_from_values
    is_pyobject_tainted = is_tainted

if TYPE_CHECKING:
    from typing import Any
    from typing import Dict
    from typing import List
    from typing import Tuple
    from typing import Union


__all__ = [
    "_convert_escaped_text_to_tainted_text",
    "new_pyobject_id",
    "setup",
    "Source",
    "OriginType",
    "TagMappingMode",
    "TaintRange",
    "get_ranges",
    "set_ranges",
    "copy_ranges_from_strings",
    "copy_and_shift_ranges_from_strings",
    "are_all_text_all_ranges",
    "shift_taint_range",
    "shift_taint_ranges",
    "get_range_by_hash",
    "is_notinterned_notfasttainted_unicode",
    "set_fast_tainted_if_notinterned_unicode",
    "aspect_helpers",
    "reset_context",
    "destroy_context",
    "initializer_size",
    "active_map_addreses_size",
    "create_context",
    "str_to_origin",
    "origin_to_str",
    "common_replace",
    "_format_aspect",
    "as_formatted_evidence",
    "parse_params",
    "num_objects_tainted",
    "debug_taint_map",
]


def taint_pyobject(pyobject, source_name, source_value, source_origin=None):
    # type: (Any, Any, Any, OriginType) -> Any

    # Pyobject must be Text with len > 1
    if not pyobject or not isinstance(pyobject, IAST.TEXT_TYPES):
        return pyobject

    if isinstance(source_name, (bytes, bytearray)):
        source_name = str(source_name, encoding="utf8", errors="ignore")
    if isinstance(source_name, OriginType):
        source_name = origin_to_str(source_name)

    if isinstance(source_value, (bytes, bytearray)):
        source_value = str(source_value, encoding="utf8", errors="ignore")
    if source_origin is None:
        source_origin = OriginType.PARAMETER

    try:
        pyobject_newid = set_ranges_from_values(pyobject, len(pyobject), source_name, source_value, source_origin)
        _set_metric_iast_executed_source(source_origin)
        return pyobject_newid
    except ValueError as e:
        _set_iast_error_metric("Tainting object error (pyobject type %s): %s" % (type(pyobject), e))
    return pyobject


def taint_pyobject_with_ranges(pyobject, ranges):  # type: (Any, tuple) -> None
    set_ranges(pyobject, tuple(ranges))


def get_tainted_ranges(pyobject):  # type: (Any) -> tuple
    return get_ranges(pyobject)


def taint_ranges_as_evidence_info(pyobject):
    # type: (Any) -> Tuple[List[Dict[str, Union[Any, int]]], list[Source]]
    value_parts = []
    sources = []
    current_pos = 0
    tainted_ranges = get_tainted_ranges(pyobject)
    if not len(tainted_ranges):
        return ([{"value": pyobject}], [])

    for _range in tainted_ranges:
        if _range.start > current_pos:
            value_parts.append({"value": pyobject[current_pos : _range.start]})

        if _range.source not in sources:
            sources.append(_range.source)

        value_parts.append(
            {"value": pyobject[_range.start : _range.start + _range.length], "source": sources.index(_range.source)}
        )
        current_pos = _range.start + _range.length

    if current_pos < len(pyobject):
        value_parts.append({"value": pyobject[current_pos:]})

    return value_parts, sources
