# -*- coding: utf-8 -*-
"""条件选股（A0 阶段）。

本包只放「与具体数据源解耦」的骨架：
- ``field_dict``：字段字典与缺失/PIT 契约（A0-3）
- ``wide_db``：外部宽表库只读连接器（A0-2，一次性导入源语义）

打分引擎（A2）在 D3 字段集定稿前不在此实现。
"""

from .field_dict import (  # noqa: F401
    FIELD_SET_VERSION,
    FieldSpec,
    UnregisteredField,
    field_set_fingerprint,
    get_field,
    missing_mask,
    require_fields,
    validate_columns,
)
