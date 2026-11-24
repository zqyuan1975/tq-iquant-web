# -*- coding: utf-8 -*-
"""条件选股。

- ``field_dict``：字段字典与缺失/PIT 契约（A0-3，D3 窄打分集合）
- ``wide_db``：外部宽表库只读连接器（A0-2，一次性导入源语义）
- ``warehouse``：宽库 → 平台导入仓（D1，运行期不再读宽库）
- ``engine``：四池因子计算、门槛、打分与命中（A2）
"""

from .field_dict import (  # noqa: F401
    FIELD_SET_VERSION,
    SCORABLE_FIELDS,
    FieldSpec,
    UnregisteredField,
    assert_scorable,
    field_set_fingerprint,
    get_field,
    missing_mask,
    require_fields,
    validate_columns,
)
