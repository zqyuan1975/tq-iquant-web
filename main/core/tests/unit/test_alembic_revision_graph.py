# -*- coding: utf-8 -*-
"""Alembic 迁移图自检：revision id 唯一、heads 唯一、父版本可达。

背景（2026-11-20）：`h5b6c7d8e9f0` 这个 revision id 先被一个临时迁移
（`h5b6c7d8e9f0_add_live_stop_audits.py`，未入库、已删除）用过，随后又被
`capital_shadow` 迁移复用。一旦旧版本先落过库，alembic 会把新迁移当成
「已应用」而**静默跳过**——列/表永远建不出来，且没有任何报错。

这里把三条硬约束变成可执行检查，任何提交只要破坏迁移图就会在测试阶段失败。
"""
import ast
from pathlib import Path

VERSIONS_DIR = Path(__file__).resolve().parents[3] / "alembic" / "versions"


def _literal(node):
    """只接受字符串 / None / 字符串元组这些迁移脚本里的字面量。"""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return None


def _scan_graph():
    """返回 (graph{revision: {down, file}}, duplicates[(rev, 旧文件, 新文件)])。"""
    graph = {}
    duplicates = []
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        # utf-8-sig：部分历史迁移脚本带 BOM，ast.parse 不接受 U+FEFF
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        revision = down_revision = None
        for node in tree.body:
            # 迁移脚本两种写法都有：revision = "..." 与 revision: str = "..."（AnnAssign）
            if isinstance(node, ast.Assign):
                targets = [t for t in node.targets if isinstance(t, ast.Name)]
                value = node.value
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                targets, value = [node.target], node.value
            else:
                continue
            for target in targets:
                if target.id == "revision":
                    revision = _literal(value)
                elif target.id == "down_revision":
                    down_revision = _literal(value)
        if revision is None:
            continue  # 非迁移脚本
        if revision in graph:
            duplicates.append((revision, graph[revision]["file"], path.name))
        graph[revision] = {"down": down_revision, "file": path.name}
    return graph, duplicates


def _downs(down):
    if down is None:
        return ()
    if isinstance(down, (tuple, list)):
        return tuple(down)
    return (down,)


def test_revision_ids_unique():
    """revision id 不得复用（复用会让后加入的迁移被静默跳过）。"""
    _, duplicates = _scan_graph()
    assert not duplicates, "发现重复 revision id：%s" % (duplicates,)


def test_single_head():
    """迁移链必须只有一个 head。"""
    graph, _ = _scan_graph()
    all_parents = {d for item in graph.values() for d in _downs(item["down"])}
    heads = sorted(set(graph) - all_parents)
    assert len(heads) == 1, "迁移链应只有一个 head，实际 %s" % (heads,)


def test_down_revisions_exist():
    """每个 down_revision 都必须能在 versions/ 里找到，断链会导致 upgrade 失败。"""
    graph, _ = _scan_graph()
    missing = sorted(
        "%s -> %s (%s)" % (rev, d, item["file"])
        for rev, item in graph.items()
        for d in _downs(item["down"])
        if d not in graph
    )
    assert not missing, "迁移链断链：%s" % missing
