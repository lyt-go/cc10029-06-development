"""闭合抽取窗口明细导出包：版本化导出、并发更正、断点恢复与映射转换核对。

场景还原：窗口闭合并导出 8 行；有人重新下载时另一边同时更正其中 2 行；
打包中断可凭断点恢复；映射入口拒收无法完成校验的部分文件；字段契约凭
包内窗口身份、数据校验值和关联契约版本生成转换结果；更正前后各下载一次，
转换后核对改变的两行与其余六行，且原版本可查。
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from app.services.extract_window import ExtractWindowService
from app.services.field_contract import FieldContractService
from app.services.source_register import SourceRegisterService
from app.store import store


@pytest.fixture(autouse=True)
def cleanup():
    """每个测试前重置 store 数据，确保测试隔离。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _closed_window_with_rows(rows: int = 8) -> tuple[ExtractWindowService, dict]:
    """造一个已闭合、含 rows 行抽取数据的窗口（源端编号带随机串，跨运行不串数据）。"""
    source_code = f"SOUR-EXP-{uuid.uuid4().hex[:8]}"
    src_svc = SourceRegisterService()
    src_svc.register_source({
        "源端编号": source_code, "系统名称": "导出测试源端", "数据域": "订单域",
        "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "1万",
        "抽取方式": "增量抽取", "登记游标": "0",
    })
    ew_svc = ExtractWindowService()
    window = ew_svc.create_window(source_code, count=rows)
    extracted = ew_svc.execute_extract(window["id"], record_count=rows)
    assert extracted["records_extracted"] == rows
    closed = ew_svc.close_window(window["id"])
    assert closed["record_count"] == rows
    assert closed["status"] == "已闭合"
    return ew_svc, window


def _activate_contract() -> FieldContractService:
    """登记并生效一条 ext_a→dst_a 的字段契约（契约版本 v1.0）。"""
    fc_svc = FieldContractService()
    entry = fc_svc.create_contract("ext_a", "dst_a", "string")
    fc_svc.run_action(entry["id"], "确认生效")
    return fc_svc


def _valid_package_rows(package_path: str):
    """校验导出包并返回 (manifest, rows)，校验失败直接断言。"""
    ok, detail = ExtractWindowService.validate_export_package(package_path)
    assert ok, detail["errors"]
    return detail["manifest"], detail["rows"]


def _business_fields(rec: dict) -> dict:
    """剔除下划线开头的易变元字段，只留业务字段用于逐行比对。"""
    return {k: v for k, v in rec.items() if not k.startswith("_")}


CORRECTIONS = [
    {"line_no": 3, "updates": {"ext_a": "CORRECTED-3"}},
    {"line_no": 7, "updates": {"ext_a": "CORRECTED-7"}},
]
CHANGED_LINES = {3, 7}
UNCHANGED_LINES = {1, 2, 4, 5, 6, 8}


class TestClosedWindowExportPackage:
    """闭合窗口明细导出：窗口范围、数据与校验信息打包成抽取数据导出包。"""

    def test_export_package_contains_window_range_data_and_validation(self):
        """导出包含窗口身份、窗口范围、8 行数据与完整校验信息。"""
        _activate_contract()
        ew_svc, window = _closed_window_with_rows(8)

        result = ew_svc.export_window_detail(window["id"])
        assert result["valid"] is True
        assert result["record_count"] == 8
        assert result["data_version"] == 1
        assert result["contract_version"] == "v1.0"

        manifest, rows = _valid_package_rows(result["package_path"])
        # 窗口身份与窗口范围
        assert manifest["window_code"] == window["窗口编号"]
        assert manifest["window_id"] == window["id"]
        assert manifest["window_start"] == window["窗口起点"]
        assert manifest["window_end"] == window["窗口终点"]
        # 校验信息
        assert manifest["record_count"] == 8
        assert manifest["data_checksum"] == result["data_checksum"]
        assert len(manifest["row_checksums"]) == 8
        # 数据行行号 1..8 连续，与下载记录逐行对应
        assert [r["line_no"] for r in rows] == list(range(1, 9))

        # 未闭合窗口不允许导出
        draft = ew_svc.create_window(window["源端编号"], count=1)
        blocked = ew_svc.export_window_detail(draft["id"])
        assert "error" in blocked

    def test_redownload_after_correction_row_by_row(self):
        """更正前后各下载一次：两包均为 8 行，恰好 2 行变化、6 行一致。"""
        ew_svc, window = _closed_window_with_rows(8)

        before = ew_svc.export_window_detail(window["id"])  # 更正前下载
        assert before["valid"] is True

        corrected = ew_svc.correct_window_records(window["id"], CORRECTIONS)
        assert corrected["data_version"] == 2
        assert corrected["changed_lines"] == [3, 7]

        after = ew_svc.export_window_detail(window["id"])  # 更正后重新下载
        assert after["valid"] is True
        assert after["export_seq"] != before["export_seq"]
        assert after["data_version"] == 2
        assert after["data_checksum"] != before["data_checksum"]
        assert after["record_count"] == before["record_count"] == 8

        _, rows_before = _valid_package_rows(before["package_path"])
        _, rows_after = _valid_package_rows(after["package_path"])
        by_line_before = {r["line_no"]: r for r in rows_before}
        by_line_after = {r["line_no"]: r for r in rows_after}
        assert set(by_line_before) == set(by_line_after) == set(range(1, 9))
        for line_no in CHANGED_LINES:
            assert by_line_before[line_no]["data"] != by_line_after[line_no]["data"]
            assert by_line_before[line_no]["row_checksum"] != by_line_after[line_no]["row_checksum"]
        for line_no in UNCHANGED_LINES:
            assert by_line_before[line_no]["data"] == by_line_after[line_no]["data"]
            assert by_line_before[line_no]["row_checksum"] == by_line_after[line_no]["row_checksum"]


class TestResumablePackaging:
    """打包中断可恢复；部分文件无法通过校验，映射入口拒收。"""

    def test_interrupted_packaging_rejected_then_resumed(self):
        """中断留下部分文件→校验不过→映射拒收→断点恢复后封箱通过。"""
        _activate_contract()
        fc_svc = FieldContractService()
        ew_svc, window = _closed_window_with_rows(8)

        # 打包到第 3 行中断
        interrupted = ew_svc.export_window_detail(window["id"], chunk_size=3, fail_after=3)
        assert interrupted["interrupted"] is True
        assert interrupted["rows_written"] == 3
        partial_path = interrupted["package_path"]
        assert Path(partial_path).exists()

        # 部分文件无法完成校验
        ok, detail = ExtractWindowService.validate_export_package(partial_path)
        assert ok is False
        assert any("封箱" in e or "不完整" in e for e in detail["errors"])

        # 映射入口拒收部分文件，不产生转换产物
        rejected = fc_svc.transform_export_package(partial_path)
        assert "error" in rejected
        assert rejected["validation_errors"]

        # 断点恢复：续写剩余 5 行并封箱
        resumed = ew_svc.resume_export(window["id"], interrupted["export_seq"], chunk_size=3)
        assert resumed["valid"] is True
        assert resumed["record_count"] == 8
        _, rows = _valid_package_rows(resumed["package_path"])
        assert [r["line_no"] for r in rows] == list(range(1, 9))

        # 恢复后的包与一次性干净导出的包逐行一致（同一数据版本）
        clean = ew_svc.export_window_detail(window["id"], chunk_size=2)
        _, clean_rows = _valid_package_rows(clean["package_path"])
        assert resumed["data_checksum"] == clean["data_checksum"]
        assert [r["data"] for r in rows] == [r["data"] for r in clean_rows]

    def test_concurrent_correction_during_packaging_keeps_snapshot(self):
        """下载途中另一边更正 2 行：恢复后包内仍是下载开始时锁定的版本，逐行不串版。"""
        ew_svc, window = _closed_window_with_rows(8)

        # 重新下载开始，写到第 4 行时中断（锁定 v1 快照）
        interrupted = ew_svc.export_window_detail(window["id"], chunk_size=4, fail_after=4)
        assert interrupted["interrupted"] is True
        v1_checksum = ew_svc.get_window_data_version(window["id"], 1)["data_checksum"]

        # 另一边同时更正其中 2 行，生成 v2
        corrected = ew_svc.correct_window_records(window["id"], CORRECTIONS)
        assert corrected["data_version"] == 2

        # 断点恢复：仍按锁定的 v1 快照续写，与中断前已下载的 4 行逐行衔接
        resumed = ew_svc.resume_export(window["id"], interrupted["export_seq"], chunk_size=4)
        assert resumed["valid"] is True
        assert resumed["data_version"] == 1
        assert resumed["data_checksum"] == v1_checksum

        _, rows = _valid_package_rows(resumed["package_path"])
        v1_rows = ew_svc.get_window_data_version(window["id"], 1)["rows"]
        assert [r["data"] for r in rows] == v1_rows  # 送去映射的数据=实际下载的记录

        # 再次发起新导出才拿到更正后的 v2
        after = ew_svc.export_window_detail(window["id"])
        assert after["data_version"] == 2
        assert after["data_checksum"] == corrected["data_checksum"]

    def test_resume_rejects_tampered_partial_file(self):
        """部分文件与断点不一致时拒绝续写。"""
        ew_svc, window = _closed_window_with_rows(8)
        interrupted = ew_svc.export_window_detail(window["id"], chunk_size=4, fail_after=4)
        assert interrupted["interrupted"] is True

        # 篡改部分文件：追加一行不属于锁定快照的数据
        ew_svc._write_jsonl(Path(interrupted["package_path"]), [
            {"type": "row", "line_no": 5, "row_checksum": "bogus", "data": {"ext_a": "x"}},
        ])
        result = ew_svc.resume_export(window["id"], interrupted["export_seq"])
        assert "error" in result


class TestTransformExportPackage:
    """字段契约凭包内窗口身份、数据校验值和契约版本生成转换结果。"""

    def test_transform_before_and_after_correction(self):
        """两版包分别转换：恰好 2 行转换结果变化、其余 6 行一致，且行行对得上下载记录。"""
        fc_svc = _activate_contract()
        ew_svc, window = _closed_window_with_rows(8)

        before = ew_svc.export_window_detail(window["id"])
        ew_svc.correct_window_records(window["id"], CORRECTIONS)
        after = ew_svc.export_window_detail(window["id"])

        result_before = fc_svc.transform_export_package(before["package_path"])
        result_after = fc_svc.transform_export_package(after["package_path"])
        assert "error" not in result_before and "error" not in result_after

        # 转换结果凭 窗口身份+数据校验值+契约版本 标识
        assert result_before["result_key"] == (
            f"{window['窗口编号']}:{before['data_checksum']}:v1.0")
        assert result_after["result_key"] == (
            f"{window['窗口编号']}:{after['data_checksum']}:v1.0")
        assert result_before["result_key"] != result_after["result_key"]
        assert result_before["records_converted"] == result_after["records_converted"] == 8

        mapped_before = fc_svc._read_jsonl(Path(result_before["artifact_path"]))
        mapped_after = fc_svc._read_jsonl(Path(result_after["artifact_path"]))
        assert len(mapped_before) == len(mapped_after) == 8

        # 转换行与下载记录逐行对应：行号、行校验值回指导出包
        _, pkg_rows_after = _valid_package_rows(after["package_path"])
        for mapped, pkg_line in zip(mapped_after, pkg_rows_after):
            assert mapped["_line_no"] == pkg_line["line_no"]
            assert mapped["_row_checksum"] == pkg_line["row_checksum"]
            assert mapped["_contract_version"] == "v1.0"

        # 核对改变的两行与其余六行
        before_by_line = {r["_line_no"]: r for r in mapped_before}
        after_by_line = {r["_line_no"]: r for r in mapped_after}
        for line_no in CHANGED_LINES:
            assert _business_fields(before_by_line[line_no]) != _business_fields(after_by_line[line_no])
            assert after_by_line[line_no]["dst_a"] == f"CORRECTED-{line_no}"
        for line_no in UNCHANGED_LINES:
            assert _business_fields(before_by_line[line_no]) == _business_fields(after_by_line[line_no])

    def test_transform_is_idempotent_for_same_package(self):
        """同一包重跑转换：产物幂等重写，不重复追加。"""
        fc_svc = _activate_contract()
        ew_svc, window = _closed_window_with_rows(8)
        package = ew_svc.export_window_detail(window["id"])

        first = fc_svc.transform_export_package(package["package_path"])
        second = fc_svc.transform_export_package(package["package_path"])
        assert first["result_key"] == second["result_key"]
        mapped = fc_svc._read_jsonl(Path(second["artifact_path"]))
        assert len(mapped) == 8

    def test_original_version_lookup_after_correction(self):
        """更正后仍能查到原版本：历史导出包与窗口数据 v1 快照都可回查。"""
        fc_svc = _activate_contract()
        ew_svc, window = _closed_window_with_rows(8)

        before = ew_svc.export_window_detail(window["id"])
        ew_svc.correct_window_records(window["id"], CORRECTIONS)
        after = ew_svc.export_window_detail(window["id"])

        # 导出登记册里两个版本都在
        listing = ew_svc.list_export_packages(window["id"])
        seqs = {p["export_seq"] for p in listing["packages"]}
        assert seqs == {before["export_seq"], after["export_seq"]}

        # 原导出包可回查且仍通过完整性校验
        original = ew_svc.get_export_package(window["id"], before["export_seq"])
        assert original["valid"] is True
        assert original["meta"]["data_version"] == 1
        assert [r["line_no"] for r in original["rows"]] == list(range(1, 9))
        # 原版本里第 3、7 行还是更正前的值
        assert original["rows"][2]["data"]["ext_a"] != "CORRECTED-3"
        assert original["rows"][6]["data"]["ext_a"] != "CORRECTED-7"

        # 窗口数据 v1 快照可回查，与更正后 v2 恰好差 2 行
        v1 = ew_svc.get_window_data_version(window["id"], 1)
        v2 = ew_svc.get_window_data_version(window["id"], 2)
        assert v1["data_checksum"] != v2["data_checksum"]
        diffs = {i + 1 for i, (a, b) in enumerate(zip(v1["rows"], v2["rows"])) if a != b}
        assert diffs == CHANGED_LINES

        # 原版本的转换结果也可凭原包重新生成
        rerun = fc_svc.transform_export_package(original["meta"]["path"])
        assert "error" not in rerun
        assert rerun["data_checksum"] == before["data_checksum"]
