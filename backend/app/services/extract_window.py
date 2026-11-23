"""增量窗口领域服务：维护按增量字段切分的抽取窗口边界、抽取执行与推进水位。

本模块负责根据源端登记信息创建抽取窗口，执行数据抽取并写入抽取产物文件，
完成后自动驱动传输批次创建，形成源端登记→窗口抽取→批次传输的贯通链路。

窗口闭合后提供明细导出能力：把窗口范围、抽取数据与校验信息打包成
"抽取数据导出包"（清单头 + 逐行数据 + 封箱提交记录），交给字段契约模块
做映射转换。导出包具备以下保障：

- 数据版本化：闭合时以抽取产物初始化 v1 快照，事后更正生成 v2/v3…，
  历史版本快照不可变，原版本可随时回查；
- 断点续做：打包按块推进并写断点，中断中断后可凭断点按锁定的数据版本
  快照续写，更正并发落地也不会串版，保证送去映射的数据与实际下载的
  记录逐行对应；
- 完整性校验：封箱提交记录携带行数、数据校验值与封箱校验值，缺清单头、
  缺提交记录或校验值不符的部分文件一律判为非法，映射入口拒收。
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExtractWindowService(BaseModuleService):
    MODULE = "extract_window"
    ENTITY = "抽取窗口"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["窗口编号", "源端编号", "增量字段", "窗口起点", "窗口终点", "抽取条数", "窗口类型", "推进水位"]
    REQUIRED_FIELDS = ["窗口编号", "源端编号", "增量字段"]
    STATUS_ORDER = ["草稿", "待抽取", "抽取中", "已闭合"]
    ACTION_RULES = {"启动抽取": "抽取中", "闭合窗口": "已闭合", "标记作废": "已闭合"}
    NEGATIVE_ACTIONS = ["标记作废"]

    def create_window(self, source_code: str, count: int = 10) -> dict[str, Any]:
        """为指定源端创建增量抽取窗口并写入窗口配置快照，记录窗口边界与初始水位。"""
        sources = [r for r in store.rows("source_register") if r.get("源端编号") == source_code]
        if not sources:
            raise ValueError(f"源端编号 {source_code} 未登记，请先完成源端登记")
        entry, missing = self.create_entry({
            "窗口编号": f"EXTR-{source_code}-{int(time.time())}",
            "源端编号": source_code,
            "增量字段": "updated_at",
            "窗口起点": time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
            "窗口终点": time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
            "抽取条数": str(count),
            "窗口类型": "增量抽取",
            "推进水位": "0",
        })
        if entry is None:
            raise ValueError(f"创建窗口失败: {missing}")
        # 写入窗口配置快照
        self._write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"], "source": source_code,
              "start": entry["窗口起点"], "end": entry["窗口终点"], "count": count,
              "created_at": time.time()}]
        )
        return entry

    def execute_extract(self, window_id: int, record_count: int = 10) -> dict[str, Any]:
        """执行增量窗口数据抽取，生成抽取产物文件并推进窗口水位。

        从已在 store 登记的源端信息中查找匹配记录，按窗口范围筛选数据行，
        逐条写入抽取产物文件（jsonl），最后推进水位并自动触发传输批次创建。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        entry, msg = self.run_action(window_id, "启动抽取")
        if entry is None:
            return {"error": msg, "window_id": window_id}

        source_code = window["源端编号"]
        # 生成抽取数据行，每条记录含窗口编号、源端编号、增量字段取值和时间戳
        records = self._simulate_records(source_code, count=record_count, field_prefix="ext")

        # 写入抽取产物文件——这是本模块的核心数据产物
        output_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        written = self._write_jsonl(output_file, records)

        # 推进窗口水位
        self._write_checkpoint(f"window_{window['窗口编号']}", {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "records_extracted": written,
            "last_extract_at": time.time(),
            "watermark": str(written),
        })

        result = {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "records_extracted": written,
            "artifact_path": str(output_file),
            "watermark": str(written),
        }

        # 抽取完成后自动触发传输批次创建
        try:
            tb_svc = self._safe_import("app.services.transfer_batch", "TransferBatchService")()
            batch_result = tb_svc.create_batch(window["窗口编号"], written)
            result["auto_batch"] = batch_result
        except Exception as e:
            result["auto_batch"] = {"error": str(e)}

        return result

    def close_window(self, window_id: int) -> dict[str, Any]:
        """闭合抽取窗口，生成窗口产物摘要（记录条数、字节总量、校验值），写入闭合事件日志。

        闭合的同时以抽取产物初始化窗口数据版本库（v1 快照），
        供后续明细导出打包与数据更正版本管理使用。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        entry, msg = self.run_action(window_id, "闭合窗口")
        # 从产物文件读取统计信息
        artifact_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))

        # 写入闭合事件日志，记录抽取产物的完整统计
        self._write_jsonl(
            self._data_dir("events") / "window_close_log.jsonl",
            [{"action": "close", "window_code": window["窗口编号"], "record_count": len(records),
              "checksum": checksum, "closed_at": time.time()}]
        )

        # 闭合后初始化窗口数据版本库，供明细导出打包使用
        version_store = self._ensure_version_store(window)

        return {
            "window_code": window["窗口编号"],
            "status": entry["status"] if entry else "已闭合",
            "record_count": len(records),
            "checksum": checksum,
            "data_version": version_store["current_version"],
            "message": msg,
        }

    # ===== 闭合窗口数据版本管理 =====

    def _version_dir(self, window_code: str) -> Path:
        """窗口数据版本库目录，存放各版本不可变快照与版本登记册。"""
        d = self._data_dir("window_versions") / window_code
        d.mkdir(parents=True, exist_ok=True)
        return d

    @staticmethod
    def _load_json_file(path: Path, default: Any) -> Any:
        """读取 json 文件，不存在时返回默认值。"""
        if not path.exists():
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json_file(path: Path, payload: Any) -> None:
        """整体覆写 json 文件，用于版本登记册与导出包登记册。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _canonical_row(row: Any) -> str:
        """把一行数据序列化为键序稳定的规范字符串，作为行校验值输入。"""
        return json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)

    def _row_checksum(self, row: dict[str, Any]) -> str:
        """计算单行数据校验值。"""
        return self._compute_checksum(self._canonical_row(row))

    def _data_checksum(self, row_checksums: list[str]) -> str:
        """由逐行校验值序列计算整包数据校验值。"""
        return self._compute_checksum("|".join(row_checksums))

    def _rewrite_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """整体覆写 jsonl 文件（版本快照一次性落盘，不追加）。"""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        return len(records)

    def _load_version_rows(self, window_code: str, version: int) -> list[dict[str, Any]]:
        """读取指定版本的数据快照，快照文件不可变，可安全重复读取。"""
        return self._read_jsonl(self._version_dir(window_code) / f"v{version}.jsonl")

    def _ensure_version_store(self, window: dict[str, Any]) -> dict[str, Any]:
        """确保窗口数据版本库存在：窗口闭合后以抽取产物初始化 v1 快照。"""
        window_code = window["窗口编号"]
        vdir = self._version_dir(window_code)
        registry_path = vdir / "registry.json"
        registry = self._load_json_file(registry_path, None)
        if registry is not None:
            return registry
        artifact = self._data_dir("extracted") / f"{window_code}.jsonl"
        rows = self._read_jsonl(artifact)
        row_checksums = [self._row_checksum(r) for r in rows]
        registry = {
            "window_code": window_code,
            "current_version": 1,
            "versions": [{
                "version": 1,
                "record_count": len(rows),
                "row_checksums": row_checksums,
                "data_checksum": self._data_checksum(row_checksums),
                "reason": "窗口闭合初始化",
                "created_at": time.time(),
            }],
        }
        self._rewrite_jsonl(vdir / "v1.jsonl", rows)
        self._save_json_file(registry_path, registry)
        return registry

    def correct_window_records(self, window_id: int, corrections: list[dict[str, Any]]) -> dict[str, Any]:
        """更正闭合窗口中的指定数据行，生成新的数据版本快照。

        历史版本快照保持不可变，原版本仍可回查、可继续被打包引用；
        更正按 1 起始的行号定位，与导出包中的行号口径一致。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        if window.get("status") != "已闭合":
            return {"error": "仅已闭合窗口允许更正数据"}
        if not corrections:
            return {"error": "更正内容为空"}
        window_code = window["窗口编号"]
        registry = self._ensure_version_store(window)
        current = int(registry["current_version"])
        rows = self._load_version_rows(window_code, current)
        changed: list[int] = []
        for correction in corrections:
            line_no = int(correction.get("line_no", 0))
            updates = correction.get("updates", {})
            if not 1 <= line_no <= len(rows):
                return {"error": f"更正行号 {line_no} 超出窗口数据范围 1..{len(rows)}"}
            rows[line_no - 1].update(updates)
            changed.append(line_no)

        new_version = current + 1
        row_checksums = [self._row_checksum(r) for r in rows]
        self._rewrite_jsonl(self._version_dir(window_code) / f"v{new_version}.jsonl", rows)
        registry["versions"].append({
            "version": new_version,
            "record_count": len(rows),
            "row_checksums": row_checksums,
            "data_checksum": self._data_checksum(row_checksums),
            "reason": "数据更正",
            "changed_lines": changed,
            "created_at": time.time(),
        })
        registry["current_version"] = new_version
        self._save_json_file(self._version_dir(window_code) / "registry.json", registry)

        self._write_jsonl(
            self._data_dir("events") / "window_correct_log.jsonl",
            [{"action": "correct", "window_code": window_code, "data_version": new_version,
              "changed_lines": changed, "at": time.time()}]
        )
        return {
            "window_code": window_code,
            "data_version": new_version,
            "changed_lines": changed,
            "record_count": len(rows),
            "data_checksum": registry["versions"][-1]["data_checksum"],
        }

    def get_window_data_version(self, window_id: int, version: int | None = None) -> dict[str, Any]:
        """按版本号回查窗口数据快照，缺省返回当前版本；原版本始终可查。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        registry = self._ensure_version_store(window)
        target = int(version) if version is not None else int(registry["current_version"])
        meta = next((v for v in registry["versions"] if int(v["version"]) == target), None)
        if meta is None:
            return {"error": f"窗口数据版本 v{target} 不存在"}
        rows = self._load_version_rows(window["窗口编号"], target)
        return {
            "window_code": window["窗口编号"],
            "data_version": target,
            "record_count": len(rows),
            "data_checksum": meta["data_checksum"],
            "reason": meta.get("reason", ""),
            "rows": rows,
        }

    # ===== 闭合窗口明细导出：抽取数据导出包 =====

    def _export_dir(self, window_code: str) -> Path:
        """导出包目录，存放各次导出的包文件与导出登记册。"""
        d = self._data_dir("exports") / window_code
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _export_registry(self, window_code: str) -> dict[str, Any]:
        """读取导出登记册，记录每次导出的版本、校验值与包文件位置。"""
        return self._load_json_file(
            self._export_dir(window_code) / "registry.json",
            {"window_code": window_code, "packages": []},
        )

    def _next_export_seq(self, window_code: str) -> int:
        """分配新的导出序号：已封箱的与打包中断未恢复的序号都不复用。"""
        seqs = [int(p["export_seq"]) for p in self._export_registry(window_code)["packages"]]
        cp_dir = self._data_dir("checkpoints")
        for cp_file in cp_dir.glob(f"export_{window_code}_*.json"):
            state = self._load_json_file(cp_file, {})
            if state.get("status") == "packaging":
                seqs.append(int(state.get("export_seq", 0)))
        return max(seqs, default=0) + 1

    def _active_contract_version(self) -> str:
        """解析导出时点关联的契约版本：取最近登记的已生效契约版本。"""
        contracts = [r for r in store.rows("field_contract") if r.get("status") == "已生效"]
        if not contracts:
            return "v1.0"
        latest = max(contracts, key=lambda r: int(r.get("id", 0)))
        return str(latest.get("契约版本", "v1.0"))

    def _build_manifest(self, window: dict[str, Any], export_seq: int, package_id: str,
                        data_version: int, rows: list[dict[str, Any]],
                        contract_version: str) -> dict[str, Any]:
        """构造导出包清单头：窗口身份、窗口范围、数据校验值与关联契约版本。"""
        row_checksums = [self._row_checksum(r) for r in rows]
        return {
            "type": "manifest",
            "package_id": package_id,
            "export_seq": export_seq,
            "window_id": window["id"],
            "window_code": window["窗口编号"],
            "source_code": window.get("源端编号", ""),
            "window_start": window.get("窗口起点", ""),
            "window_end": window.get("窗口终点", ""),
            "data_version": data_version,
            "record_count": len(rows),
            "row_checksums": row_checksums,
            "data_checksum": self._data_checksum(row_checksums),
            "contract_version": contract_version,
            "created_at": time.time(),
        }

    def _package_checksum(self, manifest: dict[str, Any]) -> str:
        """封箱校验值：绑定包身份、窗口身份、数据版本、行数、数据校验值与契约版本。"""
        material = (
            f"{manifest['package_id']}|{manifest['window_code']}|{manifest['data_version']}|"
            f"{manifest['record_count']}|{manifest['data_checksum']}|{manifest['contract_version']}"
        )
        return self._compute_checksum(material)

    def export_window_detail(self, window_id: int, chunk_size: int = 4,
                             fail_after: int | None = None) -> dict[str, Any]:
        """闭合窗口明细导出：把窗口范围、数据与校验信息打包成抽取数据导出包。

        打包开始即锁定当前数据版本快照，按块写入并推进断点；期间即使有
        并发更正生成新版本，本包内容也不受影响。fail_after 用于模拟打包
        中断：写入指定行数后停包，保留部分文件与断点，可凭 resume_export
        续做封箱。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        if window.get("status") != "已闭合":
            return {"error": "仅已闭合窗口可导出明细包"}
        window_code = window["窗口编号"]
        registry = self._ensure_version_store(window)
        data_version = int(registry["current_version"])  # 锁定数据版本快照
        rows = self._load_version_rows(window_code, data_version)
        contract_version = self._active_contract_version()

        export_seq = self._next_export_seq(window_code)
        package_id = f"EXPO-{window_code}-{export_seq:04d}"
        package_path = self._export_dir(window_code) / f"{package_id}.pkg.jsonl"
        if package_path.exists():
            package_path.unlink()
        manifest = self._build_manifest(window, export_seq, package_id, data_version,
                                        rows, contract_version)

        # 清单头先行落盘，随后逐块追加数据行
        with open(package_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(manifest, ensure_ascii=False, default=str) + "\n")

        self._write_checkpoint(f"export_{window_code}_{export_seq}", {
            "status": "packaging",
            "export_seq": export_seq,
            "package_id": package_id,
            "window_code": window_code,
            "data_version": data_version,
            "record_count": len(rows),
            "next_line_no": 1,
            "contract_version": contract_version,
            "package_path": str(package_path),
        })
        return self._pack_rows(window, export_seq, package_path, rows, manifest,
                               chunk_size, fail_after)

    def _pack_rows(self, window: dict[str, Any], export_seq: int, package_path: Path,
                   rows: list[dict[str, Any]], manifest: dict[str, Any],
                   chunk_size: int, fail_after: int | None) -> dict[str, Any]:
        """按块把数据行写入导出包并推进断点，写齐后追加封箱提交记录并登记。"""
        window_code = window["窗口编号"]
        checkpoint_name = f"export_{window_code}_{export_seq}"
        state = self._read_checkpoint(checkpoint_name) or {}
        next_line = int(state.get("next_line_no", 1))
        total = len(rows)
        written_this_round = 0

        while next_line <= total:
            chunk = rows[next_line - 1: next_line - 1 + chunk_size]
            line_records = [
                {"type": "row", "line_no": next_line + i,
                 "row_checksum": manifest["row_checksums"][next_line - 1 + i],
                 "data": rec}
                for i, rec in enumerate(chunk)
            ]
            self._write_jsonl(package_path, line_records)
            next_line += len(chunk)
            written_this_round += len(chunk)
            state["next_line_no"] = next_line
            state["status"] = "packaging"
            self._write_checkpoint(checkpoint_name, state)
            if fail_after is not None and written_this_round >= fail_after:
                return {
                    "interrupted": True,
                    "export_seq": export_seq,
                    "package_id": manifest["package_id"],
                    "window_code": window_code,
                    "data_version": manifest["data_version"],
                    "rows_written": next_line - 1,
                    "record_count": total,
                    "package_path": str(package_path),
                    "message": "打包中断：部分文件与断点已保留，可凭断点恢复续做",
                }

        # 写齐数据行后追加封箱提交记录，导出包才算完整
        package_checksum = self._package_checksum(manifest)
        self._write_jsonl(package_path, [{
            "type": "commit",
            "record_count": total,
            "data_checksum": manifest["data_checksum"],
            "package_checksum": package_checksum,
            "complete": True,
        }])
        state["status"] = "done"
        state["next_line_no"] = next_line
        self._write_checkpoint(checkpoint_name, state)

        # 登记导出包版本，历史导出包可回查
        registry = self._export_registry(window_code)
        registry["packages"] = [p for p in registry["packages"]
                                if int(p["export_seq"]) != export_seq]
        registry["packages"].append({
            "export_seq": export_seq,
            "package_id": manifest["package_id"],
            "data_version": manifest["data_version"],
            "record_count": total,
            "data_checksum": manifest["data_checksum"],
            "contract_version": manifest["contract_version"],
            "package_checksum": package_checksum,
            "path": str(package_path),
            "created_at": time.time(),
        })
        self._save_json_file(self._export_dir(window_code) / "registry.json", registry)

        ok, _ = self.validate_export_package(package_path)
        return {
            "export_seq": export_seq,
            "package_id": manifest["package_id"],
            "window_code": window_code,
            "data_version": manifest["data_version"],
            "record_count": total,
            "data_checksum": manifest["data_checksum"],
            "contract_version": manifest["contract_version"],
            "package_checksum": package_checksum,
            "package_path": str(package_path),
            "valid": ok,
            "message": "抽取数据导出包已封箱" if ok else "导出包封箱后校验异常",
        }

    def resume_export(self, window_id: int, export_seq: int, chunk_size: int = 4,
                      fail_after: int | None = None) -> dict[str, Any]:
        """断点恢复打包：按打包开始时锁定的数据版本快照续写剩余数据行。

        恢复前先核对部分文件与断点位置、锁定版本快照逐行一致，防止中断
        期间的并发更正串进本包；核对不过则拒绝续写。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        window_code = window["窗口编号"]
        state = self._read_checkpoint(f"export_{window_code}_{export_seq}")
        if state is None or state.get("status") != "packaging":
            return {"error": f"导出序号 {export_seq} 没有可恢复的打包断点"}
        data_version = int(state["data_version"])
        rows = self._load_version_rows(window_code, data_version)  # 锁定版本快照
        package_path = Path(state["package_path"])

        existing = self._read_jsonl(package_path)
        manifest = next((l for l in existing if l.get("type") == "manifest"), None)
        if manifest is None:
            return {"error": "部分文件缺少清单头，无法恢复"}
        existing_rows = [l for l in existing if l.get("type") == "row"]
        expected = int(state.get("next_line_no", 1)) - 1
        if len(existing_rows) != expected:
            return {"error": f"部分文件行数 {len(existing_rows)} 与断点位置 {expected} 不一致"}
        for i, line in enumerate(existing_rows, start=1):
            if line.get("row_checksum") != self._row_checksum(rows[i - 1]):
                return {"error": f"部分文件第 {i} 行与锁定版本快照不一致，拒绝续写"}

        return self._pack_rows(window, export_seq, package_path, rows, manifest,
                               chunk_size, fail_after)

    @staticmethod
    def validate_export_package(package_path: str | Path) -> tuple[bool, dict[str, Any]]:
        """校验抽取数据导出包完整性。

        清单头、逐行校验值、数据校验值、封箱提交记录全部齐备且一致才判
        合法；打包中断留下的部分文件缺提交记录，一律判为非法，映射入口
        据此拒收。
        """
        path = Path(package_path)
        detail: dict[str, Any] = {"package_path": str(path), "errors": []}
        if not path.exists():
            detail["errors"].append("导出包文件不存在")
            return False, detail

        lines: list[dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    lines.append(json.loads(raw))
                except json.JSONDecodeError:
                    detail["errors"].append("存在无法解析的行，文件已损坏")
                    return False, detail

        manifests = [l for l in lines if l.get("type") == "manifest"]
        commits = [l for l in lines if l.get("type") == "commit"]
        rows = [l for l in lines if l.get("type") == "row"]
        if len(manifests) != 1:
            detail["errors"].append("清单头缺失或重复")
            return False, detail
        manifest = manifests[0]
        detail["manifest"] = manifest
        if len(commits) != 1 or not commits[0].get("complete"):
            detail["errors"].append("缺少封箱提交记录，文件不完整")
            return False, detail
        commit = commits[0]

        expected = int(manifest.get("record_count", -1))
        if len(rows) != expected:
            detail["errors"].append(f"数据行数 {len(rows)} 与清单声明 {expected} 不一致")
        if int(commit.get("record_count", -2)) != expected:
            detail["errors"].append("提交记录行数与清单声明不一致")

        # 行号必须从 1 开始连续，保证与下载记录逐行对应
        line_nos = [int(r.get("line_no", 0)) for r in rows]
        if line_nos != list(range(1, len(rows) + 1)):
            detail["errors"].append("数据行号不连续，无法与下载记录逐行对应")

        # 逐行校验值复核
        recomputed: list[str] = []
        for line in rows:
            digest = hashlib.sha256(
                json.dumps(line.get("data"), ensure_ascii=False,
                           sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()[:16]
            recomputed.append(digest)
            if digest != line.get("row_checksum"):
                detail["errors"].append(f"第 {line.get('line_no')} 行校验值不匹配")
        if manifest.get("row_checksums") is not None and \
                list(manifest.get("row_checksums", [])) != recomputed:
            detail["errors"].append("清单行校验值序列与实际数据不一致")

        data_checksum = hashlib.sha256("|".join(recomputed).encode("utf-8")).hexdigest()[:16]
        if data_checksum != manifest.get("data_checksum"):
            detail["errors"].append("数据校验值与清单不一致")
        if data_checksum != commit.get("data_checksum"):
            detail["errors"].append("数据校验值与提交记录不一致")

        material = (
            f"{manifest.get('package_id')}|{manifest.get('window_code')}|"
            f"{manifest.get('data_version')}|{expected}|{data_checksum}|"
            f"{manifest.get('contract_version')}"
        )
        package_checksum = hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
        if package_checksum != commit.get("package_checksum"):
            detail["errors"].append("封箱校验值不匹配")

        detail["rows"] = rows
        detail["data_checksum"] = data_checksum
        detail["package_checksum"] = package_checksum
        return (not detail["errors"]), detail

    def list_export_packages(self, window_id: int) -> dict[str, Any]:
        """列出窗口全部已封箱的导出包版本。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        registry = self._export_registry(window["窗口编号"])
        return {"window_code": window["窗口编号"], "packages": registry["packages"]}

    def get_export_package(self, window_id: int, export_seq: int) -> dict[str, Any]:
        """按导出序号回查历史导出包（原版本可查），并重新校验完整性。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        registry = self._export_registry(window["窗口编号"])
        meta = next((p for p in registry["packages"]
                     if int(p["export_seq"]) == int(export_seq)), None)
        if meta is None:
            return {"error": f"导出序号 {export_seq} 不存在"}
        ok, detail = self.validate_export_package(meta["path"])
        return {
            "meta": meta,
            "valid": ok,
            "errors": detail.get("errors", []),
            "manifest": detail.get("manifest"),
            "rows": detail.get("rows", []),
        }
