"""闭合抽取窗口明细导出接口：导出包下载、断点恢复、数据更正与包转换入口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.extract_window import ExtractWindowService
from app.services.field_contract import FieldContractService

router = APIRouter(prefix="/api", tags=["闭合窗口明细导出"])

_window_svc = ExtractWindowService()
_contract_svc = FieldContractService()


class PackPayload(BaseModel):
    chunk_size: int = Field(default=4, ge=1)


class CorrectionItem(BaseModel):
    line_no: int
    updates: dict[str, Any] = Field(default_factory=dict)


class CorrectionPayload(BaseModel):
    corrections: list[CorrectionItem]


class TransformPayload(BaseModel):
    package_path: str


@router.post("/extract_window/{window_id}/export")
def export_window_detail(window_id: int, payload: PackPayload) -> dict[str, Any]:
    """发起一次闭合窗口明细导出，生成新的抽取数据导出包版本。"""
    result = _window_svc.export_window_detail(window_id, chunk_size=payload.chunk_size)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/extract_window/{window_id}/export/{export_seq}/resume")
def resume_export(window_id: int, export_seq: int, payload: PackPayload) -> dict[str, Any]:
    """凭断点恢复中断的打包，按锁定的数据版本快照续写封箱。"""
    result = _window_svc.resume_export(window_id, export_seq, chunk_size=payload.chunk_size)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/extract_window/{window_id}/exports")
def list_export_packages(window_id: int) -> dict[str, Any]:
    """列出窗口全部已封箱的导出包版本。"""
    result = _window_svc.list_export_packages(window_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/extract_window/{window_id}/exports/{export_seq}")
def get_export_package(window_id: int, export_seq: int) -> dict[str, Any]:
    """按导出序号回查历史导出包（原版本可查），并重新校验完整性。"""
    result = _window_svc.get_export_package(window_id, export_seq)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/extract_window/{window_id}/corrections")
def correct_window_records(window_id: int, payload: CorrectionPayload) -> dict[str, Any]:
    """更正闭合窗口中的指定数据行，生成新的数据版本。"""
    result = _window_svc.correct_window_records(
        window_id, [item.model_dump() for item in payload.corrections])
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/field_contract/transform_package")
def transform_package(payload: TransformPayload) -> dict[str, Any]:
    """映射入口：校验抽取数据导出包并生成字段转换结果，部分文件拒收。"""
    result = _contract_svc.transform_export_package(payload.package_path)
    if "error" in result:
        raise HTTPException(status_code=422, detail=result["error"])
    return result
