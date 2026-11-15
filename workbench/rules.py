"""规则引擎：规则 = 版本化的声明式 JSON 规格，可注册新版本实现升级。

规格结构：
{
  "version": "v2",
  "key_field": "record_id",                # 结果行的业务键字段（默认 record_id）
  "transforms": [ ... ],                   # 有序变换
  "filters": [ {"field","op","value"} ]    # 任一条件命中则该记录被过滤（status=filtered）
}

变换算子：
  rename           {op, from, to}
  upper / lower    {op, field}
  round            {op, field, digits}
  date_norm        {op, field, formats:[...]}        多格式日期归一为 ISO yyyy-mm-dd
  convert_currency {op, field, currency_field, to, rates:{}, out_field}
  compute          {op, out_field, expr, round?}     安全表达式（仅 +-*/ 与字段引用）
  set              {op, field, value}
  drop_fields      {op, fields:[...]}

过滤算子：eq | neq | in | not_in | exists | missing
"""

from __future__ import annotations

import ast
import json
from datetime import datetime


class RuleError(ValueError):
    pass


# ---------------------------------------------------------------- 安全表达式

_ALLOWED_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Div)
_ALLOWED_UNARY = (ast.UAdd, ast.USub)


def _check_expr_syntax(expr: str) -> None:
    """注册期校验：只允许 数字、字段名、+ - * /、括号。"""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise RuleError(f"表达式语法错误: {e}") from e
    for node in ast.walk(tree):
        if isinstance(node, (ast.Expression, ast.Load, ast.Constant, ast.Name,
                             ast.BinOp, ast.UnaryOp) + _ALLOWED_BINOPS + _ALLOWED_UNARY):
            continue
        raise RuleError(f"表达式含不允许的语法: {type(node).__name__}")


def _safe_eval(expr: str, fields: dict) -> float:
    """只允许 数字常量、字段名、+ - * / 与括号 的算术表达式。"""
    tree = ast.parse(expr, mode="eval")

    def ev(node):
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return float(node.value)
            raise RuleError(f"表达式含非法常量: {node.value!r}")
        if isinstance(node, ast.Name):
            if node.id not in fields:
                raise RuleError(f"表达式引用未知字段: {node.id}")
            val = fields[node.id]
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise RuleError(f"字段 {node.id} 不是数值: {val!r}")
            return float(val)
        if isinstance(node, ast.BinOp) and isinstance(node.op, _ALLOWED_BINOPS):
            left, right = ev(node.left), ev(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                if right == 0:
                    raise RuleError("除零")
                return left / right
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, _ALLOWED_UNARY):
            val = ev(node.operand)
            return val if isinstance(node.op, ast.UAdd) else -val
        raise RuleError(f"表达式含不允许的语法: {ast.dump(node)[:80]}")

    return ev(tree)


# ---------------------------------------------------------------- 规格校验

_FILTER_OPS = {"eq", "neq", "in", "not_in", "exists", "missing"}
_TRANSFORM_OPS = {
    "rename", "upper", "lower", "round", "date_norm",
    "convert_currency", "compute", "set", "drop_fields",
}


def validate_spec(spec: dict) -> dict:
    if not isinstance(spec, dict):
        raise RuleError("规则规格必须是 JSON 对象")
    version = spec.get("version")
    if not version or not isinstance(version, str):
        raise RuleError("规则缺少 version 字符串")
    key_field = spec.get("key_field", "record_id")
    if not isinstance(key_field, str) or not key_field:
        raise RuleError("key_field 必须是非空字符串")
    transforms = spec.get("transforms", [])
    filters = spec.get("filters", [])
    if not isinstance(transforms, list) or not isinstance(filters, list):
        raise RuleError("transforms / filters 必须是数组")
    for t in transforms:
        op = t.get("op")
        if op not in _TRANSFORM_OPS:
            raise RuleError(f"未知变换算子: {op!r}")
        if op == "compute":
            if "expr" not in t or "out_field" not in t:
                raise RuleError("compute 需要 expr 与 out_field")
            _check_expr_syntax(t["expr"])
        if op == "convert_currency":
            if not isinstance(t.get("rates"), dict) or "out_field" not in t:
                raise RuleError("convert_currency 需要 rates 表与 out_field")
    for f in filters:
        if f.get("op") not in _FILTER_OPS:
            raise RuleError(f"未知过滤算子: {f.get('op')!r}")
        if "field" not in f:
            raise RuleError("过滤条件缺少 field")
    return {
        "version": version,
        "key_field": key_field,
        "transforms": transforms,
        "filters": filters,
    }


def spec_from_json(text: str) -> dict:
    try:
        spec = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuleError(f"规则 JSON 解析失败: {e}") from e
    return validate_spec(spec)


# ---------------------------------------------------------------- 规则应用

def _to_number(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", ""))
        except ValueError:
            return None
    return None


def apply_rule(spec: dict, record_id: str, fields: dict, raw_status: str) -> dict:
    """对一条原始记录应用规则，返回派生行。

    返回 {record_key, status, payload, warnings}
    status: active | filtered | revoked（revoked 由原始层状态决定，仍给出最后已知内容便于追溯）
    """
    out = dict(fields)
    out.setdefault("record_id", record_id)
    warnings: list[str] = []

    for t in spec["transforms"]:
        op = t["op"]
        try:
            if op == "rename":
                if t["from"] in out:
                    out[t["to"]] = out.pop(t["from"])
            elif op in ("upper", "lower"):
                v = out.get(t["field"])
                if isinstance(v, str):
                    out[t["field"]] = v.upper() if op == "upper" else v.lower()
            elif op == "round":
                n = _to_number(out.get(t["field"]))
                if n is not None:
                    out[t["field"]] = round(n, int(t.get("digits", 2)))
            elif op == "date_norm":
                v = out.get(t["field"])
                if isinstance(v, str) and v:
                    parsed = None
                    for fmt in t.get("formats", []):
                        try:
                            parsed = datetime.strptime(v.strip(), fmt)
                            break
                        except ValueError:
                            continue
                    if parsed is not None:
                        out[t["field"]] = parsed.strftime("%Y-%m-%d")
                    else:
                        warnings.append(f"日期无法解析: {v!r}")
            elif op == "convert_currency":
                n = _to_number(out.get(t["field"]))
                cur = str(out.get(t["currency_field"], "")).upper()
                rates = {str(k).upper(): float(v) for k, v in t["rates"].items()}
                if n is None:
                    warnings.append(f"金额非数值: {out.get(t['field'])!r}")
                elif cur not in rates:
                    warnings.append(f"未知币种: {cur!r}")
                else:
                    out[t["out_field"]] = round(n * rates[cur], 6)
            elif op == "compute":
                try:
                    val = _safe_eval(t["expr"], out)
                    if "round" in t:
                        val = round(val, int(t["round"]))
                    out[t["out_field"]] = val
                except RuleError as e:
                    warnings.append(f"计算失败({t['out_field']}): {e}")
            elif op == "set":
                out[t["field"]] = t["value"]
            elif op == "drop_fields":
                for f in t.get("fields", []):
                    out.pop(f, None)
        except Exception as e:  # 单条记录的规则异常不拖垮整批，记入警告
            warnings.append(f"变换 {op} 异常: {e}")

    filtered = False
    for f in spec["filters"]:
        v = out.get(f["field"])
        op = f["op"]
        hit = False
        if op == "eq":
            hit = v == f.get("value")
        elif op == "neq":
            hit = v != f.get("value")
        elif op == "in":
            hit = v in (f.get("value") or [])
        elif op == "not_in":
            hit = v not in (f.get("value") or [])
        elif op == "exists":
            hit = v is not None
        elif op == "missing":
            hit = v is None
        if hit:
            filtered = True
            break

    key = out.get(spec["key_field"], record_id)
    status = "revoked" if raw_status == "revoked" else ("filtered" if filtered else "active")
    if warnings:
        out["_warnings"] = warnings
    return {
        "record_key": str(key),
        "status": status,
        "payload": out if status != "filtered" else None,
        "warnings": warnings,
    }


def content_hash_of(status: str, payload) -> str:
    import hashlib

    canonical = json.dumps({"status": status, "payload": payload},
                           sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
