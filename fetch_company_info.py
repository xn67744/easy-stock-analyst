# -*- coding: utf-8 -*-
"""
fetch_company_info.py - A 股上市公司基础信息获取与入库
支持模式：full（全量）、incremental（增量）
数据来源：akshare
数据库：PostgreSQL

接口说明（已验证字段）：
  stock_info_a_code_name()          → 全市场股票列表（code/name）
  stock_individual_info_em(symbol)  → 简要信息：行业/上市时间/总股本/流通股
  stock_profile_cninfo(symbol)      → 详细概况：法人/注册资本/省份/员工数/主营业务等
  stock_zh_a_gbjg_em(symbol)        → 股本结构历史：变更日期/总股本/已上市流通A股 等
  stock_register_all_em(symbol)     → IPO 注册信息：发行价格/上市日期 等
  stock_dividend_cninfo(symbol)     → 历史分红：除权除息日/每股派息/送股/配股 等

依赖：
    pip install akshare psycopg2-binary pandas

环境变量（或直接修改下方默认值）：
    DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""

import os
import argparse
from datetime import date, datetime, timedelta
from typing import Optional

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, Json


def get_db_config(
    host: str = None,
    port: int = None,
    dbname: str = None,
    user: str = None,
    password: str = None,
) -> dict:
    """构建数据库配置：优先外部参数，其次环境变量，最后默认值"""
    return {
        "host": host or os.getenv("DB_HOST", "localhost"),
        "port": int(port or os.getenv("DB_PORT", 5432)),
        "dbname": dbname or os.getenv("DB_NAME", "mystock"),
        "user": user or os.getenv("DB_USER", "postgres"),
        "password": password if password is not None else os.getenv("DB_PASSWORD", ""),
    }


def _get_conn(db_config: dict):
    return psycopg2.connect(**db_config)


def _to_full_code(raw_code: str) -> str:
    """6 位裸码 → 带交易所后缀，如 000001 → 000001.SZ"""
    code = str(raw_code).zfill(6)
    if code.startswith(("60", "68", "51", "11")):
        return f"{code}.SH"
    elif code.startswith(("43", "83", "87", "88")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def _safe_float(v) -> Optional[float]:
    try:
        val = str(v).replace(",", "").replace("万元", "").replace("万", "").strip()
        return float(val) if val not in ("", "None", "-", "nan") else None
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> Optional[int]:
    try:
        val = str(v).replace(",", "").strip()
        return int(float(val)) if val not in ("", "None", "-", "nan") else None
    except (TypeError, ValueError):
        return None


def _safe_date(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "None", "NaT", "-", "nan"):
        return None
    try:
        return pd.to_datetime(s).date().isoformat()
    except Exception:
        return None


def _to_dict(df: pd.DataFrame) -> dict:
    """将 item/value 两列的 DataFrame 转成字典"""
    return dict(zip(df.iloc[:, 0].astype(str), df.iloc[:, 1]))


def _merge_jsonb_list(existing: list, new_value, recorded_date: str) -> list:
    """
    向 JSONB 数组追加带时间戳记录，同日期幂等覆盖。
    格式：[{"recorded_date": "2024-03-31", "value": 12500}, ...]
    """
    merged = [e for e in existing if e.get("recorded_date") != recorded_date]
    if new_value is not None:
        merged.append({"recorded_date": recorded_date, "value": new_value})
    merged.sort(key=lambda x: x.get("recorded_date", ""))
    return merged


# ------------------------------------------------------------------
# 全市场股票列表
# ------------------------------------------------------------------
def _fetch_stock_list() -> pd.DataFrame:
    """返回 raw_code / stock_code / stock_name / exchange"""
    print("  正在拉取全市场股票列表 ...")
    df = ak.stock_info_a_code_name()
    # akshare 返回列：code / name
    df = df.rename(columns={"code": "raw_code", "name": "stock_name"})
    df["stock_code"] = df["raw_code"].apply(_to_full_code)
    df["exchange"]   = df["stock_code"].apply(lambda x: x.split(".")[-1])
    return df[["raw_code", "stock_code", "stock_name", "exchange"]]


# ------------------------------------------------------------------
# 拉取单只股票 → company_info 字段
# ------------------------------------------------------------------
# 缓存 IPO 注册表（全量拉一次即可，避免循环内重复请求）
_ipo_cache: Optional[pd.DataFrame] = None


def _get_ipo_cache() -> pd.DataFrame:
    global _ipo_cache
    if _ipo_cache is None:
        try:
            _ipo_cache = ak.stock_register_all_em(symbol="沪深A股")
            _ipo_cache["_code6"] = _ipo_cache["股票代码"].astype(str).str.zfill(6)
        except Exception as e:
            print(f"  [提示] IPO 注册表拉取失败（发行价将跳过）：{e}")
            _ipo_cache = pd.DataFrame()
    return _ipo_cache


def _fetch_company_info(raw_code: str, stock_name: str, exchange: str) -> Optional[dict]:
    """
    数据拼装顺序（后者补充前者空缺）：
      1. stock_individual_info_em  → 行业 / 上市时间 / 总股本 / 流通股
      2. stock_profile_cninfo      → 全称/法人/注册资本/省份/城市/官网/员工数/主营业务
      3. stock_zh_a_gbjg_em        → 总股本 / 流通A股（校验补充）
      4. IPO 缓存                  → 发行价
    """
    stock_code  = _to_full_code(raw_code)
    record_date = date.today().isoformat()

    result: dict = {
        "stock_code":     stock_code,
        "stock_name":     stock_name,
        "exchange":       exchange,
        "status":         "listed",
        "extra_info":     {},
        "employee_count": [],
        "avg_salary":     [],
    }

    # ── 接口 1：stock_individual_info_em ─────────────────────────
    # 实际返回字段：总市值 / 流通市值 / 行业 / 上市时间 / 股票代码 / 股票简称 / 总股本 / 流通股
    try:
        info1 = _to_dict(ak.stock_individual_info_em(symbol=raw_code))
        result["listing_date"] = _safe_date(info1.get("上市时间"))
        result["total_shares"] = _safe_int(info1.get("总股本"))
        result["float_shares"] = _safe_int(info1.get("流通股"))
        # 东财行业分类存 extra_info（非申万分类，仅备用）
        result["extra_info"]["industry_em"] = info1.get("行业")
    except Exception as e:
        print(f"\n    [警告] stock_individual_info_em 失败：{e}", end="")

    # ── 接口 2：stock_profile_cninfo ─────────────────────────────
    # 来源：巨潮资讯
    # 典型字段：公司名称 / 英文名称 / 法定代表人 / 董事长 / 注册资本 /
    #           省份 / 城市 / 官方网站 / 员工人数 / 主营业务 / 成立日期 等
    try:
        info2 = _to_dict(ak.stock_profile_cninfo(symbol=raw_code))
        result.update({
            "full_name":          info2.get("公司名称"),
            "legal_person":       info2.get("法定代表人") or info2.get("法人代表"),
            "chairman":           info2.get("董事长"),
            "registered_capital": _safe_float(info2.get("注册资本")),
            "province":           info2.get("省份"),
            "city":               info2.get("城市"),
            "website":            info2.get("官方网站") or info2.get("官网"),
        })
        result["extra_info"]["business_desc"] = info2.get("主营业务") or info2.get("经营范围")
        result["extra_info"]["en_name"]        = info2.get("英文名称")

        # 员工数带时间戳存入 JSONB
        emp = _safe_int(info2.get("员工人数"))
        if emp:
            result["employee_count"] = _merge_jsonb_list([], emp, record_date)
    except Exception as e:
        print(f"\n    [警告] stock_profile_cninfo 失败：{e}", end="")

    # ── 接口 3：stock_zh_a_gbjg_em ───────────────────────────────
    # 返回字段：变更日期 / 总股本 / 流通受限股份 / 已流通股份 / 已上市流通A股 / 变动原因
    try:
        df3 = ak.stock_zh_a_gbjg_em(symbol=raw_code)
        if not df3.empty:
            latest = df3.sort_values("变更日期", ascending=False).iloc[0]
            if result.get("total_shares") is None:
                result["total_shares"] = _safe_int(latest.get("总股本"))
            if result.get("float_shares") is None:
                result["float_shares"] = _safe_int(
                    latest.get("已上市流通A股") or latest.get("已流通股份")
                )
    except Exception as e:
        print(f"\n    [提示] stock_zh_a_gbjg_em 失败：{e}", end="")

    # ── 接口 4：IPO 注册表（缓存）────────────────────────────────
    # 返回字段：股票代码 / 股票简称 / 发行价格 / 上市日期 / 发行市盈率 / 发行方式 等
    try:
        ipo_df = _get_ipo_cache()
        if not ipo_df.empty:
            row = ipo_df[ipo_df["_code6"] == raw_code]
            if not row.empty:
                result["issue_price"] = _safe_float(
                    row.iloc[0].get("发行价格") or row.iloc[0].get("发行价")
                )
                if result.get("listing_date") is None:
                    result["listing_date"] = _safe_date(row.iloc[0].get("上市日期"))
    except Exception as e:
        print(f"\n    [提示] IPO 缓存查询失败：{e}", end="")

    return result


# ------------------------------------------------------------------
# 拉取单只股票历史分红 → dividend 表
# ------------------------------------------------------------------
def _fetch_dividend(raw_code: str) -> list[dict]:
    """
    stock_dividend_cninfo(symbol, indicator="分红")
    返回字段：股票简称 / 公告日期 / 股权登记日 / 除权除息日 /
             每股派息(税前) / 每股转增股本 / 每股配股 / 配股价格 / 股息率 / 年度 等
    """
    stock_code = _to_full_code(raw_code)
    rows: list[dict] = []
    try:
        df = ak.stock_dividend_cninfo(symbol=raw_code, indicator="分红")
        if df is None or df.empty:
            return rows
        for _, r in df.iterrows():
            ex_date = _safe_date(r.get("除权除息日"))
            if not ex_date:
                continue
            dps   = _safe_float(r.get("每股派息(税前)") or r.get("每股派息"))
            bonus = _safe_float(r.get("每股转增股本") or r.get("送股"))
            rows.append({
                "stock_code":         stock_code,
                "ex_date":            ex_date,
                "pay_date":           _safe_date(r.get("派息日") or r.get("红利到账日")),
                "record_date":        _safe_date(r.get("股权登记日")),
                "dividend_per_share": dps,
                "bonus_share_ratio":  bonus,
                "rights_ratio":       _safe_float(r.get("每股配股")),
                "rights_price":       _safe_float(r.get("配股价格")),
                "dividend_yield":     _safe_float(r.get("股息率")),
                "fiscal_year":        str(r.get("年度") or "")[:4] or None,
                "dividend_plan": {
                    "recorded_date": date.today().isoformat(),
                    "raw":  f"派{dps}元 送{bonus}股" if (dps or bonus) else str(r.to_dict()),
                    "cash":  dps,
                    "bonus": bonus,
                },
            })
    except Exception as e:
        print(f"\n    [提示] stock_dividend_cninfo 失败：{e}", end="")
    return rows


# ------------------------------------------------------------------
# 写库：company_info（UPSERT）
# ------------------------------------------------------------------
_UPSERT_COMPANY_SQL = """
INSERT INTO company_info (
    stock_code, stock_name, full_name, exchange,
    listing_date, issue_price,
    total_shares, float_shares, registered_capital,
    legal_person, chairman, province, city, website,
    status, employee_count, avg_salary, extra_info,
    updated_at
) VALUES %s
ON CONFLICT (stock_code) DO UPDATE SET
    stock_name         = EXCLUDED.stock_name,
    full_name          = COALESCE(EXCLUDED.full_name,          company_info.full_name),
    listing_date       = COALESCE(EXCLUDED.listing_date,       company_info.listing_date),
    issue_price        = COALESCE(EXCLUDED.issue_price,        company_info.issue_price),
    total_shares       = COALESCE(EXCLUDED.total_shares,       company_info.total_shares),
    float_shares       = COALESCE(EXCLUDED.float_shares,       company_info.float_shares),
    registered_capital = COALESCE(EXCLUDED.registered_capital, company_info.registered_capital),
    legal_person       = COALESCE(EXCLUDED.legal_person,       company_info.legal_person),
    chairman           = COALESCE(EXCLUDED.chairman,           company_info.chairman),
    province           = COALESCE(EXCLUDED.province,           company_info.province),
    city               = COALESCE(EXCLUDED.city,               company_info.city),
    website            = COALESCE(EXCLUDED.website,            company_info.website),
    status             = EXCLUDED.status,
    employee_count     = (
        SELECT jsonb_agg(elem ORDER BY (elem->>'recorded_date'))
        FROM (
            SELECT DISTINCT ON (elem->>'recorded_date') elem
            FROM jsonb_array_elements(
                COALESCE(company_info.employee_count, '[]'::jsonb)
                || COALESCE(EXCLUDED.employee_count,  '[]'::jsonb)
            ) elem
            ORDER BY (elem->>'recorded_date') DESC
        ) sub
    ),
    avg_salary         = (
        SELECT jsonb_agg(elem ORDER BY (elem->>'recorded_date'))
        FROM (
            SELECT DISTINCT ON (elem->>'recorded_date') elem
            FROM jsonb_array_elements(
                COALESCE(company_info.avg_salary, '[]'::jsonb)
                || COALESCE(EXCLUDED.avg_salary,  '[]'::jsonb)
            ) elem
            ORDER BY (elem->>'recorded_date') DESC
        ) sub
    ),
    extra_info         = company_info.extra_info || EXCLUDED.extra_info,
    updated_at         = NOW();
"""


def _upsert_company_batch(records: list[dict], db_config: dict):
    if not records:
        return
    rows = [
        (
            r.get("stock_code"),    r.get("stock_name"),
            r.get("full_name"),     r.get("exchange"),
            r.get("listing_date"),  r.get("issue_price"),
            r.get("total_shares"),  r.get("float_shares"),
            r.get("registered_capital"),
            r.get("legal_person"),  r.get("chairman"),
            r.get("province"),      r.get("city"),
            r.get("website"),       r.get("status", "listed"),
            Json(r.get("employee_count", [])),
            Json(r.get("avg_salary",     [])),
            Json(r.get("extra_info",     {})),
            datetime.now(),
        )
        for r in records
    ]
    conn = _get_conn(db_config)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, _UPSERT_COMPANY_SQL, rows, page_size=100)
    finally:
        conn.close()


# ------------------------------------------------------------------
# 写库：dividend（UPSERT）
# ------------------------------------------------------------------
_UPSERT_DIVIDEND_SQL = """
INSERT INTO dividend (
    stock_code, ex_date, pay_date, record_date,
    dividend_per_share, bonus_share_ratio,
    rights_ratio, rights_price, dividend_yield,
    fiscal_year, dividend_plan, updated_at
) VALUES %s
ON CONFLICT (stock_code, ex_date) DO UPDATE SET
    pay_date           = COALESCE(EXCLUDED.pay_date,           dividend.pay_date),
    record_date        = COALESCE(EXCLUDED.record_date,        dividend.record_date),
    dividend_per_share = COALESCE(EXCLUDED.dividend_per_share, dividend.dividend_per_share),
    bonus_share_ratio  = COALESCE(EXCLUDED.bonus_share_ratio,  dividend.bonus_share_ratio),
    rights_ratio       = COALESCE(EXCLUDED.rights_ratio,       dividend.rights_ratio),
    rights_price       = COALESCE(EXCLUDED.rights_price,       dividend.rights_price),
    dividend_yield     = COALESCE(EXCLUDED.dividend_yield,     dividend.dividend_yield),
    fiscal_year        = COALESCE(EXCLUDED.fiscal_year,        dividend.fiscal_year),
    dividend_plan      = EXCLUDED.dividend_plan,
    updated_at         = NOW();
"""


def _upsert_dividend_batch(records: list[dict], db_config: dict):
    if not records:
        return
    rows = [
        (
            r["stock_code"],            r["ex_date"],
            r.get("pay_date"),          r.get("record_date"),
            r.get("dividend_per_share"), r.get("bonus_share_ratio"),
            r.get("rights_ratio"),      r.get("rights_price"),
            r.get("dividend_yield"),    r.get("fiscal_year"),
            Json(r.get("dividend_plan", {})),
            datetime.now(),
        )
        for r in records
    ]
    conn = _get_conn(db_config)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, _UPSERT_DIVIDEND_SQL, rows, page_size=200)
    finally:
        conn.close()


# ------------------------------------------------------------------
# 增量：识别需要更新的股票
# ------------------------------------------------------------------
def _get_incremental_codes(days: int, db_config: dict) -> list[str]:
    """双路策略：数据库超期 + 公告变动，取并集"""
    codes = set()

    # 策略 1：数据库 updated_at 超期
    try:
        cutoff = date.today() - timedelta(days=days)
        conn   = _get_conn(db_config)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stock_code FROM company_info WHERE updated_at < %s OR updated_at IS NULL",
                (cutoff,)
            )
            db_codes = [r[0].split(".")[0].zfill(6) for r in cur.fetchall()]
        conn.close()
        codes.update(db_codes)
        print(f"  数据库兜底：{len(db_codes)} 只超期股票")
    except Exception as e:
        print(f"  [警告] 数据库查询失败：{e}")

    # 策略 2：东财公告接口
    try:
        start    = (date.today() - timedelta(days=days)).strftime("%Y%m%d")
        end      = date.today().strftime("%Y%m%d")
        ann_df   = ak.stock_notice_report(market="沪深京", start_date=start, end_date=end)
        keywords = ["年度报告", "半年度报告", "股份", "股本", "员工", "董事长", "法人", "注册资本"]
        mask     = ann_df["公告标题"].str.contains("|".join(keywords), na=False)
        ann_codes = [str(c).zfill(6) for c in ann_df.loc[mask, "股票代码"].dropna()]
        codes.update(ann_codes)
        print(f"  公告接口：新增 {len(ann_codes)} 只变动股票")
    except Exception as e:
        print(f"  [提示] 公告接口失败，仅使用数据库兜底：{e}")

    return list(codes)


# ------------------------------------------------------------------
# 主函数
# ------------------------------------------------------------------
def main(
    mode:          str  = "incremental",
    days:          int  = 7,
    stock_code:    str  = None,
    batch_size:    int  = 50,
    with_dividend: bool = True,
    export_csv:    bool = False,
    output_dir:    str  = None,
    db_config:     dict = None,
):
    """
    mode          : full=全量  incremental=增量
    days          : 增量回溯天数
    stock_code    : 单只股票（6 位裸码或带后缀）
    batch_size    : 每批写库数量
    with_dividend : 同步写入 dividend 表
    export_csv    : 导出 company_info 的 CSV
    output_dir    : CSV 输出目录
    """
    print("\n" + "=" * 70)
    print(f"  公司基础信息获取  |  模式：{mode.upper()}"
          f"  |  分红同步：{'是' if with_dividend else '否'}"
          f"  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    stock_df = _fetch_stock_list()
    db_config = db_config or get_db_config()
    code_map = {row["raw_code"]: row for _, row in stock_df.iterrows()}
    print(f"  股票列表共 {len(stock_df)} 只\n")

    # 确定本次范围
    if mode == "full":
        target_codes = list(stock_df["raw_code"])
    else:
        if stock_code:
            raw = stock_code.split(".")[0].zfill(6)
            target_codes = [raw]
            print(f"  单股模式：{_to_full_code(raw)}")
        else:
            print(f"  识别近 {days} 天变动股票 ...")
            target_codes = _get_incremental_codes(days, db_config)

    valid_codes = [c for c in target_codes if c in code_map]
    total       = len(valid_codes)
    print(f"  本次待处理：{total} 只\n")

    company_batch:  list[dict] = []
    dividend_batch: list[dict] = []
    all_company:    list[dict] = []
    success = fail = 0

    for i, raw in enumerate(valid_codes, 1):
        row  = code_map[raw]
        name = row["stock_name"]
        exch = row["exchange"]

        print(f"  [{i}/{total}] {_to_full_code(raw)} {name} ...", end=" ")

        rec = _fetch_company_info(raw, name, exch)
        if rec:
            company_batch.append(rec)
            all_company.append(rec)
            success += 1
            print("OK", end="")
        else:
            fail += 1
            print("SKIP", end="")

        if with_dividend:
            div_rows = _fetch_dividend(raw)
            dividend_batch.extend(div_rows)
            if div_rows:
                print(f"  分红 {len(div_rows)} 条", end="")

        print()

        # 批量写库
        if len(company_batch) >= batch_size:
            _upsert_company_batch(company_batch, db_config)
            print(f"  → company_info 写入 {len(company_batch)} 条\n")
            company_batch.clear()

        if with_dividend and len(dividend_batch) >= batch_size * 5:
            _upsert_dividend_batch(dividend_batch, db_config)
            print(f"  → dividend 写入 {len(dividend_batch)} 条\n")
            dividend_batch.clear()

    # 尾部写库
    if company_batch:
        _upsert_company_batch(company_batch, db_config)
        print(f"  → company_info 写入 {len(company_batch)} 条")
    if with_dividend and dividend_batch:
        _upsert_dividend_batch(dividend_batch, db_config)
        print(f"  → dividend 写入 {len(dividend_batch)} 条")

    # 可选 CSV
    if export_csv and all_company:
        output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
        filename   = f"company_info_{date.today().isoformat()}.csv"
        path       = os.path.join(output_dir, filename)
        pd.DataFrame(all_company).to_csv(path, index=False, encoding="utf-8-sig")
        print(f"\n  [导出成功] {path}")

    print("\n" + "=" * 70)
    print(f"  完成  |  成功 {success}  跳过 {fail}"
          f"  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


# ------------------------------------------------------------------
# 命令行入口
# ------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A 股公司基础信息获取工具")
    parser.add_argument("--mode",        default="incremental", choices=["full", "incremental"],
                        help="full=全量  incremental=增量（默认）")
    parser.add_argument("--days",        type=int, default=7,
                        help="增量回溯天数，默认 7 天")
    parser.add_argument("--stock_code",  default=None,
                        help="单只股票，6 位裸码或带后缀，如 000001 或 000001.SZ")
    parser.add_argument("--batch_size",  type=int, default=50,
                        help="每批写库数量，默认 50")
    parser.add_argument("--no_dividend", action="store_true",
                        help="跳过分红数据同步")
    parser.add_argument("--export_csv",  action="store_true",
                        help="同时导出 CSV 文件")
    parser.add_argument("--output_dir",  default=None,
                        help="CSV 输出目录，默认脚本同级目录")
    parser.add_argument("--db_host",     default=None, help="数据库主机")
    parser.add_argument("--db_port",     type=int, default=None, help="数据库端口")
    parser.add_argument("--db_name",     default=None, help="数据库名")
    parser.add_argument("--db_user",     default=None, help="数据库用户名")
    parser.add_argument("--db_password", default=None, help="数据库密码")
    args = parser.parse_args()

    main(
        mode          = args.mode,
        days          = args.days,
        stock_code    = args.stock_code,
        batch_size    = args.batch_size,
        with_dividend = not args.no_dividend,
        export_csv    = args.export_csv,
        output_dir    = args.output_dir,
        db_config     = get_db_config(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password,
        ),
    )