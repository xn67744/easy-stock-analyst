# -*- coding: utf-8 -*-
"""
historical_data.py - 支持 A 股/H 股 历史数据获取与导出
支持周期：日线、周线、月线、季线、年线
"""

import os
import argparse
from datetime import datetime
from typing import Optional

import akshare as ak
import pandas as pd


def _to_symbol(stock_code: str) -> str:
    """（仅 A 股使用）添加 sz/sh 前缀"""
    return f"sz{stock_code}" if stock_code.startswith(("0", "3")) else f"sh{stock_code}"


def _detect_market(stock_code: str) -> str:
    """
    根据股票代码自动识别市场
    - A 股：6 位数字（0/3/6 开头）
    - 港股：5 位数字
    """
    code_len = len(stock_code)
    if code_len == 6:
        return "mainland"
    if code_len == 5:
        return "hk"
    raise ValueError(
        f"无法自动识别股票代码 [{stock_code}] 的市场。\n"
        f"请手动传入 market 参数：'mainland'（A 股）或 'hk'（港股）"
    )


def _normalize_date_yyyymmdd(date_str: Optional[str], default: str) -> str:
    """标准化日期到 YYYYMMDD"""
    if not date_str:
        return default
    return pd.to_datetime(date_str).strftime("%Y%m%d")


def _get_market_output_dir(base_dir: str, target_market: str) -> str:
    """A 股/港股分目录输出"""
    folder = "a_share" if target_market == "mainland" else "h_share"
    path = os.path.join(base_dir, folder)
    os.makedirs(path, exist_ok=True)
    return path


def _resolve_stock_code_by_name(stock_name: str, market: str = None) -> tuple[str, str]:
    """根据股票名称解析股票代码与市场"""
    if market in (None, "mainland"):
        a_df = ak.stock_info_a_code_name()
        hit = a_df[a_df["name"].astype(str) == str(stock_name)]
        if not hit.empty:
            return str(hit.iloc[0]["code"]).zfill(6), "mainland"

    if market in (None, "hk"):
        hk_df = ak.stock_hk_spot_em()
        code_col = "代码" if "代码" in hk_df.columns else ("symbol" if "symbol" in hk_df.columns else None)
        name_col = "名称" if "名称" in hk_df.columns else ("name" if "name" in hk_df.columns else None)
        if code_col and name_col:
            hit = hk_df[hk_df[name_col].astype(str) == str(stock_name)]
            if not hit.empty:
                return str(hit.iloc[0][code_col]).zfill(5), "hk"

    raise ValueError(f"未找到股票名称 [{stock_name}] 对应的股票代码，请确认名称或显式传入 market")


def _fetch_daily(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    """获取日线数据"""
    if target_market == "mainland":
        symbol = _to_symbol(stock_code)
        df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
        df = df.reset_index(drop=True).rename(columns={
            "date": "日期", "open": "开盘", "high": "最高",
            "low": "最低", "close": "收盘", "volume": "成交量 (股)",
            "turnover": "换手率"
        })
        df["日期"] = pd.to_datetime(df["日期"])
        df["成交量 (手)"] = (pd.to_numeric(df["成交量 (股)"], errors="coerce") // 100).fillna(0).astype(int)
        df["换手率 (%)"] = (pd.to_numeric(df["换手率"], errors="coerce") * 100).round(4)
        df["涨跌幅 (%)"] = pd.to_numeric(df["收盘"], errors="coerce").pct_change().mul(100).round(2)
    else:
        df = ak.stock_hk_daily(symbol=stock_code, adjust=adjust)
        df = df.reset_index(drop=True).rename(columns={
            "date": "日期", "open": "开盘", "high": "最高",
            "low": "最低", "close": "收盘", "volume": "成交量 (股)"
        })
        df["日期"] = pd.to_datetime(df["日期"])
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        df = df[(df["日期"] >= start_dt) & (df["日期"] <= end_dt)].copy()
        df["成交量 (手)"] = (pd.to_numeric(df["成交量 (股)"], errors="coerce") // 100).fillna(0).astype(int)
        df["换手率 (%)"] = pd.NA
        df["涨跌幅 (%)"] = pd.to_numeric(df["收盘"], errors="coerce").pct_change().mul(100).round(2)

    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量 (手)", "换手率 (%)", "涨跌幅 (%)"]]


def _fetch_weekly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market).set_index("日期")
    df = df_daily.resample("W").agg({
        "开盘": "first", "收盘": "last", "最高": "max", "最低": "min",
        "成交量 (手)": "sum", "换手率 (%)": "last", "涨跌幅 (%)": "last"
    }).dropna().reset_index()
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    return df


def _fetch_monthly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market).set_index("日期")
    df = df_daily.resample("ME").agg({
        "开盘": "first", "收盘": "last", "最高": "max", "最低": "min",
        "成交量 (手)": "sum", "换手率 (%)": "last", "涨跌幅 (%)": "last"
    }).dropna().reset_index()
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    return df


def _fetch_quarterly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market).set_index("日期")
    df = df_daily.resample("QE").agg({
        "开盘": "first", "收盘": "last", "最高": "max", "最低": "min",
        "成交量 (手)": "sum", "换手率 (%)": "last", "涨跌幅 (%)": "last"
    }).dropna().reset_index()
    df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
    return df


def _fetch_yearly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market).set_index("日期")
    df = df_daily.resample("YE").agg({
        "开盘": "first", "收盘": "last", "最高": "max", "最低": "min",
        "成交量 (手)": "sum", "换手率 (%)": "last", "涨跌幅 (%)": "last"
    }).dropna().reset_index()
    df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
    return df


def fetch_history(
    stock_code: str = "000852",
    start_date: str = "20000101",
    end_date: str = None,
    adjust: str = "qfq",
    market: str = None,
    period: str = "daily",
) -> pd.DataFrame:
    """统一获取 A 股 或 港股历史数据"""
    target_market = market or _detect_market(stock_code)
    start_date = _normalize_date_yyyymmdd(start_date, "19901010")
    end_date = _normalize_date_yyyymmdd(end_date, datetime.now().strftime("%Y%m%d"))

    period = period.lower()
    if period == "daily":
        return _fetch_daily(stock_code, start_date, end_date, adjust, target_market)
    if period == "weekly":
        return _fetch_weekly(stock_code, start_date, end_date, adjust, target_market)
    if period == "monthly":
        return _fetch_monthly(stock_code, start_date, end_date, adjust, target_market)
    if period == "quarterly":
        return _fetch_quarterly(stock_code, start_date, end_date, adjust, target_market)
    if period == "yearly":
        return _fetch_yearly(stock_code, start_date, end_date, adjust, target_market)
    raise ValueError(f"不支持的周期 [{period}]，请使用：daily/weekly/monthly/quarterly/yearly")


def export_to_csv(
    df: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    period: str = "daily",
    output_dir: str = None,
    filename: str = None,
    market: str = None,
) -> str:
    output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
    target_market = market or _detect_market(stock_code)
    market_output_dir = _get_market_output_dir(output_dir, target_market)

    filename = filename or f"{stock_code}.csv"
    path = os.path.join(market_output_dir, filename)

    out = df.copy()
    out["日期"] = pd.to_datetime(out["日期"]).dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def update_daily_csv_by_stock_name(
    stock_name: str,
    start_date: str = None,
    end_date: str = None,
    adjust: str = "qfq",
    market: str = None,
    output_dir: str = None,
    filename: str = None,
):
    """
    根据股票名称和时间段获取日K并更新CSV：
    - 不存在则创建
    - 存在则合并去重更新
    - start_date 默认 1990-10-10
    - end_date 默认当前日期
    """
    stock_code, target_market = _resolve_stock_code_by_name(stock_name, market)
    start_date = _normalize_date_yyyymmdd(start_date, "19901010")
    end_date = _normalize_date_yyyymmdd(end_date, datetime.now().strftime("%Y%m%d"))

    df_new = fetch_history(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        market=target_market,
        period="daily",
    )

    output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
    market_output_dir = _get_market_output_dir(output_dir, target_market)
    filename = filename or f"{stock_code}.csv"
    csv_path = os.path.join(market_output_dir, filename)

    if os.path.exists(csv_path):
        df_old = pd.read_csv(csv_path)
        df_old["日期"] = pd.to_datetime(df_old["日期"])
        df_merged = pd.concat([df_old, df_new], ignore_index=True)
        df_merged = df_merged.drop_duplicates(subset=["日期"], keep="last").sort_values("日期")
    else:
        df_merged = df_new.sort_values("日期")

    df_merged["日期"] = pd.to_datetime(df_merged["日期"]).dt.strftime("%Y-%m-%d")
    df_merged.to_csv(csv_path, index=False, encoding="utf-8-sig")

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "market": target_market,
        "df": df_merged,
        "csv_path": csv_path,
    }


def main(
    stock_code: str = "000852",
    stock_name: str = "石化机械",
    start_date: str = "20000101",
    end_date: str = None,
    adjust: str = "qfq",
    market: str = None,
    period: str = "daily",
    preview_rows: int = 30,
    output_dir: str = None,
    output_filename: str = None,
):
    target_market = market or _detect_market(stock_code)
    market_name = "A 股" if target_market == "mainland" else "港股"

    print("\n" + "=" * 90)
    print(f"  {stock_name}（{stock_code}）{market_name} {period} 数据")
    print(f"  获取时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    df = fetch_history(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        market=market,
        period=period,
    )

    print(f"\n（共 {len(df)} 条，终端仅展示最近 {preview_rows} 条）\n")
    show = df.tail(preview_rows).copy()
    show["日期"] = pd.to_datetime(show["日期"]).dt.strftime("%Y-%m-%d")
    print(show.to_string(index=False, na_rep="-"))

    csv_path = export_to_csv(
        df,
        stock_code=stock_code,
        stock_name=stock_name,
        period=period,
        output_dir=output_dir,
        filename=output_filename,
        market=target_market,
    )
    print(f"\n[导出成功] {csv_path}\n")
    return {"df": df, "csv_path": csv_path}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="【A/H 股通用】股票历史数据获取工具")
    parser.add_argument("--stock_code", default="000852", help="股票代码 (A 股 6 位/港股 5 位)")
    parser.add_argument("--stock_name", default="石化机械", help="股票名称")
    parser.add_argument("--market", default=None, help="(可选) 强制指定市场：mainland/hk")
    parser.add_argument("--start_date", default="19900101", help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end_date", default=None, help="结束日期 (YYYYMMDD，默认今天)")
    parser.add_argument("--adjust", default="qfq", help="复权方式：qfq/hfq/none")
    parser.add_argument("--period", default="daily", help="数据周期：daily/weekly/monthly/quarterly/yearly")
    parser.add_argument("--output_dir", default=None, help="输出根目录（将自动分 a_share/h_share）")
    parser.add_argument("--output_filename", default=None, help="输出文件名")
    parser.add_argument("--update_by_name", action="store_true", help="按股票名称更新日K到CSV")
    args = parser.parse_args()

    if args.update_by_name:
        ret = update_daily_csv_by_stock_name(
            stock_name=args.stock_name,
            start_date=args.start_date,
            end_date=args.end_date,
            adjust=args.adjust,
            market=args.market,
            output_dir=args.output_dir,
            filename=args.output_filename,
        )
        print(f"[更新成功] {ret['stock_name']}({ret['stock_code']}) -> {ret['csv_path']}")
    else:
        main(
            stock_code=args.stock_code,
            stock_name=args.stock_name,
            market=args.market,
            start_date=args.start_date,
            end_date=args.end_date,
            adjust=args.adjust,
            period=args.period,
            output_dir=args.output_dir,
            output_filename=args.output_filename,
        )
