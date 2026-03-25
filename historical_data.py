# -*- coding: utf-8 -*-
"""
historical_data.py - 支持 A 股/H 股 历史数据获取与导出
支持周期：日线、周线、月线、季线、年线
"""

import os
import argparse
from datetime import datetime
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
    elif code_len == 5:
        return "hk"
    else:
        raise ValueError(
            f"无法自动识别股票代码 [{stock_code}] 的市场。\n"
            f"请手动传入 market 参数：'mainland'（A 股）或 'hk'（港股）"
        )


def _fetch_daily(
    stock_code: str,
    start_date: str,
    end_date: str,
    adjust: str,
    target_market: str
) -> pd.DataFrame:
    """获取日线数据"""
    if target_market == "mainland":
        symbol = _to_symbol(stock_code)
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )
        df = df.reset_index(drop=True).rename(columns={
            "date": "日期", "open": "开盘", "high": "最高", 
            "low": "最低", "close": "收盘", "volume": "成交量 (股)", 
            "turnover": "换手率"
        })
        df["日期"] = pd.to_datetime(df["日期"])
        df["成交量 (手)"] = (pd.to_numeric(df["成交量 (股)"], errors="coerce") // 100).fillna(0).astype(int)
        df["换手率 (%)"] = (pd.to_numeric(df["换手率"], errors="coerce") * 100).round(4)
        df["涨跌幅 (%)"] = pd.to_numeric(df["收盘"], errors="coerce").pct_change().mul(100).round(2)

    elif target_market == "hk":
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
    """获取周线数据（从日线聚合，避免代理问题）"""
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market)
    df_daily = df_daily.set_index('日期')
    df = df_daily.resample('W').agg({
        '开盘': 'first', '收盘': 'last', '最高': 'max', '最低': 'min', 
        '成交量 (手)': 'sum', '换手率 (%)': 'last', '涨跌幅 (%)': 'last'
    }).dropna().reset_index()
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime('%Y-%m-%d')
    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量 (手)", "换手率 (%)", "涨跌幅 (%)"]]


def _fetch_monthly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    """获取月线数据（从日线聚合，避免代理问题）"""
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market)
    df_daily = df_daily.set_index('日期')
    df = df_daily.resample('ME').agg({
        '开盘': 'first', '收盘': 'last', '最高': 'max', '最低': 'min', 
        '成交量 (手)': 'sum', '换手率 (%)': 'last', '涨跌幅 (%)': 'last'
    }).dropna().reset_index()
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime('%Y-%m-%d')
    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量 (手)", "换手率 (%)", "涨跌幅 (%)"]]


def _fetch_quarterly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    """获取季线数据"""
    # 从日线聚合
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market)
    df_daily = df_daily.set_index('日期')
    df = df_daily.resample('QE').agg({
        '开盘': 'first', '收盘': 'last', '最高': 'max', '最低': 'min', 
        '成交量 (手)': 'sum', '换手率 (%)': 'last', '涨跌幅 (%)': 'last'
    }).dropna().reset_index()
    df["日期"] = df["日期"].dt.strftime('%Y-%m-%d')
    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量 (手)", "换手率 (%)", "涨跌幅 (%)"]]


def _fetch_yearly(stock_code: str, start_date: str, end_date: str, adjust: str, target_market: str) -> pd.DataFrame:
    """获取年线数据"""
    # 从日线聚合
    df_daily = _fetch_daily(stock_code, start_date, end_date, adjust, target_market)
    df_daily = df_daily.set_index('日期')
    df = df_daily.resample('YE').agg({
        '开盘': 'first', '收盘': 'last', '最高': 'max', '最低': 'min', 
        '成交量 (手)': 'sum', '换手率 (%)': 'last', '涨跌幅 (%)': 'last'
    }).dropna().reset_index()
    df["日期"] = df["日期"].dt.strftime('%Y-%m-%d')
    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量 (手)", "换手率 (%)", "涨跌幅 (%)"]]


def fetch_history(
    stock_code: str = "000852",
    start_date: str = "20000101",
    end_date: str = None,
    adjust: str = "qfq",
    market: str = None,
    period: str = "daily",
) -> pd.DataFrame:
    """
    统一获取 A 股 或 港股 历史数据
    支持周期：daily（日线）, weekly（周线）, monthly（月线）, quarterly（季线）, yearly（年线）
    """
    target_market = market or _detect_market(stock_code)
    end_date = end_date or datetime.now().strftime("%Y%m%d")
    
    period = period.lower()
    if period == "daily":
        return _fetch_daily(stock_code, start_date, end_date, adjust, target_market)
    elif period == "weekly":
        return _fetch_weekly(stock_code, start_date, end_date, adjust, target_market)
    elif period == "monthly":
        return _fetch_monthly(stock_code, start_date, end_date, adjust, target_market)
    elif period == "quarterly":
        return _fetch_quarterly(stock_code, start_date, end_date, adjust, target_market)
    elif period == "yearly":
        return _fetch_yearly(stock_code, start_date, end_date, adjust, target_market)
    else:
        raise ValueError(f"不支持的周期 [{period}]，请使用：daily/weekly/monthly/quarterly/yearly")


def export_to_csv(
    df: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    period: str = "daily",
    output_dir: str = None,
    filename: str = None,
) -> str:
    period_map = {
        "daily": "日 K", "weekly": "周 K", "monthly": "月 K",
        "quarterly": "季 K", "yearly": "年 K"
    }
    period_name = period_map.get(period, "日 K")
    
    output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
    filename = filename or f"{stock_code}_{stock_name}_{period_name}数据.csv"
    path = os.path.join(output_dir, filename)
    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out["日期"]):
        out["日期"] = out["日期"].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path


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
    
    period_map = {
        "daily": "日 K", "weekly": "周 K", "monthly": "月 K",
        "quarterly": "季 K", "yearly": "年 K"
    }
    period_name = period_map.get(period, "日 K")

    print("\n" + "=" * 90)
    print(f"  {stock_name}（{stock_code}）{market_name}{period_name}数据")
    print(f"  获取时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    df = fetch_history(
        stock_code=stock_code, 
        start_date=start_date, 
        end_date=end_date, 
        adjust=adjust, 
        market=market,
        period=period
    )
    
    print(f"\n（共 {len(df)} 条，终端仅展示最近 {preview_rows} 条）\n")
    show = df.tail(preview_rows).copy()
    if pd.api.types.is_datetime64_any_dtype(show["日期"]):
        show["日期"] = show["日期"].dt.strftime("%Y-%m-%d")
    print(show.to_string(index=False, na_rep="-"))

    csv_path = export_to_csv(
        df,
        stock_code=stock_code,
        stock_name=stock_name,
        period=period,
        output_dir=output_dir,
        filename=output_filename,
    )
    print(f"\n[导出成功] {csv_path}\n")
    return {"df": df, "csv_path": csv_path}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="【A/H 股通用】股票历史数据获取工具")
    parser.add_argument("--stock_code", default="000852", help="股票代码 (A 股 6 位/港股 5 位)")
    parser.add_argument("--stock_name", default="石化机械", help="股票名称 (用于文件名和展示)")
    parser.add_argument("--market", default=None, help="(可选) 强制指定市场：mainland/hk，不填则自动识别")
    parser.add_argument("--start_date", default="19900101", help="开始日期 (格式：YYYYMMDD)")
    parser.add_argument("--end_date", default=None, help="结束日期 (格式：YYYYMMDD，默认今天)")
    parser.add_argument("--adjust", default="qfq", help="复权方式：qfq(前复权)/hfq(后复权)/none(不复权)")
    parser.add_argument("--period", default="daily", help="数据周期：daily/weekly/monthly/quarterly/yearly")
    args = parser.parse_args()

    main(
        stock_code=args.stock_code,
        stock_name=args.stock_name,
        market=args.market,
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
        period=args.period
    )
