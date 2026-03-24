# -*- coding: utf-8 -*-
"""
historical_data.py - OpenClaw Skill: 历史数据获取与导出
"""

import os
from datetime import datetime
import akshare as ak
import pandas as pd


def _to_symbol(stock_code: str) -> str:
    return f"sz{stock_code}" if stock_code.startswith(("0", "3")) else f"sh{stock_code}"


def fetch_history(
    stock_code: str = "000852",
    start_date: str = "20000101",
    end_date: str = None,
    adjust: str = "qfq",
) -> pd.DataFrame:
    """获取A股日线历史数据"""
    symbol = _to_symbol(stock_code)
    end_date = end_date or datetime.now().strftime("%Y%m%d")
    df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
    df = df.reset_index(drop=True).rename(columns={
        "date": "日期",
        "open": "开盘",
        "high": "最高",
        "low": "最低",
        "close": "收盘",
        "volume": "成交量(股)",
        "turnover": "换手率",
    })
    df["日期"] = pd.to_datetime(df["日期"])
    df["成交量(手)"] = (pd.to_numeric(df["成交量(股)"], errors="coerce") // 100).fillna(0).astype(int)
    df["换手率(%)"] = (pd.to_numeric(df["换手率"], errors="coerce") * 100).round(4)
    df["涨跌幅(%)"] = pd.to_numeric(df["收盘"], errors="coerce").pct_change().mul(100).round(2)
    return df[["日期", "开盘", "收盘", "最高", "最低", "成交量(手)", "换手率(%)", "涨跌幅(%)"]]


def export_to_csv(
    df: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    output_dir: str = None,
    filename: str = None,
) -> str:
    output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
    filename = filename or f"{stock_code}_{stock_name}_历史数据.csv"
    path = os.path.join(output_dir, filename)
    out = df.copy()
    out["日期"] = out["日期"].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def main(
    stock_code: str = "000852",
    stock_name: str = "石化机械",
    start_date: str = "20000101",
    end_date: str = None,
    adjust: str = "qfq",
    preview_rows: int = 30,
    output_dir: str = None,
    output_filename: str = None,
):
    print("\n" + "=" * 90)
    print(f"  {stock_name}（{stock_code}）历史日K线数据")
    print(f"  获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    df = fetch_history(stock_code=stock_code, start_date=start_date, end_date=end_date, adjust=adjust)
    print(f"\n（共 {len(df)} 条，终端仅展示最近 {preview_rows} 条）\n")
    show = df.tail(preview_rows).copy()
    show["日期"] = show["日期"].dt.strftime("%Y-%m-%d")
    print(show.to_string(index=False))

    csv_path = export_to_csv(
        df,
        stock_code=stock_code,
        stock_name=stock_name,
        output_dir=output_dir,
        filename=output_filename,
    )
    print(f"\n[导出成功] {csv_path}\n")
    return {"df": df, "csv_path": csv_path}


if __name__ == "__main__":
    main()
