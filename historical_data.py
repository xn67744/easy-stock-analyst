# -*- coding: utf-8 -*-
"""
historical_data.py - 支持 A股/H股 历史数据获取与导出
"""

import os
import argparse
from datetime import datetime
import akshare as ak
import pandas as pd


def _to_symbol(stock_code: str) -> str:
    """（仅A股使用）添加 sz/sh 前缀"""
    return f"sz{stock_code}" if stock_code.startswith(("0", "3")) else f"sh{stock_code}"


def _detect_market(stock_code: str) -> str:
    """
    根据股票代码自动识别市场
    - A股：6位数字（0/3/6开头）
    - 港股：5位数字
    """
    code_len = len(stock_code)
    if code_len == 6:
        return "mainland"
    elif code_len == 5:
        return "hk"
    else:
        raise ValueError(
            f"无法自动识别股票代码 [{stock_code}] 的市场。\n"
            f"请手动传入 market 参数：'mainland'（A股）或 'hk'（港股）"
        )


def fetch_history(
    stock_code: str = "000852",
    start_date: str = "20000101",
    end_date: str = None,
    adjust: str = "qfq",
    market: str = None,
) -> pd.DataFrame:
    """
    统一获取 A股 或 港股 日线历史数据
    返回统一格式的 DataFrame，缺失字段（如港股换手率）填充为 -
    """
    # 1. 确定市场
    target_market = market or _detect_market(stock_code)
    end_date = end_date or datetime.now().strftime("%Y%m%d")

    # 2. 分市场获取数据
    if target_market == "mainland":
        # --- A股逻辑 (保持不变) ---
        symbol = _to_symbol(stock_code)
        df = ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )
        # 重命名 & 计算衍生指标
        df = df.reset_index(drop=True).rename(columns={
            "date": "日期", "open": "开盘", "high": "最高", 
            "low": "最低", "close": "收盘", "volume": "成交量(股)", 
            "turnover": "换手率"
        })
        df["日期"] = pd.to_datetime(df["日期"])
        df["成交量(手)"] = (pd.to_numeric(df["成交量(股)"], errors="coerce") // 100).fillna(0).astype(int)
        df["换手率(%)"] = (pd.to_numeric(df["换手率"], errors="coerce") * 100).round(4)
        df["涨跌幅(%)"] = pd.to_numeric(df["收盘"], errors="coerce").pct_change().mul(100).round(2)

    elif target_market == "hk":
        # --- 港股逻辑 (已修复) ---
        # 1. 港股接口不接受 start_date/end_date，先拿全量数据
        df = ak.stock_hk_daily(
            symbol=stock_code,
            adjust=adjust
        )
        
        # 2. 手动处理日期列并过滤
        df = df.reset_index(drop=True).rename(columns={
            "date": "日期", "open": "开盘", "high": "最高", 
            "low": "最低", "close": "收盘", "volume": "成交量(股)"
        })
        
        # 强制转换日期格式
        df["日期"] = pd.to_datetime(df["日期"])
        
        # 3. 在本地根据 start_date 和 end_date 过滤
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        df = df[(df["日期"] >= start_dt) & (df["日期"] <= end_dt)].copy()
        
        # 4. 计算衍生指标（注意：港股接口无换手率）
        df["成交量(手)"] = (pd.to_numeric(df["成交量(股)"], errors="coerce") // 100).fillna(0).astype(int)
        df["换手率(%)"] = pd.NA  # 港股无此字段，填充为空
        df["涨跌幅(%)"] = pd.to_numeric(df["收盘"], errors="coerce").pct_change().mul(100).round(2)

    else:
        raise ValueError(f"不支持的市场 [{target_market}]，请使用 'mainland' 或 'hk'")

    # 3. 统一返回列顺序
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
    market: str = None,
    preview_rows: int = 30,
    output_dir: str = None,
    output_filename: str = None,
):
    # 确定市场显示名称
    target_market = market or _detect_market(stock_code)
    market_name = "A股" if target_market == "mainland" else "港股"

    print("\n" + "=" * 90)
    print(f"  {stock_name}（{stock_code}）{market_name}历史日K线数据")
    print(f"  获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    # 获取数据
    df = fetch_history(
        stock_code=stock_code, 
        start_date=start_date, 
        end_date=end_date, 
        adjust=adjust, 
        market=market
    )
    
    # 终端预览
    print(f"\n（共 {len(df)} 条，终端仅展示最近 {preview_rows} 条）\n")
    show = df.tail(preview_rows).copy()
    show["日期"] = show["日期"].dt.strftime("%Y-%m-%d")
    print(show.to_string(index=False, na_rep="-"))  # na_rep 让空值显示为 -

    # 导出 CSV
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
    parser = argparse.ArgumentParser(description="【A/H股通用】股票历史数据获取工具")
    parser.add_argument("--stock_code", default="000852", help="股票代码 (A股6位/港股5位)")
    parser.add_argument("--stock_name", default="石化机械", help="股票名称 (用于文件名和展示)")
    parser.add_argument("--market", default=None, help="(可选) 强制指定市场: mainland/hk，不填则自动识别")
    parser.add_argument("--start_date", default="19900101", help="开始日期 (格式: YYYYMMDD)")
    parser.add_argument("--end_date", default=None, help="结束日期 (格式: YYYYMMDD，默认今天)")
    parser.add_argument("--adjust", default="qfq", help="复权方式: qfq(前复权)/hfq(后复权)/none(不复权)")
    args = parser.parse_args()

    main(
        stock_code=args.stock_code,
        stock_name=args.stock_name,
        market=args.market,
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust
    )