# -*- coding: utf-8 -*-
"""
charts.py - OpenClaw Skill: 生成技术指标交互图
功能:
1) K线 + MA5/10/20/60 + 最高最低标注 + 波浪转折点
2) 成交量
3) MACD（红绿线柱，不用bar柱）
4) KDJ
5) BOLL 价格轨道 + 超买/超卖/背离标记
6) BOLL %B 线柱 + 带宽 BW%
7) 庄家分析：建仓VWAP、控盘成本、阶段判断
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.signal import argrelextrema

DEFAULT_CSV = "000852_石化机械_历史数据.csv"
DEFAULT_NAME = "石化机械"
DEFAULT_CODE = "000852"
DEFAULT_RECENT = 250

BG = "#0d1117"
PAPER = "#161b22"
GRID = "#21262d"
TEXT = "#c9d1d9"
RED = "#f85149"
GREEN = "#3fb950"
YELLOW = "#d29922"
BLUE = "#58a6ff"
PURPLE = "#bc8cff"
ORANGE = "#ffa657"
CYAN = "#39d353"


def load_data(csv_file=DEFAULT_CSV, recent=DEFAULT_RECENT):
    df = pd.read_csv(csv_file, encoding="utf-8-sig")
    df.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量(手)", "换手率(%)", "涨跌幅(%)"]
    df["日期"] = pd.to_datetime(df["日期"])
    for c in ["开盘", "收盘", "最高", "最低", "成交量(手)", "换手率(%)", "涨跌幅(%)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("日期").reset_index(drop=True)
    if recent:
        df = df.tail(recent).reset_index(drop=True)
    df["xi"] = np.arange(len(df))  # 连续x轴，消除非交易日空隙
    return df


def calc_indicators(df, ma_periods=(5, 10, 20, 60)):
    c = df["收盘"]
    for n in ma_periods:
        df[f"MA{n}"] = c.rolling(n).mean()

    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_BAR"] = 2 * (df["DIF"] - df["DEA"])

    low9 = df["最低"].rolling(9).min()
    high9 = df["最高"].rolling(9).max()
    rsv = (c - low9) / (high9 - low9 + 1e-9) * 100
    df["K"] = rsv.ewm(com=2, adjust=False).mean()
    df["D"] = df["K"].ewm(com=2, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    df["BOLL_MID"] = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    df["BOLL_UP"] = df["BOLL_MID"] + 2 * std20
    df["BOLL_LOW"] = df["BOLL_MID"] - 2 * std20
    df["BOLL_BW"] = (df["BOLL_UP"] - df["BOLL_LOW"]) / (df["BOLL_MID"] + 1e-9) * 100
    df["BOLL_PB"] = (c - df["BOLL_LOW"]) / (df["BOLL_UP"] - df["BOLL_LOW"] + 1e-9)
    return df


def _seg_lines(x_arr, y_arr, base):
    xr, yr, xg, yg = [], [], [], []
    for x, y in zip(x_arr, y_arr):
        if pd.isna(y):
            continue
        if y >= base:
            xr += [x, x, None]
            yr += [base, y, None]
        else:
            xg += [x, x, None]
            yg += [base, y, None]
    return xr, yr, xg, yg


def find_wave_pivots(df, order=8):
    prices = df["收盘"].values
    hi = argrelextrema(prices, np.greater_equal, order=order)[0]
    lo = argrelextrema(prices, np.less_equal, order=order)[0]
    pts = [{"idx": i, "price": prices[i], "type": "H"} for i in hi] + [{"idx": i, "price": prices[i], "type": "L"} for i in lo]
    pts = sorted(pts, key=lambda z: z["idx"])
    clean = []
    for p in pts:
        if clean and clean[-1]["type"] == p["type"]:
            if p["type"] == "H" and p["price"] > clean[-1]["price"]:
                clean[-1] = p
            elif p["type"] == "L" and p["price"] < clean[-1]["price"]:
                clean[-1] = p
        else:
            clean.append(p)
    return clean


def get_boll_signals(df, w=20):
    sigs = []
    for i in df.index:
        pb = df.loc[i, "BOLL_PB"]
        if pd.isna(pb):
            continue
        if pb > 1.0:
            sigs.append((i, "超买", RED, f"BOLL超买: %B={pb:.2f}>1"))
        elif pb < 0.0:
            sigs.append((i, "超卖", GREEN, f"BOLL超卖: %B={pb:.2f}<0"))

    for k in range(w, len(df)):
        i = df.index[k]
        seg = df.iloc[k - w:k + 1]
        if df.loc[i, "收盘"] == seg["收盘"].max() and df.loc[i, "BOLL_PB"] < 0.85:
            sigs.append((i, "顶背离", ORANGE, "BOLL顶背离: 创新高但未触上轨"))
        if df.loc[i, "收盘"] == seg["收盘"].min() and df.loc[i, "BOLL_PB"] > 0.15:
            sigs.append((i, "底背离", CYAN, "BOLL底背离: 创新低但未触下轨"))
    return sigs


def analyze_zhuang(df):
    c = df["收盘"]
    v = df["成交量(手)"]
    tr = df["换手率(%)"]

    low_zone = c <= c.quantile(0.25) * 1.1
    build = df[low_zone & (v > 0)]
    out = {}
    if len(build) > 5:
        vwap = (build["收盘"] * build["成交量(手)"]).sum() / build["成交量(手)"].sum()
        cum_tr = build["换手率(%)"].sum()
        out["build_vwap"] = float(vwap)
        out["build_cost"] = float(vwap * 1.03)
        out["cum_turnover"] = float(cum_tr)
        out["build_complete"] = bool(cum_tr >= 150)
        out["build_start"] = build["日期"].min().strftime("%Y-%m-%d")
        out["build_end"] = build["日期"].max().strftime("%Y-%m-%d")

    cur = float(c.iloc[-1])
    ma20 = float(c.rolling(20).mean().iloc[-1])
    ma60 = float(c.rolling(60).mean().iloc[-1])
    h60 = float(c.tail(60).max())
    l60 = float(c.tail(60).min())
    vr = float(v.tail(20).mean() / (v.tail(60).head(40).mean() + 1e-9))
    rise = (cur - l60) / (l60 + 1e-9) * 100
    fall = (h60 - cur) / (h60 + 1e-9) * 100

    if rise > 20 and vr > 1.3 and cur > ma20:
        phase, tip = "拉升期", "放量上涨+突破，庄家拉升特征"
    elif fall > 15 and vr > 1.5 and cur < ma20:
        phase, tip = "出货期", "高位放量下跌，庄家派发特征"
    elif cur <= ma60 * 1.05 and vr < 0.9:
        phase, tip = "建仓期", "低位横盘+温和换手，庄家吸筹特征"
    else:
        phase, tip = "洗盘期", "区间震荡清洗浮筹"

    out.update({
        "phase": phase,
        "phase_tip": tip,
        "current_price": cur,
        "ma20": ma20,
        "ma60": ma60,
        "vol_ratio": vr,
        "rise_from_low": float(rise),
        "fall_from_high": float(fall),
    })
    return out


def build_chart(df, stock_name=DEFAULT_NAME, stock_code=DEFAULT_CODE):
    x = df["xi"].tolist()
    tick_step = max(1, len(df) // 12)
    tickvals = list(range(0, len(df), tick_step))
    if tickvals[-1] != len(df)-1:
        tickvals.append(len(df)-1)
    ticktext = [df.iloc[i]["日期"].strftime("%m-%d") for i in tickvals]

    fig = make_subplots(
        rows=6, cols=1, shared_xaxes=True, vertical_spacing=0.015,
        row_heights=[0.30, 0.09, 0.14, 0.14, 0.16, 0.17],
        subplot_titles=[
            f"{stock_name}({stock_code}) K线+均线+波浪",
            "成交量", "MACD(12,26,9)", "KDJ(9,3,3)", "BOLL(20,2)", "BOLL %B + 带宽"
        ]
    )

    # Row1 K线
    hover_txt = [
        f"{d.strftime('%Y-%m-%d')}<br>开:{o:.2f} 收:{c:.2f}<br>高:{h:.2f} 低:{l:.2f}<br>量:{v:,.0f}手"
        for d, o, c, h, l, v in zip(df["日期"], df["开盘"], df["收盘"], df["最高"], df["最低"], df["成交量(手)"])
    ]
    fig.add_trace(go.Candlestick(
        x=x, open=df["开盘"], high=df["最高"], low=df["最低"], close=df["收盘"],
        increasing_line_color=RED, decreasing_line_color=GREEN,
        increasing_fillcolor=RED, decreasing_fillcolor=GREEN,
        name="K线", text=hover_txt, hoverinfo="text"
    ), row=1, col=1)

    for ma, color in [("MA5", YELLOW), ("MA10", ORANGE), ("MA20", BLUE), ("MA60", PURPLE)]:
        if ma in df.columns:
            fig.add_trace(go.Scatter(
                x=x, y=df[ma], name=ma, line=dict(color=color, width=1.3),
                hovertemplate=f"{ma}<br>值:%{{y:.3f}}<extra></extra>"
            ), row=1, col=1)

    max_i = int(df["最高"].idxmax())
    min_i = int(df["最低"].idxmin())
    fig.add_annotation(x=df.loc[max_i, "xi"], y=df.loc[max_i, "最高"],
        text=f"最高 {df.loc[max_i, '最高']:.2f}", showarrow=True, arrowhead=2, arrowcolor=RED,
        font=dict(color=RED, size=10), ay=-25, row=1, col=1)
    fig.add_annotation(x=df.loc[min_i, "xi"], y=df.loc[min_i, "最低"],
        text=f"最低 {df.loc[min_i, '最低']:.2f}", showarrow=True, arrowhead=2, arrowcolor=GREEN,
        font=dict(color=GREEN, size=10), ay=25, row=1, col=1)

    pivots = find_wave_pivots(df, order=8)
    highs = [p for p in pivots if p["type"] == "H"]
    lows = [p for p in pivots if p["type"] == "L"]
    if highs:
        fig.add_trace(go.Scatter(
            x=[p["idx"] for p in highs], y=[p["price"] for p in highs], mode="markers",
            name="波段高点", marker=dict(symbol="triangle-down", size=8, color="rgba(248,81,73,0.7)"),
            hovertemplate="波段高点:%{y:.2f}<extra></extra>"
        ), row=1, col=1)
    if lows:
        fig.add_trace(go.Scatter(
            x=[p["idx"] for p in lows], y=[p["price"] for p in lows], mode="markers",
            name="波段低点", marker=dict(symbol="triangle-up", size=8, color="rgba(63,185,80,0.7)"),
            hovertemplate="波段低点:%{y:.2f}<extra></extra>"
        ), row=1, col=1)
    if len(pivots) >= 2:
        fig.add_trace(go.Scatter(
            x=[p["idx"] for p in pivots], y=[p["price"] for p in pivots], mode="lines",
            name="波浪趋势线", line=dict(color="rgba(200,200,200,0.25)", width=1, dash="dot"), hoverinfo="skip"
        ), row=1, col=1)

    # Row2 成交量
    vol_color = [RED if c >= o else GREEN for c, o in zip(df["收盘"], df["开盘"])]
    fig.add_trace(go.Bar(
        x=x, y=df["成交量(手)"], name="成交量", marker_color=vol_color, showlegend=False,
        customdata=[d.strftime("%Y-%m-%d") for d in df["日期"]],
        hovertemplate="%{customdata}<br>成交量:%{y:,.0f}手<extra></extra>"
    ), row=2, col=1)

    # Row3 MACD 线柱
    xr, yr, xg, yg = _seg_lines(df["xi"], df["MACD_BAR"], 0)
    fig.add_trace(go.Scatter(x=xr, y=yr, mode="lines", name="MACD红柱", line=dict(color=RED, width=2)), row=3, col=1)
    fig.add_trace(go.Scatter(x=xg, y=yg, mode="lines", name="MACD绿柱", line=dict(color=GREEN, width=2)), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["DIF"], name="DIF", line=dict(color=BLUE, width=1.5)), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["DEA"], name="DEA", line=dict(color=ORANGE, width=1.5)), row=3, col=1)
    fig.add_hline(y=0, line_color=GRID, line_width=1, row=3, col=1)

    # Row4 KDJ
    fig.add_trace(go.Scatter(x=x, y=df["K"], name="K", line=dict(color=BLUE, width=1.5)), row=4, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["D"], name="D", line=dict(color=ORANGE, width=1.5)), row=4, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["J"], name="J", line=dict(color=PURPLE, width=1, dash="dot")), row=4, col=1)
    fig.add_hline(y=80, line_color=RED, line_dash="dash", row=4, col=1)
    fig.add_hline(y=20, line_color=GREEN, line_dash="dash", row=4, col=1)

    # Row5 BOLL
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_UP"], name="上轨", line=dict(color=RED, width=1.2, dash="dash")), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_MID"], name="中轨", line=dict(color=YELLOW, width=1.2)), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_LOW"], name="下轨", line=dict(color=GREEN, width=1.2, dash="dash"), fill="tonexty", fillcolor="rgba(63,185,80,0.05)"), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["收盘"], name="收盘(BOLL)", line=dict(color=TEXT, width=0.9)), row=5, col=1)
    for i, nm, col, tip in get_boll_signals(df):
        fig.add_trace(go.Scatter(x=[df.loc[i, "xi"]], y=[df.loc[i, "收盘"]], mode="markers", name=f"BOLL{nm}",
            marker=dict(symbol="star", size=9, color=col), hovertemplate=tip+"<extra></extra>"), row=5, col=1)

    # Row6 BOLL %B + 带宽
    xr2, yr2, xg2, yg2 = _seg_lines(df["xi"], df["BOLL_PB"], 0.5)
    fig.add_trace(go.Scatter(x=xr2, y=yr2, mode="lines", name="%B红柱", line=dict(color=RED, width=2)), row=6, col=1)
    fig.add_trace(go.Scatter(x=xg2, y=yg2, mode="lines", name="%B绿柱", line=dict(color=GREEN, width=2)), row=6, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_BW"], name="带宽BW%", line=dict(color=PURPLE, width=1.3)), row=6, col=1)
    fig.add_hline(y=1.0, line_color=RED, line_dash="dash", row=6, col=1)
    fig.add_hline(y=0.0, line_color=GREEN, line_dash="dash", row=6, col=1)
    fig.add_hline(y=0.5, line_color=GRID, row=6, col=1)

    fig.update_layout(
        title=dict(text=f"{stock_name}({stock_code}) 技术指标综合图表", x=0.5, font=dict(size=18, color=TEXT)),
        paper_bgcolor=PAPER, plot_bgcolor=BG, font=dict(color=TEXT, size=11),
        height=1400, margin=dict(l=60, r=60, t=80, b=40),
        hovermode="x", xaxis_rangeslider_visible=False,
        legend=dict(bgcolor="rgba(22,27,34,0.85)", bordercolor=GRID, borderwidth=1, font=dict(size=9))
    )
    for r in range(1, 7):
        fig.update_xaxes(row=r, col=1, gridcolor=GRID, tickvals=tickvals, ticktext=ticktext)
        fig.update_yaxes(row=r, col=1, gridcolor=GRID)
    for r in range(1, 6):
        fig.update_xaxes(row=r, col=1, showticklabels=False)

    return fig


def main(csv_file=DEFAULT_CSV, stock_name=DEFAULT_NAME, stock_code=DEFAULT_CODE,
         recent=DEFAULT_RECENT, output_html=None, ma_periods=(5, 10, 20, 60)):
    if output_html is None:
        output_html = f"{stock_code}_{stock_name}_技术指标图.html"

    print("\n" + "=" * 70)
    print(f"  {stock_name}({stock_code}) 技术指标图表生成")
    print("=" * 70)

    df_full = calc_indicators(load_data(csv_file, None), ma_periods)
    z = analyze_zhuang(df_full)
    print("  [庄家分析]")
    if "build_vwap" in z:
        print(f"    建仓期间: {z['build_start']} -> {z['build_end']}")
        print(f"    建仓均价VWAP: {z['build_vwap']:.3f} 元")
        print(f"    控盘成本估算: {z['build_cost']:.3f} 元")
        print(f"    累计换手率: {z['cum_turnover']:.1f}%")
    print(f"    当前阶段: [{z['phase']}] {z['phase_tip']}")
    print(f"    当前: {z['current_price']:.3f} | MA20: {z['ma20']:.3f} | MA60: {z['ma60']:.3f}")

    df = calc_indicators(load_data(csv_file, recent), ma_periods)
    fig = build_chart(df, stock_name, stock_code)

    if "build_vwap" in z:
        fig.add_hline(y=z["build_vwap"], line_color=CYAN, line_dash="dash", row=1, col=1,
                      annotation_text=f"建仓均价 {z['build_vwap']:.3f}", annotation_font_color=CYAN)
        fig.add_hline(y=z["build_cost"], line_color=ORANGE, line_dash="dot", row=1, col=1,
                      annotation_text=f"控盘成本 {z['build_cost']:.3f}", annotation_font_color=ORANGE)

    fig.write_html(output_html, include_plotlyjs="cdn")
    print(f"  [生成成功] {output_html}")
    print("  请用浏览器打开HTML文件查看交互图表")
    print()
    return {"output_html": output_html, "zhuang": z}


if __name__ == "__main__":
    main()
