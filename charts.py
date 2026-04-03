# -*- coding: utf-8 -*-
"""
charts.py - OpenClaw Skill: 生成技术指标交互图
功能:
1) K线 + MA5/10/20/60 + 最高最低标注 + 波浪转折点
2) 成交量
3) MACD（红绿线柱，不用bar柱）
4) KDJ
5) BOLL 价格轨道 + K线蜡烛图 + 超买/超卖/背离标记（已移除BOLL %B子图）
6) 庄家分析：建仓VWAP、控盘成本、阶段判断
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.signal import argrelextrema

DEFAULT_CSV = "601012_隆基绿能_日 K数据.csv"
DEFAULT_NAME = "隆基绿能"
DEFAULT_CODE = "601012"
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


def get_cross_indices(fast_s: pd.Series, slow_s: pd.Series):
    valid = fast_s.notna() & slow_s.notna() & fast_s.shift(1).notna() & slow_s.shift(1).notna()
    golden = valid & (fast_s.shift(1) <= slow_s.shift(1)) & (fast_s > slow_s)
    dead = valid & (fast_s.shift(1) >= slow_s.shift(1)) & (fast_s < slow_s)
    return golden[golden].index.tolist(), dead[dead].index.tolist()


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
        rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.015,
        row_heights=[0.30, 0.09, 0.14, 0.14, 0.33],
        subplot_titles=[
            f"{stock_name}({stock_code}) K线+均线+波浪",
            "成交量", "MACD(12,26,9)", "KDJ(9,3,3)", "BOLL(20,2) + K线"
        ]
    )

    ma_trace_idx = []
    wave_trace_idx = []

    # Row1: K线 + 均线 + 波段标记
    hover_txt = [
        f"{d.strftime('%Y-%m-%d')}<br>开:{o:.2f} 收:{c:.2f}<br>高:{h:.2f} 低:{l:.2f}<br>量:{v:,.0f}手"
        for d, o, c, h, l, v in zip(df["日期"], df["开盘"], df["收盘"], df["最高"], df["最低"], df["成交量(手)"])
    ]
    fig.add_trace(go.Candlestick(
        x=x, open=df["开盘"], high=df["最高"], low=df["最低"], close=df["收盘"],
        increasing_line_color=RED, decreasing_line_color=GREEN,
        increasing_fillcolor=RED, decreasing_fillcolor=GREEN,
        name="K线", text=hover_txt, hoverinfo="text",
        legendgroup="row1", legendgrouptitle_text="K线图"
    ), row=1, col=1)
    # 提高主图悬停捕捉，保证在均线图区域也能逐K移动
    fig.add_trace(go.Scatter(
        x=x, y=df["收盘"], mode="lines",
        line=dict(color="rgba(0,0,0,0)", width=18),
        hoverinfo="skip", showlegend=False, legendgroup="row1"
    ), row=1, col=1)

    for ma, color in [("MA5", YELLOW), ("MA10", ORANGE), ("MA20", BLUE), ("MA60", PURPLE)]:
        if ma in df.columns:
            fig.add_trace(go.Scatter(
                x=x, y=df[ma], name=ma, line=dict(color=color, width=1.3),
                hoverinfo="skip",
                legendgroup="row1"
            ), row=1, col=1)
            ma_trace_idx.append(len(fig.data) - 1)

    # 主图金叉/死叉（MA5 与 MA20）
    if "MA5" in df.columns and "MA20" in df.columns:
        gx_idx, dx_idx = get_cross_indices(df["MA5"], df["MA20"])
        if gx_idx:
            fig.add_trace(go.Scatter(
                x=df.loc[gx_idx, "xi"], y=df.loc[gx_idx, "收盘"], mode="markers",
                name="均线金叉", marker=dict(symbol="triangle-up", size=10, color="#9be9a8"),
                hovertemplate="均线金叉<extra></extra>", legendgroup="row1"
            ), row=1, col=1)
        if dx_idx:
            fig.add_trace(go.Scatter(
                x=df.loc[dx_idx, "xi"], y=df.loc[dx_idx, "收盘"], mode="markers",
                name="均线死叉", marker=dict(symbol="triangle-down", size=10, color="#ffaba8"),
                hovertemplate="均线死叉<extra></extra>", legendgroup="row1"
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
    lows  = [p for p in pivots if p["type"] == "L"]
    if highs:
        fig.add_trace(go.Scatter(
            x=[p["idx"] for p in highs], y=[p["price"] for p in highs], mode="markers",
            name="波段高点", marker=dict(symbol="triangle-down", size=8, color="rgba(248,81,73,0.7)"),
            hovertemplate="波段高点:%{y:.2f}<extra></extra>",
            legendgroup="row1", visible=True
        ), row=1, col=1)
        wave_trace_idx.append(len(fig.data) - 1)
    if lows:
        fig.add_trace(go.Scatter(
            x=[p["idx"] for p in lows], y=[p["price"] for p in lows], mode="markers",
            name="波段低点", marker=dict(symbol="triangle-up", size=8, color="rgba(63,185,80,0.7)"),
            hovertemplate="波段低点:%{y:.2f}<extra></extra>",
            legendgroup="row1", visible=True
        ), row=1, col=1)
        wave_trace_idx.append(len(fig.data) - 1)
    if len(pivots) >= 2:
        fig.add_trace(go.Scatter(
            x=[p["idx"] for p in pivots], y=[p["price"] for p in pivots], mode="lines",
            name="波浪趋势线", line=dict(color="rgba(200,200,200,0.25)", width=1, dash="dot"),
            hoverinfo="skip", legendgroup="row1", visible=True
        ), row=1, col=1)
        wave_trace_idx.append(len(fig.data) - 1)

        for i in range(len(pivots) - 1):
            p0, p1 = pivots[i], pivots[i + 1]
            rise = p1["price"] >= p0["price"]
            arrow_color = "#f4c542" if rise else "#ffb347"
            fig.add_trace(go.Scatter(
                x=[p0["idx"], p1["idx"]],
                y=[p0["price"], p1["price"]],
                mode="lines+markers",
                line=dict(color=arrow_color, width=2.2),
                marker=dict(size=[0, 10], symbol=["circle", "triangle-up" if rise else "triangle-down"], color=arrow_color),
                name="波段箭头",
                showlegend=(i == 0),
                hovertemplate="波段: %{y:.2f}<extra></extra>",
                legendgroup="row1", visible=True
            ), row=1, col=1)
            wave_trace_idx.append(len(fig.data) - 1)

            pct = (p1["price"] / (p0["price"] + 1e-9) - 1) * 100
            pct_txt = f"{pct:+.2f}%"
            y_pad = (df["最高"].max() - df["最低"].min()) * 0.03
            pct_y = ((p0["price"] + p1["price"]) / 2) + (y_pad if rise else -y_pad)
            fig.add_trace(go.Scatter(
                x=[(p0["idx"] + p1["idx"]) / 2],
                y=[pct_y],
                mode="text",
                text=[pct_txt],
                textposition="middle center",
                textfont=dict(color="#9be9a8" if rise else "#ffaba8", size=10),
                name="波段涨跌幅",
                showlegend=False,
                hoverinfo="skip",
                legendgroup="row1",
                visible=True
            ), row=1, col=1)
            wave_trace_idx.append(len(fig.data) - 1)

    # Row2: 成交量
    vol_color = [RED if c >= o else GREEN for c, o in zip(df["收盘"], df["开盘"])]
    fig.add_trace(go.Bar(
        x=x, y=df["成交量(手)"], name="成交量", marker_color=vol_color, showlegend=False,
        customdata=[d.strftime("%Y-%m-%d") for d in df["日期"]],
        hovertemplate="%{customdata}<br>成交量:%{y:,.0f}手<extra></extra>",
        legendgroup="row2"
    ), row=2, col=1)

    # Row3: MACD 线柱（红绿柱不加图例，只显示DIF/DEA）
    xr, yr, xg, yg = _seg_lines(df["xi"], df["MACD_BAR"], 0)
    fig.add_trace(go.Scatter(x=xr, y=yr, mode="lines", name="MACD红柱",
        line=dict(color=RED, width=2), showlegend=False, legendgroup="row3"), row=3, col=1)
    fig.add_trace(go.Scatter(x=xg, y=yg, mode="lines", name="MACD绿柱",
        line=dict(color=GREEN, width=2), showlegend=False, legendgroup="row3"), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["DIF"], name="DIF",
        line=dict(color=BLUE, width=1.5), legendgroup="row3",
        legendgrouptitle_text="MACD"), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["DEA"], name="DEA",
        line=dict(color=ORANGE, width=1.5), legendgroup="row3"), row=3, col=1)

    gx_idx, dx_idx = get_cross_indices(df["DIF"], df["DEA"])
    if gx_idx:
        fig.add_trace(go.Scatter(
            x=df.loc[gx_idx, "xi"], y=df.loc[gx_idx, "DIF"], mode="markers",
            name="MACD金叉", marker=dict(symbol="diamond", size=8, color="#9be9a8"),
            hovertemplate="MACD金叉<extra></extra>", legendgroup="row3"
        ), row=3, col=1)
    if dx_idx:
        fig.add_trace(go.Scatter(
            x=df.loc[dx_idx, "xi"], y=df.loc[dx_idx, "DIF"], mode="markers",
            name="MACD死叉", marker=dict(symbol="diamond", size=8, color="#ffaba8"),
            hovertemplate="MACD死叉<extra></extra>", legendgroup="row3"
        ), row=3, col=1)
    fig.add_hline(y=0, line_color=GRID, line_width=1, row=3, col=1)

    # Row4: KDJ
    fig.add_trace(go.Scatter(x=x, y=df["K"], name="K",
        line=dict(color=BLUE, width=1.5), legendgroup="row4",
        legendgrouptitle_text="KDJ"), row=4, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["D"], name="D",
        line=dict(color=ORANGE, width=1.5), legendgroup="row4"), row=4, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["J"], name="J",
        line=dict(color=PURPLE, width=1, dash="dot"), legendgroup="row4"), row=4, col=1)

    kx_idx, sx_idx = get_cross_indices(df["K"], df["D"])
    if kx_idx:
        fig.add_trace(go.Scatter(
            x=df.loc[kx_idx, "xi"], y=df.loc[kx_idx, "K"], mode="markers",
            name="KDJ金叉", marker=dict(symbol="triangle-up", size=8, color="#9be9a8"),
            hovertemplate="KDJ金叉<extra></extra>", legendgroup="row4"
        ), row=4, col=1)
    if sx_idx:
        fig.add_trace(go.Scatter(
            x=df.loc[sx_idx, "xi"], y=df.loc[sx_idx, "K"], mode="markers",
            name="KDJ死叉", marker=dict(symbol="triangle-down", size=8, color="#ffaba8"),
            hovertemplate="KDJ死叉<extra></extra>", legendgroup="row4"
        ), row=4, col=1)
    fig.add_hline(y=80, line_color=RED, line_dash="dash", row=4, col=1)
    fig.add_hline(y=20, line_color=GREEN, line_dash="dash", row=4, col=1)

    # Row5: BOLL 轨道 + K线蜡烛（替换收盘折线）
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_UP"], name="上轨",
        line=dict(color=RED, width=1.2, dash="dash"), legendgroup="row5",
        legendgrouptitle_text="BOLL"), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_MID"], name="中轨",
        line=dict(color=YELLOW, width=1.2), legendgroup="row5"), row=5, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["BOLL_LOW"], name="下轨",
        line=dict(color=GREEN, width=1.2, dash="dash"),
        fill="tonexty", fillcolor="rgba(63,185,80,0.05)",
        legendgroup="row5"), row=5, col=1)
    # BOLL子图内嵌K线蜡烛（不重复图例）
    fig.add_trace(go.Candlestick(
        x=x, open=df["开盘"], high=df["最高"], low=df["最低"], close=df["收盘"],
        increasing_line_color=RED, decreasing_line_color=GREEN,
        increasing_fillcolor=RED, decreasing_fillcolor=GREEN,
        name="K线", showlegend=False, opacity=0.65,
        legendgroup="row5"
    ), row=5, col=1)
    # BOLL信号标记（不重复图例）
    seen_boll_sigs = set()
    for i, nm, col, tip in get_boll_signals(df):
        show_leg = nm not in seen_boll_sigs
        seen_boll_sigs.add(nm)
        fig.add_trace(go.Scatter(
            x=[df.loc[i, "xi"]], y=[df.loc[i, "收盘"]], mode="markers",
            name=f"BOLL{nm}", marker=dict(symbol="star", size=9, color=col),
            hovertemplate=tip+"<extra></extra>",
            showlegend=show_leg, legendgroup="row5"
        ), row=5, col=1)



    show_ma_mask = [tr.visible if tr.visible is not None else True for tr in fig.data]
    for i in wave_trace_idx:
        show_ma_mask[i] = "legendonly"

    show_wave_mask = [tr.visible if tr.visible is not None else True for tr in fig.data]
    for i in ma_trace_idx:
        show_wave_mask[i] = "legendonly"
    for i in wave_trace_idx:
        show_wave_mask[i] = True

    fig.update_layout(
        title=dict(text=f"{stock_name}({stock_code}) 技术指标综合图表", x=0.5, font=dict(size=18, color=TEXT)),
        paper_bgcolor=PAPER, plot_bgcolor=BG, font=dict(color=TEXT, size=11),
        height=1200, margin=dict(l=60, r=320, t=80, b=95),
        hovermode="x", hoverdistance=-1, spikedistance=-1,
        xaxis_rangeslider_visible=False,
        legend=dict(
            bgcolor="rgba(22,27,34,0.92)", bordercolor=GRID, borderwidth=1,
            font=dict(size=9), tracegroupgap=12,
            x=1.01, y=1, xanchor="left", yanchor="top"
        ),
        updatemenus=[
            dict(
                type="buttons",
                direction="down",
                x=0.5, y=-0.08,
                xanchor="center", yanchor="top",
                bgcolor="#dbeafe", bordercolor="#93c5fd", borderwidth=1,
                active=1, showactive=True,
                font=dict(color="#0f172a", size=10),
                buttons=[
                    dict(label="显示均线(隐藏波段)", method="update", args=[{"visible": show_ma_mask}]),
                    dict(label="显示波段(隐藏均线)", method="update", args=[{"visible": show_wave_mask}])
                ]
            )
        ]
    )

    fig.add_annotation(
        x=1.01, y=0.72, xref="paper", yref="paper",
        text=(
            f"<b>固定看板</b><br>{df.iloc[-1]['日期'].strftime('%Y-%m-%d')}"
            f"<br>开盘: {df.iloc[-1]['开盘']:.2f}"
            f"<br>收盘: {df.iloc[-1]['收盘']:.2f}"
            f"<br>最高: {df.iloc[-1]['最高']:.2f}"
            f"<br>最低: {df.iloc[-1]['最低']:.2f}"
            f"<br>成交量: {df.iloc[-1]['成交量(手)']:,.0f}手"
            f"<br><br>MA5/10/20/60: - / - / - / -"
            f"<br>MACD DIF/DEA/柱: - / - / -"
            f"<br>KDJ K/D/J: - / - / -"
            f"<br>BOLL 上/中/下: - / - / -"
            f"<br>BOLL %B: -"
        ),
        showarrow=False, align="left", font=dict(color=TEXT, size=10),
        bgcolor="rgba(22,27,34,0.95)", bordercolor=GRID, borderwidth=1
    )
    for r in range(1, 6):
        fig.update_xaxes(
            row=r, col=1,
            gridcolor=GRID, tickvals=tickvals, ticktext=ticktext,
            rangeslider_visible=False,
            showspikes=False
        )
        fig.update_yaxes(
            row=r, col=1, gridcolor=GRID,
            showspikes=False
        )
    for r in range(1, 5):
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

    fig.write_html(
        output_html,
        include_plotlyjs="cdn",
        config={"editable": True, "scrollZoom": True, "displaylogo": False}
    )
    _inject_hover_board_script(output_html)
    print(f"  [生成成功] {output_html}")
    print("  请用浏览器打开HTML文件查看交互图表")
    print()
    return {"output_html": output_html, "zhuang": z}


def _inject_hover_board_script(output_html):
    script = """
<!-- fixed-board-hook -->
<script>
(function() {
  const gd = document.querySelector('.plotly-graph-div');
  if (!gd || !window.Plotly) return;

  const style = document.createElement('style');
  style.innerHTML = '.hoverlayer .hovertext{display:none !important;}';
  document.head.appendChild(style);

  function ensureCrosshairShape() {
    const shapes = (gd.layout && gd.layout.shapes) ? gd.layout.shapes.slice() : [];
    const idx = shapes.findIndex(s => s && s.name === 'cursor-vline');
    if (idx >= 0) return idx;
    shapes.push({
      type: 'line',
      name: 'cursor-vline',
      xref: 'x',
      yref: 'paper',
      x0: 0,
      x1: 0,
      y0: 0,
      y1: 1,
      line: { color: '#f4c542', width: 1.2, dash: 'solid' }
    });
    Plotly.relayout(gd, { shapes });
    return shapes.length - 1;
  }

  function ensureMainHLineShape() {
    const shapes = (gd.layout && gd.layout.shapes) ? gd.layout.shapes.slice() : [];
    const idx = shapes.findIndex(s => s && s.name === 'cursor-hline-main');
    if (idx >= 0) return idx;
    shapes.push({
      type: 'line',
      name: 'cursor-hline-main',
      xref: 'x domain',
      yref: 'y',
      x0: 0,
      x1: 1,
      y0: 0,
      y1: 0,
      line: { color: '#f4c542', width: 1, dash: 'dot' }
    });
    Plotly.relayout(gd, { shapes });
    return shapes.length - 1;
  }

  function ensurePriceLabelAnnotation() {
    const anns = (gd.layout && gd.layout.annotations) ? gd.layout.annotations.slice() : [];
    const idx = anns.findIndex(a => a && a.name === 'cursor-price-label');
    if (idx >= 0) return idx;
    anns.push({
      name: 'cursor-price-label',
      xref: 'paper',
      yref: 'y',
      x: 0,
      y: 0,
      xanchor: 'right',
      yanchor: 'middle',
      text: '-',
      showarrow: false,
      font: { color: '#f4c542', size: 10 },
      bgcolor: 'rgba(13,17,23,0.92)',
      bordercolor: '#f4c542',
      borderwidth: 1,
      align: 'right'
    });
    Plotly.relayout(gd, { annotations: anns });
    return anns.length - 1;
  }

  function updateCrosshairXi(xi) {
    const k = getMainCandle();
    if (!k || !k.close || !k.close.length) return null;

    const i = Math.max(0, Math.min(k.close.length - 1, Math.round(Number(xi))));
    const px = Number(k.close[i]);

    const vIdx = ensureCrosshairShape();
    const hIdx = ensureMainHLineShape();
    const aIdx = ensurePriceLabelAnnotation();

    Plotly.relayout(gd, {
      ['shapes[' + vIdx + '].x0']: i,
      ['shapes[' + vIdx + '].x1']: i,
      ['shapes[' + hIdx + '].y0']: px,
      ['shapes[' + hIdx + '].y1']: px,
      ['annotations[' + aIdx + '].y']: px,
      ['annotations[' + aIdx + '].text']: px.toFixed(2)
    });

    return i;
  }

  function getTraceByName(name) {
    return (gd.data || []).find(t => t && t.name === name);
  }

  function num(v, n=2) {
    if (v == null || Number.isNaN(Number(v))) return '-';
    return Number(v).toFixed(n);
  }

  function fmtVol(v) {
    if (v == null || Number.isNaN(Number(v))) return '-';
    return Number(v).toLocaleString('en-US', {maximumFractionDigits: 0});
  }

  function parseDateVolFromText(txt) {
    const s = String(txt || '');
    const dt = (s.split('<br>')[0] || '-').trim();
    const m = s.match(/量:([\d,]+)手/);
    const vol = m ? m[1] : '-';
    return { dt, vol };
  }

  function getMainCandle() {
    return (gd.data || []).find(t => t && t.type === 'candlestick' && t.legendgroup === 'row1');
  }

  function formatBoardByXi(xi) {
    const i = Math.max(0, Math.round(Number(xi)));
    const k = getMainCandle();
    if (!k || !k.open || k.open[i] == null) return null;

    const tv = parseDateVolFromText(k.text && k.text[i]);
    const dt = tv.dt;
    const o = k.open[i], c = k.close[i], h = k.high[i], l = k.low[i];
    const vol = tv.vol;

    const ma5 = getTraceByName('MA5');
    const ma10 = getTraceByName('MA10');
    const ma20 = getTraceByName('MA20');
    const ma60 = getTraceByName('MA60');
    const dif = getTraceByName('DIF');
    const dea = getTraceByName('DEA');
    const kLine = getTraceByName('K');
    const dLine = getTraceByName('D');
    const jLine = getTraceByName('J');
    const bollUp = getTraceByName('上轨');
    const bollMid = getTraceByName('中轨');
    const bollLow = getTraceByName('下轨');

    const v_ma5 = ma5 && ma5.y ? ma5.y[i] : null;
    const v_ma10 = ma10 && ma10.y ? ma10.y[i] : null;
    const v_ma20 = ma20 && ma20.y ? ma20.y[i] : null;
    const v_ma60 = ma60 && ma60.y ? ma60.y[i] : null;

    const v_dif = dif && dif.y ? dif.y[i] : null;
    const v_dea = dea && dea.y ? dea.y[i] : null;
    const v_macd = (v_dif != null && v_dea != null) ? 2 * (Number(v_dif) - Number(v_dea)) : null;

    const v_k = kLine && kLine.y ? kLine.y[i] : null;
    const v_d = dLine && dLine.y ? dLine.y[i] : null;
    const v_j = jLine && jLine.y ? jLine.y[i] : null;

    const v_bu = bollUp && bollUp.y ? bollUp.y[i] : null;
    const v_bm = bollMid && bollMid.y ? bollMid.y[i] : null;
    const v_bl = bollLow && bollLow.y ? bollLow.y[i] : null;
    const v_pb = (v_bu != null && v_bl != null && Number(v_bu) !== Number(v_bl))
      ? (Number(c) - Number(v_bl)) / (Number(v_bu) - Number(v_bl))
      : null;

    return `<b>固定看板</b><br>${dt}`
      + `<br>开盘: ${num(o)} | 收盘: ${num(c)}`
      + `<br>最高: ${num(h)} | 最低: ${num(l)}`
      + `<br>成交量: ${vol}手`
      + `<br><br>MA5/10/20/60: ${num(v_ma5)} / ${num(v_ma10)} / ${num(v_ma20)} / ${num(v_ma60)}`
      + `<br>MACD DIF/DEA/柱: ${num(v_dif)} / ${num(v_dea)} / ${num(v_macd)}`
      + `<br>KDJ K/D/J: ${num(v_k)} / ${num(v_d)} / ${num(v_j)}`
      + `<br>BOLL 上/中/下: ${num(v_bu)} / ${num(v_bm)} / ${num(v_bl)}`
      + `<br>BOLL %B: ${num(v_pb, 3)}`;
  }

  gd.on('plotly_hover', function(ev) {
    if (!ev || !ev.points || !ev.points.length) return;
    const p0 = ev.points[0];
    const xi = p0.x;
    if (xi == null) return;

    const idxXi = updateCrosshairXi(xi);
    if (idxXi == null) return;

    const anns = (gd.layout && gd.layout.annotations) ? gd.layout.annotations : [];
    const idx = anns.findIndex(a => (a.text || '').includes('固定看板'));
    if (idx < 0) return;

    const board = formatBoardByXi(idxXi);
    if (!board) return;
    Plotly.relayout(gd, {['annotations[' + idx + '].text']: board});
  });
})();
</script>
"""
    with open(output_html, "r", encoding="utf-8") as f:
        html = f.read()
    if "fixed-board-hook" in html:
        return
    html = html.replace("</body>", script + "\n</body>")
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
