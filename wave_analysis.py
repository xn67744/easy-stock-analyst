# -*- coding: utf-8 -*-
"""
Demo 3: 均线分析 + 波浪理论波段识别
数据来源: 本地CSV文件
"""

import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import warnings
warnings.filterwarnings('ignore')

CSV_FILE   = "000852_石化机械_历史数据.csv"
STOCK_NAME = "石化机械"
STOCK_CODE = "000852"


def load_data(csv_file: str, recent: int = None) -> pd.DataFrame:
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    df.columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量(手)', '换手率(%)', '涨跌幅(%)']
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values('日期').reset_index(drop=True)
    for col in ['开盘', '收盘', '最高', '最低']:
        df[col] = df[col].astype(float)
    df['成交量(手)'] = df['成交量(手)'].astype(float)
    if recent:
        df = df.tail(recent).reset_index(drop=True)
    return df


MA_PERIODS = [5, 10, 20, 60, 120, 250]

def calc_ma(df: pd.DataFrame) -> pd.DataFrame:
    for n in MA_PERIODS:
        df[f'MA{n}'] = df['收盘'].rolling(n).mean().round(3)
    return df


def detect_ma_signals(df: pd.DataFrame) -> list:
    signals = []
    pairs = [(5, 20), (5, 60), (20, 60)]
    for short, long in pairs:
        s_col, l_col = f'MA{short}', f'MA{long}'
        mask = df[s_col].notna() & df[l_col].notna()
        sub = df[mask].copy()
        if len(sub) < 2:
            continue
        prev_above = (sub[s_col].shift(1) > sub[l_col].shift(1))
        curr_above = (sub[s_col] > sub[l_col])
        for _, row in sub[~prev_above & curr_above].iterrows():
            signals.append({'date': row['日期'], 'type': '金叉',
                            'detail': f'MA{short} 上穿 MA{long}',
                            'price': row['收盘'], 'action': '看涨信号'})
        for _, row in sub[prev_above & ~curr_above].iterrows():
            signals.append({'date': row['日期'], 'type': '死叉',
                            'detail': f'MA{short} 下穿 MA{long}',
                            'price': row['收盘'], 'action': '看跌信号'})

    recent = df.dropna(subset=[f'MA{n}' for n in [5, 10, 20, 60]]).tail(5)
    if len(recent) > 0:
        last = recent.iloc[-1]
        ma5, ma10, ma20, ma60 = last['MA5'], last['MA10'], last['MA20'], last['MA60']
        if ma5 > ma10 > ma20 > ma60:
            signals.append({'date': last['日期'], 'type': '多头排列',
                            'detail': 'MA5>MA10>MA20>MA60', 'price': last['收盘'], 'action': '强势上行'})
        elif ma5 < ma10 < ma20 < ma60:
            signals.append({'date': last['日期'], 'type': '空头排列',
                            'detail': 'MA5<MA10<MA20<MA60', 'price': last['收盘'], 'action': '强势下行'})
    return sorted(signals, key=lambda x: x['date'])


def find_pivot_points(df: pd.DataFrame, order: int = 10) -> pd.DataFrame:
    prices = df['收盘'].values
    high_idx = argrelextrema(prices, np.greater_equal, order=order)[0]
    low_idx  = argrelextrema(prices, np.less_equal,    order=order)[0]
    pivots = []
    for i in high_idx:
        pivots.append({'idx': i, 'date': df.iloc[i]['日期'], 'price': prices[i], 'type': 'H'})
    for i in low_idx:
        pivots.append({'idx': i, 'date': df.iloc[i]['日期'], 'price': prices[i], 'type': 'L'})
    pivot_df = pd.DataFrame(pivots).sort_values('idx').reset_index(drop=True)
    cleaned = []
    for _, row in pivot_df.iterrows():
        if cleaned and cleaned[-1]['type'] == row['type']:
            if row['type'] == 'H' and row['price'] > cleaned[-1]['price']:
                cleaned[-1] = row.to_dict()
            elif row['type'] == 'L' and row['price'] < cleaned[-1]['price']:
                cleaned[-1] = row.to_dict()
        else:
            cleaned.append(row.to_dict())
    return pd.DataFrame(cleaned).reset_index(drop=True)


def identify_elliott_waves(pivots: pd.DataFrame) -> list:
    waves = []
    pts = pivots.to_dict('records')
    n = len(pts)
    i = 0
    while i < n - 5:
        p = pts[i:i+6]
        types = [x['type'] for x in p]
        if types == ['L','H','L','H','L','H']:
            w1 = p[1]['price'] - p[0]['price']
            w3 = p[3]['price'] - p[2]['price']
            w5 = p[5]['price'] - p[4]['price']
            if w3 >= min(w1, w5) and w1 > 0 and w3 > 0 and w5 > 0:
                waves.append({'type': '上升五浪', 'start': p[0]['date'], 'end': p[5]['date'],
                              'w1': round(w1,2), 'w3': round(w3,2), 'w5': round(w5,2),
                              'total': round(p[5]['price']-p[0]['price'],2), 'points': p})
                i += 5; continue
        if types == ['H','L','H','L','H','L']:
            w1 = p[0]['price'] - p[1]['price']
            w3 = p[2]['price'] - p[3]['price']
            w5 = p[4]['price'] - p[5]['price']
            if w3 >= min(w1, w5) and w1 > 0 and w3 > 0 and w5 > 0:
                waves.append({'type': '下降五浪', 'start': p[0]['date'], 'end': p[5]['date'],
                              'w1': round(w1,2), 'w3': round(w3,2), 'w5': round(w5,2),
                              'total': round(p[0]['price']-p[5]['price'],2), 'points': p})
                i += 5; continue
        i += 1

    j = 0
    while j < n - 2:
        p = pts[j:j+3]
        types = [x['type'] for x in p]
        if types == ['H','L','H']:
            waves.append({'type': 'ABC下跌调整', 'start': p[0]['date'], 'end': p[2]['date'],
                          'A': round(p[0]['price']-p[1]['price'],2),
                          'C': round(p[2]['price']-p[1]['price'],2), 'points': p})
        elif types == ['L','H','L']:
            waves.append({'type': 'ABC上涨调整', 'start': p[0]['date'], 'end': p[2]['date'],
                          'A': round(p[1]['price']-p[0]['price'],2),
                          'C': round(p[1]['price']-p[2]['price'],2), 'points': p})
        j += 1
    return waves


def analyze_current_position(pivots: pd.DataFrame, df: pd.DataFrame) -> dict:
    if len(pivots) < 2:
        return {}
    last  = pivots.iloc[-1]
    prev  = pivots.iloc[-2]
    cur_p = df.iloc[-1]['收盘']
    cur_d = df.iloc[-1]['日期']
    direction = '上行' if cur_p > last['price'] else '下行'
    change    = cur_p - last['price']
    return {
        'date':        cur_d.strftime('%Y-%m-%d'),
        'price':       cur_p,
        'last_pivot':  f"{last['type']} {last['price']:.2f} ({last['date'].strftime('%Y-%m-%d')})",
        'prev_pivot':  f"{prev['type']} {prev['price']:.2f} ({prev['date'].strftime('%Y-%m-%d')})",
        'direction':   direction,
        'change':      round(change, 2),
        'change_pct':  round(change / last['price'] * 100, 2)
    }


def main():
    print(f"\n{'='*80}")
    print(f"  {STOCK_NAME}（{STOCK_CODE}）均线分析 + 波浪理论波段识别")
    print(f"{'='*80}")

    df = load_data(CSV_FILE)
    df = calc_ma(df)

    # ── 均线信号（近2年）──
    print(f"\n{'─'*80}")
    print("  【均线信号 — 近2年】")
    print(f"{'─'*80}")
    signals = detect_ma_signals(df.tail(500))
    for s in signals[-20:]:
        d = s['date'].strftime('%Y-%m-%d') if hasattr(s['date'], 'strftime') else str(s['date'])
        print(f"  {d}  [{s['type']:6}]  {s['detail']:<22}  收盘:{s['price']:.2f}  → {s['action']}")

    last = df.dropna(subset=['MA5','MA20','MA60']).iloc[-1]
    print(f"\n  当前状态（{last['日期'].strftime('%Y-%m-%d')}，收盘 {last['收盘']:.2f}）:")
    for n in MA_PERIODS:
        col = f'MA{n}'
        if col in df.columns and not pd.isna(last[col]):
            diff = last['收盘'] - last[col]
            tag  = '↑上方' if diff > 0 else '↓下方'
            print(f"    MA{n:4d}: {last[col]:.3f}  ({tag} {abs(diff):.2f})")

    # ── 波浪分析 ──
    print(f"\n{'─'*80}")
    print("  【波浪理论 — 宏观波段（近5年，order=30）】")
    print(f"{'─'*80}")
    df5y    = df.tail(1250).reset_index(drop=True)
    pivots_m = find_pivot_points(df5y, order=30)
    waves_m  = identify_elliott_waves(pivots_m)
    print(f"  识别到 {len(pivots_m)} 个关键转折点，{len([w for w in waves_m if '五浪' in w['type']])} 个五浪结构")
    for w in [x for x in waves_m if '五浪' in x['type']][-5:]:
        s = w['start'].strftime('%Y-%m-%d') if hasattr(w['start'],'strftime') else str(w['start'])
        e = w['end'].strftime('%Y-%m-%d')   if hasattr(w['end'],  'strftime') else str(w['end'])
        print(f"  [{w['type']}]  {s} → {e}  总幅:{w['total']:.2f}  浪1:{w['w1']}  浪3:{w['w3']}  浪5:{w['w5']}")

    print(f"\n{'─'*80}")
    print("  【波浪理论 — 中观波段（近1年，order=10）】")
    print(f"{'─'*80}")
    df1y    = df.tail(250).reset_index(drop=True)
    pivots_s = find_pivot_points(df1y, order=10)
    waves_s  = identify_elliott_waves(pivots_s)
    print(f"  识别到 {len(pivots_s)} 个关键转折点")
    for w in waves_s[-8:]:
        s = w['start'].strftime('%Y-%m-%d') if hasattr(w['start'],'strftime') else str(w['start'])
        e = w['end'].strftime('%Y-%m-%d')   if hasattr(w['end'],  'strftime') else str(w['end'])
        if '五浪' in w['type']:
            print(f"  [{w['type']}]  {s} → {e}  总幅:{w['total']:.2f}  浪1:{w['w1']}  浪3:{w['w3']}  浪5:{w['w5']}")
        else:
            print(f"  [{w['type']}]  {s} → {e}  A浪:{w['A']:.2f}  C浪:{w['C']:.2f}")

    # ── 当前波浪位置 ──
    print(f"\n{'─'*80}")
    print("  【当前所处波浪位置】")
    print(f"{'─'*80}")
    pos = analyze_current_position(pivots_s, df1y)
    if pos:
        print(f"  当前日期:   {pos['date']}")
        print(f"  当前价格:   {pos['price']:.2f} 元")
        print(f"  上一转折点: {pos['last_pivot']}")
        print(f"  再前转折点: {pos['prev_pivot']}")
        print(f"  当前走势:   {pos['direction']}")
        print(f"  距上一转折: {pos['change']:+.2f} 元 ({pos['change_pct']:+.2f}%)")

    # 转折点列表
    print(f"\n  最近10个关键转折点（中观）:")
    for _, row in pivots_s.tail(10).iterrows():
        tag = '▲高点' if row['type'] == 'H' else '▼低点'
        d   = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'],'strftime') else str(row['date'])
        print(f"    {d}  {tag}  {row['price']:.2f}")

    print()


if __name__ == "__main__":
    main()
