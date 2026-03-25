# -*- coding: utf-8 -*-
"""
Demo 3: 均线分析 + 波浪理论波段识别
数据来源: 本地CSV文件

修复内容:
  1. 上升/下降五浪补充两条艾略特硬性规则（浪2不破浪1起点、浪4不进入浪1区间）
  2. 波浪不成立时记录并输出具体原因
  3. ABC 调整浪过滤掉已被五浪覆盖的时间段，避免重叠识别
  4. iterrows 金叉/死叉检测改为向量化，提升性能
"""

import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import warnings
warnings.filterwarnings('ignore')

CSV_FILE   = "000852_石化机械_历史数据.csv"
STOCK_NAME = "石化机械"
STOCK_CODE = "000852"


# ══════════════════════════════════════════════════════════════════
#  数据加载
# ══════════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════════
#  均线计算
# ══════════════════════════════════════════════════════════════════
MA_PERIODS = [5, 10, 20, 60, 120, 250]

def calc_ma(df: pd.DataFrame) -> pd.DataFrame:
    for n in MA_PERIODS:
        df[f'MA{n}'] = df['收盘'].rolling(n).mean().round(3)
    return df


# ══════════════════════════════════════════════════════════════════
#  均线信号检测（向量化，替换 iterrows）
# ══════════════════════════════════════════════════════════════════
def detect_ma_signals(df: pd.DataFrame) -> list:
    signals = []
    pairs = [(5, 20), (5, 60), (20, 60)]

    for short, long in pairs:
        s_col, l_col = f'MA{short}', f'MA{long}'
        mask = df[s_col].notna() & df[l_col].notna()
        sub  = df[mask].copy()
        if len(sub) < 2:
            continue

        # ── 向量化金叉 / 死叉检测 ──
        prev_above = sub[s_col].shift(1) > sub[l_col].shift(1)
        curr_above = sub[s_col] > sub[l_col]

        golden = sub[(~prev_above) & curr_above]
        dead   = sub[prev_above & (~curr_above)]

        for _, row in golden.iterrows():
            signals.append({'date': row['日期'], 'type': '金叉',
                            'detail': f'MA{short} 上穿 MA{long}',
                            'price': row['收盘'], 'action': '看涨信号'})
        for _, row in dead.iterrows():
            signals.append({'date': row['日期'], 'type': '死叉',
                            'detail': f'MA{short} 下穿 MA{long}',
                            'price': row['收盘'], 'action': '看跌信号'})

    # ── 多头 / 空头排列（取最新5根有效K线）──
    valid_cols = [f'MA{n}' for n in [5, 10, 20, 60]]
    recent = df.dropna(subset=valid_cols).tail(5)
    if len(recent) > 0:
        last = recent.iloc[-1]
        ma5, ma10, ma20, ma60 = last['MA5'], last['MA10'], last['MA20'], last['MA60']
        if ma5 > ma10 > ma20 > ma60:
            signals.append({'date': last['日期'], 'type': '多头排列',
                            'detail': 'MA5>MA10>MA20>MA60',
                            'price': last['收盘'], 'action': '强势上行'})
        elif ma5 < ma10 < ma20 < ma60:
            signals.append({'date': last['日期'], 'type': '空头排列',
                            'detail': 'MA5<MA10<MA20<MA60',
                            'price': last['收盘'], 'action': '强势下行'})

    return sorted(signals, key=lambda x: x['date'])


# ══════════════════════════════════════════════════════════════════
#  极值点识别
# ══════════════════════════════════════════════════════════════════
def find_pivot_points(df: pd.DataFrame, order: int = 10) -> pd.DataFrame:
    prices   = df['收盘'].values
    high_idx = argrelextrema(prices, np.greater_equal, order=order)[0]
    low_idx  = argrelextrema(prices, np.less_equal,    order=order)[0]

    pivots = []
    for i in high_idx:
        pivots.append({'idx': i, 'date': df.iloc[i]['日期'], 'price': prices[i], 'type': 'H'})
    for i in low_idx:
        pivots.append({'idx': i, 'date': df.iloc[i]['日期'], 'price': prices[i], 'type': 'L'})

    pivot_df = pd.DataFrame(pivots).sort_values('idx').reset_index(drop=True)

    # 同向相邻极值点合并（保留极值更强的那一个）
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


# ══════════════════════════════════════════════════════════════════
#  五浪规则校验辅助函数
#  返回: (是否成立: bool, 原因说明: str)
# ══════════════════════════════════════════════════════════════════
def _check_impulse_up(p: list) -> tuple:
    """
    上升五浪（L H L H L H）三条硬性规则:
      Rule1: 浪2回调不能跌破浪1起点  → p[2] > p[0]
      Rule2: 浪4不能进入浪1的价格区间 → p[4] > p[1]
      Rule3: 浪3不能是最短的推动浪   → w3 >= min(w1, w5)
    """
    w1 = p[1]['price'] - p[0]['price']
    w3 = p[3]['price'] - p[2]['price']
    w5 = p[5]['price'] - p[4]['price']

    reasons = []
    if not (w1 > 0 and w3 > 0 and w5 > 0):
        reasons.append('推动浪幅度须为正值')
    if p[2]['price'] <= p[0]['price']:
        reasons.append(f'浪2({p[2]["price"]:.2f})跌破浪1起点({p[0]["price"]:.2f})，违反Rule1')
    if p[4]['price'] <= p[1]['price']:
        reasons.append(f'浪4({p[4]["price"]:.2f})进入浪1区间(>{p[1]["price"]:.2f})，违反Rule2')
    if w3 < min(w1, w5):
        reasons.append(f'浪3({w3:.2f})为最短推动浪(浪1:{w1:.2f} 浪5:{w5:.2f})，违反Rule3')

    if reasons:
        return False, '；'.join(reasons)
    return True, ''


def _check_impulse_down(p: list) -> tuple:
    """
    下降五浪（H L H L H L）三条硬性规则:
      Rule1: 浪2反弹不能超过浪1起点  → p[2] < p[0]
      Rule2: 浪4不能进入浪1的价格区间 → p[4] < p[1]
      Rule3: 浪3不能是最短的推动浪
    """
    w1 = p[0]['price'] - p[1]['price']
    w3 = p[2]['price'] - p[3]['price']
    w5 = p[4]['price'] - p[5]['price']

    reasons = []
    if not (w1 > 0 and w3 > 0 and w5 > 0):
        reasons.append('推动浪幅度须为正值')
    if p[2]['price'] >= p[0]['price']:
        reasons.append(f'浪2({p[2]["price"]:.2f})超过浪1起点({p[0]["price"]:.2f})，违反Rule1')
    if p[4]['price'] >= p[1]['price']:
        reasons.append(f'浪4({p[4]["price"]:.2f})进入浪1区间(<{p[1]["price"]:.2f})，违反Rule2')
    if w3 < min(w1, w5):
        reasons.append(f'浪3({w3:.2f})为最短推动浪(浪1:{w1:.2f} 浪5:{w5:.2f})，违反Rule3')

    if reasons:
        return False, '；'.join(reasons)
    return True, ''


# ══════════════════════════════════════════════════════════════════
#  艾略特波浪识别
# ══════════════════════════════════════════════════════════════════
def identify_elliott_waves(pivots: pd.DataFrame) -> tuple:
    """
    返回:
      waves        : list[dict]  — 成立的波浪
      invalid_waves: list[dict]  — 不成立的候选波浪（附原因）
    """
    waves         = []
    invalid_waves = []
    pts = pivots.to_dict('records')
    n   = len(pts)

    # ── 五浪扫描 ──
    used_ranges = []          # 记录已识别五浪覆盖的转折点索引范围
    i = 0
    while i < n - 5:
        p     = pts[i:i+6]
        types = [x['type'] for x in p]

        if types == ['L', 'H', 'L', 'H', 'L', 'H']:
            ok, reason = _check_impulse_up(p)
            w1 = p[1]['price'] - p[0]['price']
            w3 = p[3]['price'] - p[2]['price']
            w5 = p[5]['price'] - p[4]['price']
            entry = {
                'type':   '上升五浪',
                'start':  p[0]['date'],
                'end':    p[5]['date'],
                'start_idx': p[0]['idx'],
                'end_idx':   p[5]['idx'],
                'w1':     round(w1, 2),
                'w3':     round(w3, 2),
                'w5':     round(w5, 2),
                'total':  round(p[5]['price'] - p[0]['price'], 2),
                'points': p,
            }
            if ok:
                waves.append(entry)
                used_ranges.append((p[0]['idx'], p[5]['idx']))
                i += 5
                continue
            else:
                entry['reason'] = reason
                invalid_waves.append(entry)

        elif types == ['H', 'L', 'H', 'L', 'H', 'L']:
            ok, reason = _check_impulse_down(p)
            w1 = p[0]['price'] - p[1]['price']
            w3 = p[2]['price'] - p[3]['price']
            w5 = p[4]['price'] - p[5]['price']
            entry = {
                'type':   '下降五浪',
                'start':  p[0]['date'],
                'end':    p[5]['date'],
                'start_idx': p[0]['idx'],
                'end_idx':   p[5]['idx'],
                'w1':     round(w1, 2),
                'w3':     round(w3, 2),
                'w5':     round(w5, 2),
                'total':  round(p[0]['price'] - p[5]['price'], 2),
                'points': p,
            }
            if ok:
                waves.append(entry)
                used_ranges.append((p[0]['idx'], p[5]['idx']))
                i += 5
                continue
            else:
                entry['reason'] = reason
                invalid_waves.append(entry)

        i += 1

    # ── ABC 调整浪扫描（过滤五浪已覆盖的区间）──
    def _in_used(idx_a: int, idx_b: int) -> bool:
        """判断 [idx_a, idx_b] 是否与任意已用五浪区间重叠"""
        for s, e in used_ranges:
            if idx_a >= s and idx_b <= e:
                return True
        return False

    j = 0
    while j < n - 2:
        p     = pts[j:j+3]
        types = [x['type'] for x in p]

        if _in_used(p[0]['idx'], p[2]['idx']):
            j += 1
            continue

        if types == ['H', 'L', 'H']:
            A = round(p[0]['price'] - p[1]['price'], 2)
            C = round(p[2]['price'] - p[1]['price'], 2)
            waves.append({'type': 'ABC下跌调整', 'start': p[0]['date'], 'end': p[2]['date'],
                          'A': A, 'C': C, 'points': p})
        elif types == ['L', 'H', 'L']:
            A = round(p[1]['price'] - p[0]['price'], 2)
            C = round(p[1]['price'] - p[2]['price'], 2)
            waves.append({'type': 'ABC上涨调整', 'start': p[0]['date'], 'end': p[2]['date'],
                          'A': A, 'C': C, 'points': p})
        j += 1

    return waves, invalid_waves


# ══════════════════════════════════════════════════════════════════
#  当前波浪位置
# ══════════════════════════════════════════════════════════════════
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
        'date':       cur_d.strftime('%Y-%m-%d'),
        'price':      cur_p,
        'last_pivot': f"{last['type']} {last['price']:.2f} ({last['date'].strftime('%Y-%m-%d')})",
        'prev_pivot': f"{prev['type']} {prev['price']:.2f} ({prev['date'].strftime('%Y-%m-%d')})",
        'direction':  direction,
        'change':     round(change, 2),
        'change_pct': round(change / last['price'] * 100, 2),
    }


# ══════════════════════════════════════════════════════════════════
#  辅助：日期格式化
# ══════════════════════════════════════════════════════════════════
def _fmt(d) -> str:
    return d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)


# ══════════════════════════════════════════════════════════════════
#  主函数
# ══════════════════════════════════════════════════════════════════
def main():
    print(f"\n{'='*80}")
    print(f"  {STOCK_NAME}（{STOCK_CODE}）均线分析 + 波浪理论波段识别")
    print(f"{'='*80}")

    df = load_data(CSV_FILE)
    df = calc_ma(df)

    # ── 均线信号（近2年）──────────────────────────────────────────
    print(f"\n{'─'*80}")
    print("  【均线信号 — 近2年】")
    print(f"{'─'*80}")
    signals = detect_ma_signals(df.tail(500))
    for s in signals[-20:]:
        print(f"  {_fmt(s['date'])}  [{s['type']:6}]  "
              f"{s['detail']:<22}  收盘:{s['price']:.2f}  → {s['action']}")

    last = df.dropna(subset=['MA5', 'MA20', 'MA60']).iloc[-1]
    print(f"\n  当前状态（{_fmt(last['日期'])}，收盘 {last['收盘']:.2f}）:")
    for n in MA_PERIODS:
        col = f'MA{n}'
        if col in df.columns and not pd.isna(last[col]):
            diff = last['收盘'] - last[col]
            tag  = '↑上方' if diff > 0 else '↓下方'
            print(f"    MA{n:4d}: {last[col]:.3f}  ({tag} {abs(diff):.2f})")

    # ── 宏观波浪（近5年）─────────────────────────────────────────
    print(f"\n{'─'*80}")
    print("  【波浪理论 — 宏观波段（近5年，order=30）】")
    print(f"{'─'*80}")
    df5y     = df.tail(1250).reset_index(drop=True)
    pivots_m = find_pivot_points(df5y, order=30)
    waves_m, invalid_m = identify_elliott_waves(pivots_m)

    five_m = [w for w in waves_m if '五浪' in w['type']]
    print(f"  识别到 {len(pivots_m)} 个关键转折点，{len(five_m)} 个有效五浪结构")
    for w in five_m[-5:]:
        print(f"  [{w['type']}]  {_fmt(w['start'])} → {_fmt(w['end'])}"
              f"  总幅:{w['total']:.2f}  浪1:{w['w1']}  浪3:{w['w3']}  浪5:{w['w5']}")

    if invalid_m:
        print(f"\n  ⚠ 不成立的五浪候选（宏观，共 {len(invalid_m)} 个）:")
        for w in invalid_m[-5:]:
            print(f"    [{w['type']}]  {_fmt(w['start'])} → {_fmt(w['end'])}"
                  f"  → 原因: {w['reason']}")

    # ── 中观波浪（近1年）─────────────────────────────────────────
    print(f"\n{'─'*80}")
    print("  【波浪理论 — 中观波段（近1年，order=10）】")
    print(f"{'─'*80}")
    df1y     = df.tail(250).reset_index(drop=True)
    pivots_s = find_pivot_points(df1y, order=10)
    waves_s, invalid_s = identify_elliott_waves(pivots_s)

    print(f"  识别到 {len(pivots_s)} 个关键转折点")
    for w in waves_s[-8:]:
        if '五浪' in w['type']:
            print(f"  [{w['type']}]  {_fmt(w['start'])} → {_fmt(w['end'])}"
                  f"  总幅:{w['total']:.2f}  浪1:{w['w1']}  浪3:{w['w3']}  浪5:{w['w5']}")
        else:
            print(f"  [{w['type']}]  {_fmt(w['start'])} → {_fmt(w['end'])}"
                  f"  A浪:{w['A']:.2f}  C浪:{w['C']:.2f}")

    if invalid_s:
        print(f"\n  ⚠ 不成立的五浪候选（中观，共 {len(invalid_s)} 个）:")
        for w in invalid_s[-5:]:
            print(f"    [{w['type']}]  {_fmt(w['start'])} → {_fmt(w['end'])}"
                  f"  → 原因: {w['reason']}")

    # ── 当前所处波浪位置 ─────────────────────────────────────────
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

    print(f"\n  最近10个关键转折点（中观）:")
    for _, row in pivots_s.tail(10).iterrows():
        tag = '▲高点' if row['type'] == 'H' else '▼低点'
        print(f"    {_fmt(row['date'])}  {tag}  {row['price']:.2f}")

    print()


if __name__ == "__main__":
    main()