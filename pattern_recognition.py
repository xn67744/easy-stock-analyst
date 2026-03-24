# -*- coding: utf-8 -*-
"""
Demo 4: 股票形态识别
识别: 矩形箱体、对称/上升/下降三角形、旗形、楔形、上升/下降通道
"""

import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
import warnings
warnings.filterwarnings('ignore')

CSV_FILE   = "000852_石化机械_历史数据.csv"
STOCK_NAME = "石化机械"
STOCK_CODE = "000852"


def load_data(csv_file):
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    df.columns = ['日期','开盘','收盘','最高','最低','成交量(手)','换手率(%)','涨跌幅(%)']
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values('日期').reset_index(drop=True)
    for c in ['开盘','收盘','最高','最低']:
        df[c] = df[c].astype(float)
    df['成交量(手)'] = df['成交量(手)'].astype(float)
    return df


def find_pivots(arr, order=5):
    hi = argrelextrema(arr, np.greater_equal, order=order)[0]
    lo = argrelextrema(arr, np.less_equal,    order=order)[0]
    return hi, lo


def linreg(x, y):
    A = np.vstack([x, np.ones(len(x))]).T
    k, b = np.linalg.lstsq(A, y, rcond=None)[0]
    return k, b


def detect_rectangle(df, window=60, tol=0.03):
    results, n, step = [], len(df), window // 2
    for start in range(0, n - window, step):
        seg = df.iloc[start:start+window]
        hi, lo = find_pivots(seg['最高'].values, 5)
        if len(hi) < 2 or len(lo) < 2:
            continue
        hp, lp = seg['最高'].values[hi], seg['最低'].values[lo]
        upper, lower = np.mean(hp), np.mean(lp)
        if lower >= upper:
            continue
        if np.std(hp)/upper < tol and np.std(lp)/lower < tol:
            pct = (upper-lower)/lower*100
            results.append({
                'pattern': '矩形箱体',
                'start': seg.iloc[0]['日期'], 'end': seg.iloc[-1]['日期'],
                'upper': round(upper,2), 'lower': round(lower,2),
                'detail': f'上轨{upper:.2f} 下轨{lower:.2f} 宽度{pct:.1f}%',
                'signal': '放量突破上轨->翻箱体上涨；跌破下轨->下跌延续'
            })
    return results


def detect_triangle(df, window=60, tol=0.02):
    results, n, step = [], len(df), window // 2
    for start in range(0, n - window, step):
        seg = df.iloc[start:start+window]
        hi, lo = find_pivots(seg['最高'].values, 5)
        if len(hi) < 2 or len(lo) < 2:
            continue
        k_h, _ = linreg(hi.astype(float), seg['最高'].values[hi])
        k_l, _ = linreg(lo.astype(float), seg['最低'].values[lo])
        s, e = seg.iloc[0]['日期'], seg.iloc[-1]['日期']
        if k_h < -tol and k_l > tol:
            results.append({'pattern':'对称三角形','start':s,'end':e,
                'detail':f'高点降低(斜率{k_h:.4f}) 低点抬高(斜率{k_l:.4f})',
                'signal':'多空均衡，放量突破方向延续趋势'})
        elif abs(k_h) <= tol and k_l > tol:
            res = round(np.mean(seg['最高'].values[hi]),2)
            results.append({'pattern':'上升三角形','start':s,'end':e,
                'detail':f'水平压力位{res} 低点持续抬高(斜率{k_l:.4f})',
                'signal':'强势看涨，90%概率放量突破水平压力上行'})
        elif k_h < -tol and abs(k_l) <= tol:
            sup = round(np.mean(seg['最低'].values[lo]),2)
            results.append({'pattern':'下降三角形','start':s,'end':e,
                'detail':f'水平支撑位{sup} 高点持续降低(斜率{k_h:.4f})',
                'signal':'弱势看跌，90%概率放量跌破水平支撑下行'})
    return results


def detect_flag(df, window=40, pole_min=0.08):
    results, n = [], len(df)
    pole_len, flag_len = 10, window - 10
    for start in range(0, n - window, 10):
        pole = df.iloc[start:start+pole_len]
        flag = df.iloc[start+pole_len:start+window]
        pole_chg = (pole.iloc[-1]['收盘'] - pole.iloc[0]['收盘']) / pole.iloc[0]['收盘']
        if abs(pole_chg) < pole_min:
            continue
        flag_range = (flag['最高'].max() - flag['最低'].min()) / flag['收盘'].mean()
        if flag_range > 0.06:
            continue
        flag_slope = (flag.iloc[-1]['收盘'] - flag.iloc[0]['收盘']) / flag.iloc[0]['收盘']
        if pole_chg > 0 and flag_slope < 0:
            pt, sig = '上升旗形', '整理完毕后放量向上突破，目标约旗杆幅度'
        elif pole_chg < 0 and flag_slope > 0:
            pt, sig = '下降旗形', '整理完毕后放量向下突破，目标约旗杆幅度'
        else:
            continue
        results.append({'pattern':pt,
            'start':pole.iloc[0]['日期'],'end':flag.iloc[-1]['日期'],
            'detail':f'旗杆{pole_chg*100:.1f}% 旗面振幅{flag_range*100:.1f}%',
            'signal':sig})
    return results


def detect_wedge(df, window=60, tol=0.015):
    results, n, step = [], len(df), window // 2
    for start in range(0, n - window, step):
        seg = df.iloc[start:start+window]
        hi, lo = find_pivots(seg['最高'].values, 5)
        if len(hi) < 2 or len(lo) < 2:
            continue
        k_h, _ = linreg(hi.astype(float), seg['最高'].values[hi])
        k_l, _ = linreg(lo.astype(float), seg['最低'].values[lo])
        s, e = seg.iloc[0]['日期'], seg.iloc[-1]['日期']
        if k_h > tol and k_l > tol and k_l > k_h:
            results.append({'pattern':'上升楔形','start':s,'end':e,
                'detail':f'双线向上收敛 上线斜率{k_h:.4f} 下线斜率{k_l:.4f}',
                'signal':'下跌趋势中反弹整理，大概率向下突破延续下跌'})
        elif k_h < -tol and k_l < -tol and k_h > k_l:
            results.append({'pattern':'下降楔形','start':s,'end':e,
                'detail':f'双线向下收敛 上线斜率{k_h:.4f} 下线斜率{k_l:.4f}',
                'signal':'上涨趋势中回调整理，大概率向上突破延续上涨'})
    return results


def detect_channel(df, window=60, tol=0.015):
    results, n, step = [], len(df), window // 2
    for start in range(0, n - window, step):
        seg = df.iloc[start:start+window]
        hi, lo = find_pivots(seg['最高'].values, 5)
        if len(hi) < 2 or len(lo) < 2:
            continue
        k_h, _ = linreg(hi.astype(float), seg['最高'].values[hi])
        k_l, _ = linreg(lo.astype(float), seg['最低'].values[lo])
        if abs(k_h - k_l) < tol:
            s, e, avg_k = seg.iloc[0]['日期'], seg.iloc[-1]['日期'], (k_h+k_l)/2
            if avg_k > tol:
                results.append({'pattern':'上升通道','start':s,'end':e,
                    'detail':f'平行上行 斜率约{avg_k:.4f}',
                    'signal':'下轨支撑买入，上轨压力减仓，突破上轨趋势加速'})
            elif avg_k < -tol:
                results.append({'pattern':'下降通道','start':s,'end':e,
                    'detail':f'平行下行 斜率约{avg_k:.4f}',
                    'signal':'上轨压力做空，下轨支撑止盈，跌破下轨趋势加速'})
    return results


def print_results(patterns, title):
    print(f"  >> {title}")
    if not patterns:
        print("    未识别到该形态\n")
        return
    seen = {}
    for p in patterns:
        seen[p['pattern']] = p
    for p in seen.values():
        s = p['start'].strftime('%Y-%m-%d') if hasattr(p['start'],'strftime') else str(p['start'])
        e = p['end'].strftime('%Y-%m-%d')   if hasattr(p['end'],  'strftime') else str(p['end'])
        print(f"    [{p['pattern']}] {s} -> {e}")
        print(f"    核心特征: {p['detail']}")
        print(f"    信号含义: {p['signal']}")
        print()


def analyze(df, label):
    print(f"\n{'─'*80}")
    print(f"  【{label} 形态分析 {df.iloc[0]['日期'].strftime('%Y-%m-%d')} -> {df.iloc[-1]['日期'].strftime('%Y-%m-%d')}】")
    print(f"{'─'*80}\n")
    print_results(detect_rectangle(df), '矩形箱体（翻箱体）')
    print_results(detect_triangle(df),  '三角形（对称/上升/下降）')
    print_results(detect_flag(df),      '旗形')
    print_results(detect_wedge(df),     '楔形（上升/下降）')
    print_results(detect_channel(df),   '通道（上升/下降）')


def main():
    print(f"\n{'='*80}")
    print(f"  {STOCK_NAME}（{STOCK_CODE}）股票形态识别分析")
    print(f"{'='*80}")
    df = load_data(CSV_FILE)
    analyze(df.tail(60).reset_index(drop=True),  '近60日（短期）')
    analyze(df.tail(250).reset_index(drop=True), '近1年（中期）')
    analyze(df.tail(500).reset_index(drop=True), '近2年（中长期）')
    print()


if __name__ == '__main__':
    main()
