# -*- coding: utf-8 -*-
"""
realtime_data.py - OpenClaw Skill: 实时行情 + 买卖五档 + 分钟K线
"""

from datetime import datetime
import requests
import pandas as pd

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://finance.sina.com.cn",
}


def _to_symbol(stock_code: str) -> str:
    return f"sz{stock_code}" if stock_code.startswith(("0", "3")) else f"sh{stock_code}"


def get_realtime_quote(stock_code: str = "000852", stock_name: str = "石化机械", timeout: int = 10):
    symbol = _to_symbol(stock_code)
    url = f"https://hq.sinajs.cn/list={symbol}"
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    fields = resp.text.split('"')[1].split(',')

    cur = float(fields[3]); open_p = float(fields[1]); yclose = float(fields[2])
    high = float(fields[4]); low = float(fields[5])
    volume_hand = int(fields[8]) // 100
    amount = float(fields[9])
    change = cur - yclose
    chg_pct = change / yclose * 100 if yclose else 0

    buy_prices = [float(fields[11]), float(fields[13]), float(fields[15]), float(fields[17]), float(fields[19])]
    buy_vols = [int(fields[10])//100, int(fields[12])//100, int(fields[14])//100, int(fields[16])//100, int(fields[18])//100]
    sell_prices = [float(fields[21]), float(fields[23]), float(fields[25]), float(fields[27]), float(fields[29])]
    sell_vols = [int(fields[20])//100, int(fields[22])//100, int(fields[24])//100, int(fields[26])//100, int(fields[28])//100]

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "time": f"{fields[30]} {fields[31]}",
        "price": cur,
        "open": open_p,
        "yclose": yclose,
        "high": high,
        "low": low,
        "change": change,
        "change_pct": chg_pct,
        "volume_hand": volume_hand,
        "amount": amount,
        "buy_prices": buy_prices,
        "buy_vols": buy_vols,
        "sell_prices": sell_prices,
        "sell_vols": sell_vols,
    }


def get_minute_kline(stock_code: str = "000852", scale: int = 1, datalen: int = 30, timeout: int = 15):
    symbol = _to_symbol(stock_code)
    url = (
        "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
        f"?symbol={symbol}&scale={scale}&ma=no&datalen={datalen}"
    )
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return None
    df = pd.DataFrame(data)
    df.columns = ["时间", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
    return df


def main(stock_code: str = "000852", stock_name: str = "石化机械", minute_scale: int = 1, minute_len: int = 30):
    print("\n" + "=" * 70)
    print(f"  {stock_name}({stock_code}) 实时数据")
    print(f"  获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    quote = get_realtime_quote(stock_code=stock_code, stock_name=stock_name)
    print(f"\n当前: {quote['price']:.2f}  涨跌: {quote['change']:+.2f} ({quote['change_pct']:+.2f}%)")
    print(f"开:{quote['open']:.2f} 高:{quote['high']:.2f} 低:{quote['low']:.2f} 昨:{quote['yclose']:.2f}")
    print(f"量:{quote['volume_hand']:,}手 额:{quote['amount']:,.0f}元")

    kline = get_minute_kline(stock_code=stock_code, scale=minute_scale, datalen=minute_len)
    if kline is None:
        print("\n分钟K线: 暂无数据（可能非交易时段）")
    else:
        print(f"\n分钟K线: {len(kline)} 条")
        print(kline.tail(min(10, len(kline))).to_string(index=False))

    return {"quote": quote, "minute_kline": kline}


if __name__ == "__main__":
    main()
