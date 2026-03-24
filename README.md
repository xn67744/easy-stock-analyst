# 股票分析 Skill（OpenClaw）

这是一个可被 OpenClaw 调用的股票分析 Skill 项目，包含：
- 历史数据获取
- 实时行情获取
- 均线/波浪分析
- 形态识别
- 交互式图表（MACD/KDJ/BOLL/波浪/庄家成本）

## 安装

```bash
pip install -r requirements.txt
```

## 文件说明

- `historical_data.py`：历史数据获取与CSV导出
- `realtime_data.py`：实时行情、五档盘口、分钟K线
- `wave_analysis.py`：均线+波浪分析
- `pattern_recognition.py`：形态识别（箱体/三角形/旗形/楔形/通道）
- `charts.py`：技术指标图表与庄家分析

---

## OpenClaw 可调用参数

### 1) `historical_data.main(...)`

参数：
- `stock_code: str` 股票代码（例：`"000852"`）
- `stock_name: str` 股票名称
- `start_date: str` 开始日期（`YYYYMMDD`）
- `end_date: str | None` 结束日期（`YYYYMMDD`，`None`=今天）
- `adjust: str` 复权方式（`qfq`/`hfq`/`""`）
- `preview_rows: int` 控制台预览行数
- `output_dir: str | None` 导出目录
- `output_filename: str | None` 导出文件名

返回：
- `{"df": DataFrame, "csv_path": str}`

---

### 2) `realtime_data.main(...)`

参数：
- `stock_code: str`
- `stock_name: str`
- `minute_scale: int` 分钟级别（1/5/15/30/60）
- `minute_len: int` 返回分钟K线条数

返回：
- `{"quote": dict, "minute_kline": DataFrame | None}`

---

### 3) `charts.main(...)`

参数：
- `csv_file: str` 历史数据CSV路径
- `stock_name: str`
- `stock_code: str`
- `recent: int | None` 图表最近交易日数量
- `output_html: str | None` 输出HTML文件名
- `ma_periods: tuple[int, ...]` 均线周期

返回：
- `{"output_html": str, "zhuang": dict}`

`zhuang` 字段说明：
- `build_vwap`：估算建仓均价
- `build_cost`：估算控盘成本（建仓均价*1.03）
- `cum_turnover`：建仓区累计换手率
- `phase`：当前阶段（建仓/洗盘/拉升/出货）
- `phase_tip`：阶段说明

---

## 当前图表特性（已按需求实现）

- MACD 使用“线柱”样式（非bar量柱样式）
- 悬停优先展示K线信息
- X轴连续无交易日空隙（用交易序列索引）
- 主图标注区间最高价/最低价
- BOLL 增加超买/超卖/背离标记
- 加入波浪转折点与趋势线
- 加入庄家建仓均价与控盘成本线

---

## 快速使用

```bash
python historical_data.py
python realtime_data.py
python wave_analysis.py
python pattern_recognition.py
python charts.py
```

生成的交互图示例：
- `000852_石化机械_技术指标图.html`
