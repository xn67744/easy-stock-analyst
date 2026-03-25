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

## 图表结构与交互操作说明

### 图表结构（5个子图，已移除BOLL %B子图）

| 子图 | 内容 | 图例分组 |
|------|------|----------|
| Row1 | K线 + MA5/10/20/60 + 波段高低点 + 趋势线 | 右侧「K线图」分组 |
| Row2 | 成交量（红涨绿跌） | 无图例 |
| Row3 | MACD（DIF/DEA线 + 红绿柱） | 右侧「MACD」分组 |
| Row4 | KDJ（K/D/J线） | 右侧「KDJ」分组 |
| Row5 | BOLL轨道（上/中/下轨）+ 内嵌K线蜡烛图 + 信号标记 | 右侧「BOLL」分组 |

### 最新变更

- **已去掉底部额外面板**：所有子图 `rangeslider` 已关闭，不再出现最下方缩略条
- **BOLL %B 子图已移除**：BOLL子图内嵌K线蜡烛图，与布林带轨道叠加显示
- **图例分组**：每个子图图例独立分组显示在右侧，不混用
- **图例去重**：BOLL信号（超买/超卖/顶背离/底背离）同类型只显示一条图例
- **新增波段箭头**：主图按波段连接显示黄/橙箭头
- **新增互斥开关**：右侧按钮支持“显示均线(隐藏波段)”与“显示波段(隐藏均线)”

### 交互操作

#### 鼠标悬停
- 在K线图（Row1）上移动鼠标，提示框仅显示该K线的**日期、开盘、收盘、最高、最低、成交量**
- 均线数值不在悬停提示中显示，减少信息干扰
- 鼠标十字线为黄色，X/Y坐标线可跨子图贯穿显示
- 右侧提供固定看板（不跟随鼠标），显示最新一根K线核心数据
- 其余子图悬停显示对应指标数值

#### 图例控制
- **单击图例项**：切换该曲线的显示/隐藏
- **双击图例项**：仅保留该曲线，其余隐藏；再次双击恢复全部
- 均线、波段高低点、波浪趋势线均可通过图例单独开关

#### 缩放与平移
- **框选**图表区域可局部放大
- **双击**图表区域恢复全局视图
- 拖动 X 轴可平移时间范围
- 各子图 Y 轴可独立拖拽缩放
- 所有子图共享 X 轴，缩放/平移时全部联动

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
- `09880_优必选_技术指标图.html`

## 本次功能测试记录

已执行并通过：

```bash
python -m py_compile charts.py
python charts.py
```
