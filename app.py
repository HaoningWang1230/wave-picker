import re
import os
import pandas as pd
from datetime import datetime, date, time, timedelta

# =========================
# 0) 基本配置
# =========================
FILE_PATH = "outbound_10.csv"   # 修改为你的文件路径
# 支持的列名别名（英/中混合）
ALIASES = {
    "Package ID":   ["Package ID","package_id","包裹号","包裹编号","包裹编码","包裹id"],
    "Goods ID":     ["Goods ID","goods_id","sku_id","SKU ID","SKU","货品编码","商品编码","SKU编码"],
    "Goods quantity":["Goods quantity","quantity","qty","件数","数量","Goods qty","货品数量"],
    "Created time": ["Created time","created_time","create_time","创建时间","下单时间","订单创建时间"],
}
NEEDED = ["Package ID","Goods ID","Goods quantity","Created time"]

# =========================
# 1) 读取、标准化（大文件友好版）
# =========================
def _map_aliases(header_cols):
    """根据表头把别名映射成标准列名"""
    lower = {str(c).strip().lower(): c for c in header_cols}
    col_map = {}
    for std, aliases in ALIASES.items():
        hit = None
        for a in aliases:
            key = str(a).strip().lower()
            if key in lower:
                hit = lower[key]; break
        if not hit:
            raise KeyError(f"找不到必需列：{std}（可用别名：{ALIASES[std]}）")
        col_map[hit] = std
    return col_map

def _post_clean(df: pd.DataFrame) -> pd.DataFrame:
    """统一类型 + 时间解析 + 去无效时间"""
    df["Package ID"] = df["Package ID"].astype(str)
    df["Goods ID"]   = df["Goods ID"].astype(str)
    df["Goods quantity"] = pd.to_numeric(df["Goods quantity"], errors="coerce").fillna(0).astype("int32")
    # 先按常见格式走，失败再兜底；避免 dateutil 的慢解析 & 大量 UserWarning
    try:
        df["Created time"] = pd.to_datetime(df["Created time"], format="%Y-%m-%d %H:%M:%S", errors="raise")
    except Exception:
        df["Created time"] = pd.to_datetime(df["Created time"], errors="coerce")
    df = df.dropna(subset=["Created time"]).reset_index(drop=True)
    return df

def load_minimal_df(path: str) -> pd.DataFrame:
    """
    只读取 4 列（支持别名）；CSV 采用分块读取，降低内存峰值。
    Excel 一次读（建议尽量用 CSV）。
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        raw = pd.read_excel(path, engine="openpyxl")
        col_map = _map_aliases(raw.columns)
        out = raw.rename(columns=col_map)[NEEDED].copy()
        return _post_clean(out)

    # —— CSV：先读表头拿到列映射 —— 
    # 试常见编码：utf-8 -> gb18030
    def _read_head(enc: str):
        return pd.read_csv(path, nrows=0, encoding=enc)

    enc_used = None
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            head = _read_head(enc)
            enc_used = enc
            break
        except UnicodeDecodeError:
            continue
    if enc_used is None:
        raise ValueError("无法识别 CSV 编码（尝试了 utf-8 / utf-8-sig / gb18030）")

    col_map = _map_aliases(head.columns)

    # —— 分块读取 —— 
    chunks, total = [], 0
    try:
        itr = pd.read_csv(
            path,
            usecols=list(col_map.keys()),
            chunksize=200_000,     # 可按机器内存调整
            dtype=str,             # 避免类型推断卡顿
            low_memory=True,
            on_bad_lines="skip",
            encoding=enc_used
        )
    except UnicodeDecodeError:
        # 防御：若前面识别有误，再退回 gb18030
        itr = pd.read_csv(
            path,
            usecols=list(col_map.keys()),
            chunksize=200_000,
            dtype=str,
            low_memory=True,
            on_bad_lines="skip",
            encoding="gb18030"
        )

    for i, chunk in enumerate(itr, 1):
        c = chunk.rename(columns=col_map)
        c = c[NEEDED]
        c = _post_clean(c)
        chunks.append(c)
        total += len(c)
        if i % 5 == 0:
            print(f"[读入进度] 已处理 {i} 个块，共 {total:,} 行有效数据…")

    if not chunks:
        return pd.DataFrame(columns=NEEDED)
    out = pd.concat(chunks, ignore_index=True)
    print(f"[完成] 总计装载 {len(out):,} 行。时间范围：{out['Created time'].min()} → {out['Created time'].max()}")
    return out

# —— 用新的加载函数 —— 
df = load_minimal_df(FILE_PATH)

# =========================
# 2) 生产日与窗口
# =========================
NOON = time(12, 0)

def prod_date(dt: datetime) -> date:
    """<12:00 → 当日；≥12:00 → 次日"""
    return dt.date() if dt.time() < NOON else (dt.date() + timedelta(days=1))

def day_window(d: date):
    """生产日 d 的窗口：[d-1 12:00, d 12:00)"""
    end_dt = datetime.combine(d, NOON)
    start_dt = end_dt - timedelta(days=1)
    return start_dt, end_dt

# 最早/最晚 + 首/末生产日
earliest = df["Created time"].min()
latest   = df["Created time"].max()

first_prod_date = (earliest.date() + timedelta(days=1)) if earliest.time() < NOON else (earliest.date() + timedelta(days=2))
last_prod_date  = (latest.date() - timedelta(days=1))   if latest.time()   < NOON else  latest.date()

# 预切出“生产日→当日行数据”映射（便于反复使用）
DAILY_ROWS = {}
cur = first_prod_date
while cur <= last_prod_date:
    s, e = day_window(cur)
    day_rows = df[(df["Created time"] >= s) & (df["Created time"] < e)].copy()
    day_rows["生产日期"] = cur
    DAILY_ROWS[cur] = day_rows
    cur += timedelta(days=1)

# =========================
# 3) 工具函数：切块与统计
# =========================
def chunk_by_packages(pkg_ids, max_packages):
    out, cur = [], []
    for pid in pkg_ids:
        if len(cur) >= max_packages:
            out.append(cur); cur = []
        cur.append(pid)
    if cur: out.append(cur)
    return out

def chunk_by_units(pkg_ids, units_map, max_units):
    out, cur, cur_units = [], [], 0
    for pid in pkg_ids:
        u = int(units_map[pid])
        if cur and cur_units + u > max_units:
            out.append(cur); cur, cur_units = [], 0
        cur.append(pid); cur_units += u
    if cur: out.append(cur)
    return out

def summarize_picklist(day_rows: pd.DataFrame, pkg_list):
    sub = day_rows[day_rows["Package ID"].isin(pkg_list)]
    return (
        len(pkg_list),
        int(sub["Goods quantity"].sum()),
        int(sub["Goods ID"].nunique()),
    )

# =========================
# 4) 单条策略在某生产日的执行
# =========================
def apply_strategy_for_day(day_rows: pd.DataFrame, cutoff_t: time, strategy: str, assigned_pkgs: set, step_tag: str):
    """
    day_rows：该生产日窗口内的全部行
    cutoff_t：此条策略的截止时间（当日 00:00~12:00）
    strategy：'top' / '3-6' / 'mod' / 'daily'
    assigned_pkgs：该生产日已分配包裹集合（会更新）
    step_tag：用于拣选单ID的标记（例如 'STEP1-04:00'）
    """
    if day_rows.empty:
        return []

    d = day_rows["生产日期"].iloc[0]
    start_dt, _ = day_window(d)
    cutoff_dt = datetime.combine(d, cutoff_t)  # [start_dt, cutoff_dt)

    # 还未分配 & 截止时间之前
    avail = day_rows[(~day_rows["Package ID"].isin(assigned_pkgs)) & (day_rows["Created time"] < cutoff_dt)].copy()
    if avail.empty:
        return []

    # 包裹粒度
    per_pkg = (avail.groupby("Package ID")
                    .agg(创建时间=("Created time","min"),
                         总件数=("Goods quantity","sum"),
                         品类数=("Goods ID", pd.Series.nunique),
                         SKU列表=("Goods ID", lambda x: tuple(sorted(map(str, x)))))
                    .reset_index()
                    .sort_values("创建时间"))
    units_map = avail.groupby("Package ID")["Goods quantity"].sum().to_dict()

    rows = []

    if strategy == "top":
        # 单品单件：SKU >= 7 包裹；≤999件/拣选单（按件切块）
        singles = per_pkg[(per_pkg["总件数"] == 1) & (per_pkg["品类数"] == 1)].copy()
        if not singles.empty:
            singles["SKU"] = singles["SKU列表"].apply(lambda t: t[0])
            cnt = singles.groupby("SKU")["Package ID"].nunique().reset_index(name="包裹数")
            hot_sku = set(cnt.loc[cnt["包裹数"] >= 7, "SKU"])
            for sku in sorted(hot_sku):
                pkg_ids = singles.loc[singles["SKU"] == sku, "Package ID"].tolist()
                chunks = chunk_by_units(pkg_ids, units_map, max_units=999)
                for i, group in enumerate(chunks, 1):
                    b,u,s = summarize_picklist(avail, group)
                    if b == 0: 
                        continue
                    rows.append([d, "Top", f"SKU:{sku}", f"{step_tag}-TOP-SKU-{sku}-{i}", b,u,s, group])
                    assigned_pkgs.update(group)

        # 组合双品：2件&2个不同SKU，组合>=7包裹；≤999件/拣选单（按包裹数切块，容量=floor(999/2)）
        pairs = per_pkg[(per_pkg["总件数"] == 2) & (per_pkg["品类数"] == 2)].copy()
        if not pairs.empty:
            pairs["PAIR"] = pairs["SKU列表"].apply(lambda t: "+".join(t))
            cnt = pairs.groupby("PAIR")["Package ID"].nunique().reset_index(name="包裹数")
            hot_pair = set(cnt.loc[cnt["包裹数"] >= 7, "PAIR"])
            cap = max(1, 999 // 2)
            for pair in sorted(hot_pair):
                pkg_ids = pairs.loc[pairs["PAIR"] == pair, "Package ID"].tolist()
                chunks = chunk_by_packages(pkg_ids, max_packages=cap)
                for i, group in enumerate(chunks, 1):
                    group = [p for p in group if p not in assigned_pkgs]
                    if not group: 
                        continue
                    b,u,s = summarize_picklist(avail, group)
                    rows.append([d, "Top", f"PAIR:{pair}", f"{step_tag}-TOP-PAIR-{pair}-{i}", b,u,s, group])
                    assigned_pkgs.update(group)

    elif strategy == "3-6":
        # —— 混装（按 SKU 整包装入，不拆分）：单品单件且 SKU 在窗口内出现 3~6 包裹
        #     将每个 SKU 的包裹（3~6件）视为一个“整包”装入；每拣选单 ≤ 50 包裹
        singles = per_pkg[(per_pkg["总件数"] == 1) & (per_pkg["品类数"] == 1)].copy()
        if not singles.empty:
            singles["SKU"] = singles["SKU列表"].apply(lambda t: t[0])
            cnt = singles.groupby("SKU")["Package ID"].nunique().reset_index(name="包裹数")
            ok_skus = cnt.loc[cnt["包裹数"].between(3, 6), "SKU"].tolist()
            if ok_skus:
                # 先到先拣：按每个 SKU 的最早创建时间排序
                first_ts = singles.groupby("SKU")["创建时间"].min().to_dict()
                ok_skus.sort(key=lambda s: first_ts[s])

                # 构建 SKU -> 包裹列表（按时间），并剔除已分配的包裹
                sku_pkgs = {
                    sku: [p for p in singles.loc[singles["SKU"] == sku]
                                       .sort_values("创建时间")["Package ID"].tolist()
                          if p not in assigned_pkgs]
                    for sku in ok_skus
                }
                # 保留 3~6 件的有效“整包”
                sku_pkgs = {sku: pkgs for sku, pkgs in sku_pkgs.items() if 3 <= len(pkgs) <= 6}

                mix_idx = 0
                cur_chunk, cur_count = [], 0
                for sku, pkgs in sku_pkgs.items():
                    k = len(pkgs)  # 3~6
                    # 放不下就先落一单
                    if cur_count > 0 and cur_count + k > 50:
                        mix_idx += 1
                        b, u, s_ = summarize_picklist(avail, cur_chunk)
                        rows.append([d, "3-6", "MIX", f"{step_tag}-36-MIX-{mix_idx}", b, u, s_, cur_chunk])
                        assigned_pkgs.update(cur_chunk)
                        cur_chunk, cur_count = [], 0
                    # 放入当前拣选单
                    cur_chunk.extend(pkgs)
                    cur_count += k

                # 收尾
                if cur_chunk:
                    mix_idx += 1
                    b, u, s_ = summarize_picklist(avail, cur_chunk)
                    rows.append([d, "3-6", "MIX", f"{step_tag}-36-MIX-{mix_idx}", b, u, s_, cur_chunk])
                    assigned_pkgs.update(cur_chunk)

    elif strategy == "mod":
        # 总件数>1；≤30包裹/拣选单
        mods = per_pkg[per_pkg["总件数"] > 1]
        if not mods.empty:
            pkg_ids = mods["Package ID"].tolist()
            chunks = chunk_by_packages(pkg_ids, max_packages=30)
            for i, group in enumerate(chunks, 1):
                group = [p for p in group if p not in assigned_pkgs]
                if not group: 
                    continue
                b,u,s = summarize_picklist(avail, group)
                rows.append([d, "MOD", "MOD", f"{step_tag}-MOD-{i}", b,u,s, group])
                assigned_pkgs.update(group)

    else:  # daily
        # 剩余全部；≤30包裹/拣选单
        rest_ids = [p for p in per_pkg["Package ID"].tolist() if p not in assigned_pkgs]
        chunks = chunk_by_packages(rest_ids, max_packages=30)
        for i, group in enumerate(chunks, 1):
            if not group: 
                continue
            b,u,s = summarize_picklist(avail, group)
            rows.append([d, "Daily", "ALL", f"{step_tag}-DAILY-{i}", b,u,s, group])
            assigned_pkgs.update(group)

    return rows

# =========================
# 5) 解析输入（多条顺序策略）
# =========================
def parse_pipeline(inp: str):
    """
    输入示例：'04:00 top; 09:00 3-6; 11:30 mod; 12:00 daily'
    支持：'4点 top' / '09:00 3到6' / '09:00 3-6' / '剩下'->daily
    """
    items = re.split(r"[;,\n]+", inp.strip())
    out = []
    for it in items:
        it = it.strip().lower()
        if not it: 
            continue
        m = re.search(r"(\d{1,2})(?:[:：点时](\d{2}))?", it)
        if not m:
            raise ValueError(f"无法识别时间：{it}")
        hh = int(m.group(1)); mm = int(m.group(2) or 0)
        if not (0 <= hh <= 12 and 0 <= mm < 60):
            raise ValueError(f"时间必须在 00:00~12:00：{hh:02d}:{mm:02d}")
        t = time(hh, mm)

        if "top" in it:
            s = "top"
        elif "3-6" in it or "3到6" in it or "3~6" in it:
            s = "3-6"
        elif "mod" in it:
            s = "mod"
        elif "daily" in it or "剩下" in it:
            s = "daily"
        else:
            raise ValueError(f"无法识别策略（支持 top / 3-6 / mod / daily）：{it}")
        out.append((t, s))
    return out  # 保持输入顺序

# =========================
# 6) 主流程：弹窗获取输入 → 逐生产日按顺序执行
# =========================
def get_user_input_popup(default_text="04:00 top; 09:00 3-6; 12:00 daily"):
    """优先弹窗（tkinter），失败则回退到控制台 input()。"""
    try:
        import tkinter as tk
        from tkinter import simpledialog
        root = tk.Tk()
        root.withdraw()
        s = simpledialog.askstring(
            title="汇波策略输入",
            prompt="请输入策略（例：04:00 top; 09:00 3-6; 12:00 daily）：",
            initialvalue=default_text
        )
        try:
            root.destroy()
        except Exception:
            pass
        if s is None:
            # 用户点取消，回退到控制台
            s = input("请输入策略（例：04:00 top; 09:00 3-6; 12:00 daily）：\n> ")
        return s
    except Exception:
        # 无图形界面环境
        return input("请输入策略（例：04:00 top; 09:00 3-6; 12:00 daily）：\n> ")

def run_pipeline_with_input(daily_rows_map: dict):
    steps_text = get_user_input_popup()
    steps = parse_pipeline(steps_text)

    prod_days = sorted(daily_rows_map.keys())
    assigned = {d: set() for d in prod_days}

    all_rows = []
    for idx, (t, s) in enumerate(steps, start=1):
        step_tag = f"STEP{idx}-{t.strftime('%H%M')}"
        for d in prod_days:
            rows = apply_strategy_for_day(daily_rows_map[d], t, s, assigned[d], step_tag)
            all_rows.extend(rows)

    picklist_detail = pd.DataFrame(all_rows, columns=["生产日期","策略","分组","拣选单ID","包裹数","总件数","品类数","包裹清单"]) \
                        .sort_values(["生产日期","策略","拣选单ID"])
    daily_counts = (picklist_detail.groupby("生产日期")["拣选单ID"]
                    .nunique().reset_index(name="拣选单数量")
                    .sort_values("生产日期"))
    return steps_text, picklist_detail, daily_counts

# =========================
# 7) 执行（运行本脚本后会弹出输入框）
# =========================
steps_text, PICKLIST_DETAIL, DAILY_COUNTS = run_pipeline_with_input(DAILY_ROWS)

# —— 仅输出：总数、单品、两品、>2 平均品数、以及“≈2品包裹占比”（逐天，≈2品=品类数>=2） ——
print("【每天拣选单概览：总数 / 单品 / 两品 / >2 平均品数 / ≈2品包裹占比】")

# 每天×品类数 的计数表
by_day_kind = (
    PICKLIST_DETAIL
    .groupby(["生产日期", "品类数"])["拣选单ID"]
    .nunique()
    .reset_index(name="拣选单数")
)

# 每天总拣选单数
total_by_day = (
    PICKLIST_DETAIL
    .groupby("生产日期")["拣选单ID"]
    .nunique()
    .to_dict()
)

all_days = sorted(DAILY_ROWS.keys())

for d in all_days:
    sub_cnt = by_day_kind[by_day_kind["生产日期"] == d]

    total_cnt  = int(total_by_day.get(d, 0))
    single_cnt = int(sub_cnt.loc[sub_cnt["品类数"] == 1, "拣选单数"].sum()) if not sub_cnt.empty else 0
    two_cnt    = int(sub_cnt.loc[sub_cnt["品类数"] == 2, "拣选单数"].sum()) if not sub_cnt.empty else 0

    # >2 平均品数
    gt2_rows = PICKLIST_DETAIL[(PICKLIST_DETAIL["生产日期"] == d) & (PICKLIST_DETAIL["品类数"] > 2)]
    avg_str = f"{gt2_rows['品类数'].mean():.2f}" if len(gt2_rows) > 0 else "-"

    # —— ≈2品包裹占比（>=2）：分母=当天所有拣选单的包裹总数；分子=当天品类数>=2的拣选单的包裹总数 —— 
    pl_today = PICKLIST_DETAIL[PICKLIST_DETAIL["生产日期"] == d]
    total_packages = int(pl_today["包裹数"].sum())
    approx_two_packages = int(pl_today.loc[pl_today["品类数"] >= 2, "包裹数"].sum())
    ratio_str = f"{(approx_two_packages / total_packages * 100):.2f}%" if total_packages > 0 else "-"

    print(f"{d}: 总计 {total_cnt} 条，单品 {single_cnt} 条，两个品 {two_cnt} 条，>2 平均 {avg_str} 个品，散单占比 {ratio_str}")
