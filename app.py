# app.py
import re
import pandas as pd
import streamlit as st
from datetime import datetime, date, time, timedelta

# =========================
# 页面与基本配置
# =========================
st.set_page_config(page_title="拣选汇波助手", layout="wide")
st.title("拣选汇波助手")

# 别名与必需列
ALIASES = {
    "Package ID":   ["Package ID","package_id","包裹号","包裹编号","包裹编码","包裹id"],
    "Goods ID":     ["Goods ID","goods_id","sku_id","SKU ID","SKU","货品编码","商品编码","SKU编码"],
    "Goods quantity":["Goods quantity","quantity","qty","件数","数量","Goods qty","货品数量"],
    "Created time": ["Created time","created_time","create_time","创建时间","下单时间","订单创建时间"],
}
NEEDED = ["Package ID","Goods ID","Goods quantity","Created time"]
NOON = time(12, 0)

# =========================
# 列映射&清洗
# =========================
def _map_aliases(header_cols):
    lower = {str(c).strip().lower(): c for c in header_cols}
    col_map = {}
    for std, aliases in ALIASES.items():
        hit = None
        for a in aliases:
            key = str(a).strip().lower()
            if key in lower:
                hit = lower[key]; break
        if not hit:
            raise KeyError(f"找不到必需列：{std}（支持别名：{ALIASES[std]}）")
        col_map[hit] = std
    return col_map

def _post_clean(df: pd.DataFrame) -> pd.DataFrame:
    df["Package ID"] = df["Package ID"].astype(str)
    df["Goods ID"]   = df["Goods ID"].astype(str)
    df["Goods quantity"] = pd.to_numeric(df["Goods quantity"], errors="coerce").fillna(0).astype("int32")
    # 先尝试常见格式，失败再兜底
    try:
        df["Created time"] = pd.to_datetime(df["Created time"], format="%Y-%m-%d %H:%M:%S", errors="raise")
    except Exception:
        df["Created time"] = pd.to_datetime(df["Created time"], errors="coerce")
    df = df.dropna(subset=["Created time"]).reset_index(drop=True)
    return df

# =========================
# 读取上传文件（CSV 分块 / Excel 一次读）
# =========================
@st.cache_data(show_spinner=False)
def load_minimal_df_from_upload(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()

    # Excel
    if name.endswith((".xlsx", ".xls")):
        raw = pd.read_excel(uploaded_file, engine="openpyxl")
        col_map = _map_aliases(raw.columns)
        out = raw.rename(columns=col_map)[NEEDED].copy()
        return _post_clean(out)

    # CSV：先读表头拿映射
    uploaded_file.seek(0)
    head = pd.read_csv(uploaded_file, nrows=0)
    col_map = _map_aliases(head.columns)

    # 分块读取
    uploaded_file.seek(0)
    chunks = []
    total = 0
    for i, chunk in enumerate(pd.read_csv(
        uploaded_file,
        usecols=list(col_map.keys()),
        chunksize=200_000,
        dtype=str,
        low_memory=True,
        on_bad_lines="skip",
    ), start=1):
        c = chunk.rename(columns=col_map)
        c = c[NEEDED]
        c = _post_clean(c)
        chunks.append(c)
        total += len(c)
        if i % 5 == 0:
            st.write(f"· 已读取 {total:,} 行…")

    if not chunks:
        return pd.DataFrame(columns=NEEDED)
    return pd.concat(chunks, ignore_index=True)

# =========================
# 工具：生产窗口与切块
# =========================
def day_window(d: date):
    """生产日 d 的窗口：[d-1 12:00, d 12:00)"""
    end_dt = datetime.combine(d, NOON)
    start_dt = end_dt - timedelta(days=1)
    return start_dt, end_dt

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
# 核心：按策略在某生产日执行
# =========================
def apply_strategy_for_day(day_rows: pd.DataFrame, cutoff_t: time, strategy: str, assigned_pkgs: set, step_tag: str):
    if day_rows.empty:
        return []
    d = day_rows["生产日期"].iloc[0]
    cutoff_dt = datetime.combine(d, cutoff_t)

    # 未分配 & 截止前
    avail = day_rows[(~day_rows["Package ID"].isin(assigned_pkgs)) & (day_rows["Created time"] < cutoff_dt)].copy()
    if avail.empty:
        return []

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
        # 单品单件：SKU >= 7 包裹；≤999件/单（按件切）
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

        # 双品组合：2件&2个不同SKU，组合>=7 包裹；≤999件/单（容量= floor(999/2)）
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
        # 混装（按 SKU 整包装入，不拆分）：单品单件且该 SKU 在窗口内 3~6 包裹；≤50 包裹/单
        singles = per_pkg[(per_pkg["总件数"] == 1) & (per_pkg["品类数"] == 1)].copy()
        if not singles.empty:
            singles["SKU"] = singles["SKU列表"].apply(lambda t: t[0])
            cnt = singles.groupby("SKU")["Package ID"].nunique().reset_index(name="包裹数")
            ok_skus = cnt.loc[cnt["包裹数"].between(3, 6), "SKU"].tolist()
            if ok_skus:
                first_ts = singles.groupby("SKU")["创建时间"].min().to_dict()
                ok_skus.sort(key=lambda s: first_ts[s])

                sku_pkgs = {
                    sku: [p for p in singles.loc[singles["SKU"] == sku]
                                       .sort_values("创建时间")["Package ID"].tolist()
                          if p not in assigned_pkgs]
                    for sku in ok_skus
                }
                sku_pkgs = {sku: pkgs for sku, pkgs in sku_pkgs.items() if 3 <= len(pkgs) <= 6}

                mix_idx = 0
                cur_chunk, cur_count = [], 0
                for sku, pkgs in sku_pkgs.items():
                    k = len(pkgs)  # 3~6
                    if cur_count > 0 and cur_count + k > 50:
                        mix_idx += 1
                        b, u, s_ = summarize_picklist(avail, cur_chunk)
                        rows.append([d, "3-6", "MIX", f"{step_tag}-36-MIX-{mix_idx}", b, u, s_, cur_chunk])
                        assigned_pkgs.update(cur_chunk)
                        cur_chunk, cur_count = [], 0
                    cur_chunk.extend(pkgs)
                    cur_count += k

                if cur_chunk:
                    mix_idx += 1
                    b, u, s_ = summarize_picklist(avail, cur_chunk)
                    rows.append([d, "3-6", "MIX", f"{step_tag}-36-MIX-{mix_idx}", b, u, s_, cur_chunk])
                    assigned_pkgs.update(cur_chunk)

    elif strategy == "mod":
        # 总件数>1；≤30 包裹/单
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
        # 剩余全部；≤30 包裹/单
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
# 解析输入策略
# =========================
def parse_pipeline(inp: str):
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
    return out

# =========================
# UI：策略输入 & 文件上传
# =========================
default_steps = "04:00 top; 07:00 top; 08:00 3-6; 09:00 mod; 12:00 daily"
steps_text = st.sidebar.text_input(
    "多条策略（顺序执行）",
    value=default_steps,
    help="示例：04:00 top; 09:00 3-6; 12:00 daily（支持 top / 3-6 / mod / daily）"
)

file = st.file_uploader("上传报表（CSV 或 Excel）", type=["csv","xlsx","xls"])

if file is None:
    st.info("请先上传报表文件。")
    st.stop()

# 读取数据
try:
    with st.spinner("正在读取并标准化（大文件请稍等）…"):
        df = load_minimal_df_from_upload(file)
    st.success(f"文件读取成功：{len(df):,} 行；时间范围 {df['Created time'].min()} → {df['Created time'].max()}")
except Exception as e:
    st.error("读取/标准化失败，请检查列名或文件格式。")
    st.exception(e)
    st.stop()

# =========================
# 准备生产日切片
# =========================
earliest = df["Created time"].min()
latest   = df["Created time"].max()
first_prod_date = (earliest.date() + timedelta(days=1)) if earliest.time() < NOON else (earliest.date() + timedelta(days=2))
last_prod_date  = (latest.date() - timedelta(days=1))   if latest.time()   < NOON else  latest.date()

DAILY_ROWS = {}
cur = first_prod_date
while cur <= last_prod_date:
    s, e = day_window(cur)
    day_rows = df[(df["Created time"] >= s) & (df["Created time"] < e)].copy()
    day_rows["生产日期"] = cur
    DAILY_ROWS[cur] = day_rows
    cur += timedelta(days=1)

# =========================
# 运行与结果
# =========================
def run_pipeline_with_text(daily_rows_map: dict, steps_text: str):
    steps = parse_pipeline(steps_text)
    prod_days = sorted(daily_rows_map.keys())
    assigned = {d: set() for d in prod_days}

    all_rows = []
    for idx, (t, s) in enumerate(steps, start=1):
        step_tag = f"STEP{idx}-{t.strftime('%H%M')}"
        for d in prod_days:
            rows = apply_strategy_for_day(daily_rows_map[d], t, s, assigned[d], step_tag)
            all_rows.extend(rows)

    picklist_detail = pd.DataFrame(
        all_rows,
        columns=["生产日期","策略","分组","拣选单ID","包裹数","总件数","品类数","包裹清单"]
    ).sort_values(["生产日期","策略","拣选单ID"])
    daily_counts = (picklist_detail.groupby("生产日期")["拣选单ID"]
                    .nunique().reset_index(name="拣选单数量")
                    .sort_values("生产日期"))
    return picklist_detail, daily_counts

if st.button("开始计算", type="primary", use_container_width=True):
    try:
        with st.spinner("正在执行汇波策略…"):
            PICKLIST_DETAIL, DAILY_COUNTS = run_pipeline_with_text(DAILY_ROWS, steps_text)

        if PICKLIST_DETAIL.empty:
            st.warning("未生成任何拣选单（窗口内可能没有完整订单或策略筛空）。")
            st.stop()

        # 1) 每天的拣选单数量表
        st.subheader("每天拣选单数量")
        st.dataframe(DAILY_COUNTS, use_container_width=True)

        # 2) 每天概览：总数 / 单品 / 两品 / >2 平均品数 / ≥2 品包裹占比
        st.subheader("每天拣选单概览")
        by_day_kind = (PICKLIST_DETAIL.groupby(["生产日期","品类数"])["拣选单ID"]
                       .nunique().reset_index(name="拣选单数"))
        total_by_day = (PICKLIST_DETAIL.groupby("生产日期")["拣选单ID"]
                        .nunique().to_dict())

        lines = ["【每天拣选单概览：总数 / 单品 / 两品 / >2 平均品数 / ≥2品包裹占比】"]
        for d in sorted(DAILY_ROWS.keys()):
            sub_cnt = by_day_kind[by_day_kind["生产日期"] == d]
            total_cnt  = int(total_by_day.get(d, 0))
            single_cnt = int(sub_cnt.loc[sub_cnt["品类数"] == 1, "拣选单数"].sum()) if not sub_cnt.empty else 0
            two_cnt    = int(sub_cnt.loc[sub_cnt["品类数"] == 2, "拣选单数"].sum()) if not sub_cnt.empty else 0
            gt2_rows = PICKLIST_DETAIL[(PICKLIST_DETAIL["生产日期"] == d) & (PICKLIST_DETAIL["品类数"] > 2)]
            avg_str = f"{gt2_rows['品类数'].mean():.2f}" if len(gt2_rows) > 0 else "-"
            pl_today = PICKLIST_DETAIL[PICKLIST_DETAIL["生产日期"] == d]
            total_packages = int(pl_today["包裹数"].sum())
            ge2_packages = int(pl_today.loc[pl_today["品类数"] >= 2, "包裹数"].sum())
            ratio_str = f"{(ge2_packages / total_packages * 100):.2f}%" if total_packages > 0 else "-"
            lines.append(f"{d}: 总计 {total_cnt} 条，单品 {single_cnt} 条，两个品 {two_cnt} 条，>2 平均 {avg_str} 个品，≥2品包裹占比 {ratio_str}")

        st.code("\n".join(lines), language="text")
        st.toast("计算完成 ✅", icon="✅")

    except Exception as e:
        st.error("计算失败，请查看异常：")
        st.exception(e)
