import re
from datetime import datetime, date, time, timedelta
import pandas as pd
import streamlit as st

# ====== 配置 ======
NOON = time(12, 0)
ALIASES = {
    "Package ID":   ["Package ID","package_id","包裹号","包裹编号","包裹编码","包裹id"],
    "Goods ID":     ["Goods ID","goods_id","sku_id","SKU ID","SKU","货品编码","商品编码","SKU编码"],
    "Goods quantity":["Goods quantity","quantity","qty","件数","数量","Goods qty","货品数量"],
    "Created time": ["Created time","created_time","create_time","创建时间","下单时间","订单创建时间"],
}

# ====== UI ======
st.set_page_config(page_title="拣选汇波助手", layout="wide")
st.title("拣选汇波助手")
with st.sidebar:
    st.subheader("策略输入")
    user_input = st.text_input(
        "多条策略（顺序执行）",
        value="04:00 top; 07:00 top; 08:00 3-6; 09:00 mod; 12:00 daily",
        help="时间 00:00~12:00；支持 top / 3-6 / mod / daily"
    )
    st.caption("必需列（中英/大小写均可）：Package ID, Goods ID, Goods quantity, Created time")

file = st.file_uploader("上传报表（CSV 或 Excel）", type=["csv","xlsx","xls"])

# ====== 工具函数 ======
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    lower_cols = {str(c).strip().lower(): c for c in df.columns}
    col_map = {}
    for std, aliases in ALIASES.items():
        hit = None
        for a in aliases:
            key = str(a).strip().lower()
            if key in lower_cols:
                hit = lower_cols[key]; break
        if not hit:
            raise KeyError(f"找不到必需列：{std}（可用别名：{ALIASES[std]}）")
        col_map[hit] = std
    out = df.rename(columns=col_map)[["Package ID","Goods ID","Goods quantity","Created time"]].copy()
    out["Package ID"] = out["Package ID"].astype(str)
    out["Goods ID"]   = out["Goods ID"].astype(str)
    out["Goods quantity"] = pd.to_numeric(out["Goods quantity"], errors="coerce").fillna(0).astype(int)
    out["Created time"] = pd.to_datetime(out["Created time"], errors="coerce")
    out = out.dropna(subset=["Created time"]).reset_index(drop=True)
    return out

def day_window(d: date):
    end_dt = datetime.combine(d, NOON)
    start_dt = end_dt - timedelta(days=1)
    return start_dt, end_dt

def parse_pipeline(inp: str):
    items = re.split(r"[;,\n]+", inp.strip())
    out = []
    for it in items:
        it = it.strip().lower()
        if not it: continue
        m = re.search(r"(\d{1,2})(?:[:：点时](\d{2}))?", it)
        if not m: raise ValueError(f"无法识别时间：{it}")
        hh = int(m.group(1)); mm = int(m.group(2) or 0)
        if not (0 <= hh <= 12 and 0 <= mm < 60):
            raise ValueError(f"时间必须在 00:00~12:00：{hh:02d}:{mm:02d}")
        t = time(hh, mm)
        if "top" in it: s = "top"
        elif "3-6" in it or "3到6" in it or "3~6" in it: s = "3-6"
        elif "mod" in it: s = "mod"
        elif "daily" in it or "剩下" in it: s = "daily"
        else: raise ValueError(f"无法识别策略：{it}")
        out.append((t, s))
    return out

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
    return (len(pkg_list), int(sub["Goods quantity"].sum()), int(sub["Goods ID"].nunique()))

def apply_strategy_for_day(day_rows: pd.DataFrame, cutoff_t: time, strategy: str, assigned_pkgs: set, step_tag: str):
    if day_rows.empty: return []
    d = day_rows["生产日期"].iloc[0]
    cutoff_dt = datetime.combine(d, cutoff_t)
    avail = day_rows[(~day_rows["Package ID"].isin(assigned_pkgs)) & (day_rows["Created time"] < cutoff_dt)].copy()
    if avail.empty: return []
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
                    if b == 0: continue
                    rows.append([d, "Top", f"SKU:{sku}", f"{step_tag}-TOP-SKU-{sku}-{i}", b,u,s, group])
                    assigned_pkgs.update(group)
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
                    if not group: continue
                    b,u,s = summarize_picklist(avail, group)
                    rows.append([d, "Top", f"PAIR:{pair}", f"{step_tag}-TOP-PAIR-{pair}-{i}", b,u,s, group])
                    assigned_pkgs.update(group)
    elif strategy == "3-6":
        singles = per_pkg[(per_pkg["总件数"] == 1) & (per_pkg["品类数"] == 1)].copy()
        if not singles.empty:
            singles["SKU"] = singles["SKU列表"].apply(lambda t: t[0])
            cnt = singles.groupby("SKU")["Package ID"].nunique().reset_index(name="包裹数")
            ok_skus = cnt.loc[cnt["包裹数"].between(3,6), "SKU"].tolist()
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
                for _, pkgs in sku_pkgs.items():
                    k = len(pkgs)
                    if cur_count > 0 and cur_count + k > 50:
                        mix_idx += 1
                        b,u,s = summarize_picklist(avail, cur_chunk)
                        rows.append([d, "3-6", "MIX", f"{step_tag}-36-MIX-{mix_idx}", b,u,s, cur_chunk])
                        assigned_pkgs.update(cur_chunk)
                        cur_chunk, cur_count = [], 0
                    cur_chunk.extend(pkgs); cur_count += k
                if cur_chunk:
                    mix_idx += 1
                    b,u,s = summarize_picklist(avail, cur_chunk)
                    rows.append([d, "3-6", "MIX", f"{step_tag}-36-MIX-{mix_idx}", b,u,s, cur_chunk])
                    assigned_pkgs.update(cur_chunk)
    elif strategy == "mod":
        mods = per_pkg[per_pkg["总件数"] > 1]
        if not mods.empty:
            pkg_ids = mods["Package ID"].tolist()
            chunks = chunk_by_packages(pkg_ids, max_packages=30)
            for i, group in enumerate(chunks, 1):
                group = [p for p in group if p not in assigned_pkgs]
                if not group: continue
                b,u,s = summarize_picklist(avail, group)
                rows.append([d, "MOD", "MOD", f"{step_tag}-MOD-{i}", b,u,s, group])
                assigned_pkgs.update(group)
    else:
        rest_ids = [p for p in per_pkg["Package ID"].tolist() if p not in assigned_pkgs]
        chunks = chunk_by_packages(rest_ids, max_packages=30)
        for i, group in enumerate(chunks, 1):
            if not group: continue
            b,u,s = summarize_picklist(avail, group)
            rows.append([d, "Daily", "ALL", f"{step_tag}-DAILY-{i}", b,u,s, group])
            assigned_pkgs.update(group)
    return rows

def run_pipeline(df: pd.DataFrame, user_input: str):
    earliest = df["Created time"].min()
    latest   = df["Created time"].max()
    first_prod = (earliest.date() + timedelta(days=1)) if earliest.time() < NOON else (earliest.date() + timedelta(days=2))
    last_prod  = (latest.date() - timedelta(days=1)) if latest.time() < NOON else latest.date()

    daily_rows = {}
    cur = first_prod
    while cur <= last_prod:
        s, e = day_window(cur)
        day_rows = df[(df["Created time"] >= s) & (df["Created time"] < e)].copy()
        day_rows["生产日期"] = cur
        daily_rows[cur] = day_rows
        cur += timedelta(days=1)

    steps = parse_pipeline(user_input)
    assigned = {d: set() for d in daily_rows}
    all_rows = []
    for idx, (t, s) in enumerate(steps, start=1):
        step_tag = f"STEP{idx}-{t.strftime('%H%M')}"
        for d in sorted(daily_rows):
            rows = apply_strategy_for_day(daily_rows[d], t, s, assigned[d], step_tag)
            all_rows.extend(rows)

    picklists = pd.DataFrame(all_rows, columns=["生产日期","策略","分组","拣选单ID","包裹数","总件数","品类数","包裹清单"]) \
                 .sort_values(["生产日期","策略","拣选单ID"])
    return picklists

def summarize_for_view(picklists: pd.DataFrame):
    # 每天总拣选单数
    total_by_day = picklists.groupby("生产日期")["拣选单ID"].nunique().rename("总拣选单数")
    # 单品/两品条数
    by_kind = (picklists.groupby(["生产日期","品类数"])["拣选单ID"]
               .nunique().reset_index(name="拣选单数"))
    one = by_kind[by_kind["品类数"]==1].set_index("生产日期")["拣选单数"].rename("单品条数")
    two = by_kind[by_kind["品类数"]==2].set_index("生产日期")["拣选单数"].rename("两品条数")
    # >2 平均品数
    gt2 = picklists[picklists["品类数"]>2].groupby("生产日期")["品类数"].mean().round(2).rename(">2平均品数")
    # “≈2品包裹占比”——按你的口径：品类数 >= 2
    total_pkg = picklists.groupby("生产日期")["包裹数"].sum().rename("总包裹数")
    approx2_pkg = picklists[picklists["品类数"] >= 2].groupby("生产日期")["包裹数"].sum().rename("散单包裹数(≥2)")
    ratio = ((approx2_pkg / total_pkg) * 100).round(2).rename("散单占比(%)")

    out = (pd.concat([total_by_day, one, two, gt2, ratio], axis=1)
             .fillna({"单品条数":0, "两品条数":0})
             .sort_index())
    return out.reset_index()

# ====== 主流程 ======
if file is None:
    st.info("请上传报表文件（CSV/Excel）。")
else:
    try:
        if file.name.lower().endswith((".xlsx",".xls")):
            raw = pd.read_excel(file)
        else:
            raw = pd.read_csv(file)
        df = standardize_columns(raw)
    except Exception as e:
        st.error(f"读取/标准化失败：{e}")
        st.stop()

    if st.button("开始计算", type="primary", use_container_width=True):
        try:
            picklists = run_pipeline(df, user_input)
            if picklists.empty:
                st.warning("未生成任何拣选单（检查时间范围或策略输入）")
                st.stop()

            summary = summarize_for_view(picklists)
            st.subheader("每天拣选单概览")
            st.dataframe(summary, use_container_width=True)

            csv = summary.to_csv(index=False).encode("utf-8-sig")
            st.download_button("下载概览CSV", data=csv, file_name="daily_summary.csv", mime="text/csv")

        except Exception as e:
            st.error(f"计算失败：{e}")
