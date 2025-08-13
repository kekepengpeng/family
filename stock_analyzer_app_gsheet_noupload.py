
import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import date, datetime

st.set_page_config(page_title="Investment收益分析", layout="wide")
st.title("📈 Investment收益分析")
st.caption("步骤 1 导入 Fidelity数据；步骤 2 浏览分析结果；步骤 3 可选择保存 新Fidelity 到 Google。")

# ====================== Google Sheets 连接工具（无需上传，直接用 st.secrets） ======================
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def get_spreadsheet_id_from_url(url: str):
    if not url:
        return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return m.group(1) if m else None

@st.cache_resource(show_spinner=False)
def get_gspread_client_from_secrets():
    try:
        info = dict(st.secrets["gcp_service_account"])
    except Exception as e:
        st.error("未在 st.secrets 中找到 gcp_service_account。请在部署平台的 Secrets 中配置完整的 Service Account JSON。")
        st.stop()
    creds = Credentials.from_service_account_info(info, scopes=SCOPE)
    gc = gspread.authorize(creds)
    sa_email = info.get("client_email", "(unknown)")
    return gc, sa_email

def get_spreadsheet_from_secrets():
    # 不加 cache_resource；或者也可以加但不传参以避免哈希问题
    gc, _ = get_gspread_client_from_secrets()
    gsheet_url = st.secrets.get("google_sheet_url", "").strip()
    if not gsheet_url:
        st.error("未在 st.secrets 中找到 google_sheet_url。请填入你的 Google 表格链接。")
        st.stop()
    try:
        # 用 URL 打开（更稳，不依赖提取 ID）
        sh = gc.open_by_url(gsheet_url)
        return sh
    except Exception as e:
        # 回退用 ID
        sid = get_spreadsheet_id_from_url(gsheet_url)
        if not sid:
            st.error(f"无法从 google_sheet_url 解析表格ID：{gsheet_url}")
            st.stop()
        try:
            sh = gc.open_by_key(sid)
            return sh
        except Exception as ee:
            st.error(f"无法打开 Google 表格（请确认已将该表共享给 Service Account）：{ee}")
            st.stop()

def ensure_worksheet(sh, title: str, rows=1000, cols=26):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    return ws

# 拿到 gspread 客户端 & 表格
gc, sa_email = get_gspread_client_from_secrets()
sh = get_spreadsheet_from_secrets()
st.sidebar.success(f"已连接 Google 表格（Service Account: {sa_email}）")

# ====================== Fidelity 计算（不计费用，FIFO） ======================
def to_float(x):
    if pd.isna(x): return np.nan
    s = str(x).strip().replace(",", "").replace("$","")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except:
        return np.nan

def detect_columns(df):
    cols = {c.strip().lower(): c for c in df.columns}
    date_col = cols.get("trade date") or cols.get("run date") or cols.get("settlement date") or cols.get("date") or list(df.columns)[0]
    ticker_col = cols.get("symbol") or cols.get("ticker") or cols.get("security") or cols.get("instrument")
    action_col = cols.get("action") or cols.get("description") or cols.get("type")
    qty_col = cols.get("quantity") or cols.get("qty") or cols.get("shares")
    price_col = cols.get("price ($)") or cols.get("price") or cols.get("fill price")
    need = [date_col, ticker_col, action_col, qty_col, price_col]
    if any(x is None for x in need):
        st.error("无法识别必要列：日期、代码、操作、数量、价格。"); return None
    return {"date":date_col,"ticker":ticker_col,"action":action_col,"qty":qty_col,"price":price_col}

def map_action(s: str):
    s = str(s).upper()
    if "SOLD" in s: return "SELL"
    if "BOUGHT" in s: return "BUY"
    return None

def fmt_pct(x, digits=4):
    if x is None or pd.isna(x) or not np.isfinite(x): return "—"
    return f"{x*100:.{digits}f}%"

def prepare_trades(df: pd.DataFrame):
    mp = detect_columns(df)
    if mp is None: return None
    d = df.copy()
    d["日期"] = pd.to_datetime(d[mp["date"]], errors="coerce")
    d["代码"] = d[mp["ticker"]].astype(str).str.strip().str.upper()
    d["方向"] = d[mp["action"]].map(map_action)
    d["数量"] = d[mp["qty"]].apply(to_float).abs()
    d["价格"] = d[mp["price"]].apply(to_float)
    d = d[(~d["日期"].isna()) & (~d["代码"].isna()) & (~d["方向"].isna()) & (d["数量"]>0) & (~d["价格"].isna())]
    d = d.sort_values(["代码","日期"]).reset_index(drop=True)
    return d

def fifo_analyze(trade_df: pd.DataFrame):
    realized = []
    holdings = {}
    for _, row in trade_df.iterrows():
        tkr=row["代码"]; side=row["方向"]; qty=float(row["数量"]); price=float(row["价格"]); date=row["日期"]
        holdings.setdefault(tkr, [])
        if side=="BUY":
            total_cost = qty*price  # 不计费用
            holdings[tkr].append({"date":date, "qty":qty, "cps": total_cost/qty})
        elif side=="SELL":
            rem = qty
            while rem>0 and holdings[tkr]:
                lot = holdings[tkr][0]
                used = min(rem, lot["qty"])
                pnl_wo = used*(price - lot["cps"])  # 不计费用
                cost_used = used*lot["cps"]
                days = max((date - lot["date"]).days, 0)
                roi = pnl_wo / cost_used if cost_used>0 else np.nan
                ann = ((1+roi)**(365.0/days)-1) if (days>0 and pd.notna(roi)) else roi
                realized.append({
                    "代码": tkr,
                    "买入日期": lot["date"].date(),
                    "买入成本(每股)": lot["cps"],
                    "卖出日期": date.date(),
                    "卖出价": price,
                    "卖出股数": used,
                    "持有天数": days,
                    "利润(不含费用)": pnl_wo,
                    "收益率(不含费用)": roi,
                    "年化收益率(不含费用)": ann
                })
                lot["qty"] -= used
                rem -= used
                if lot["qty"] <= 1e-9:
                    holdings[tkr].pop(0)

    # 当前持仓（加权均价）
    hold_rows=[]
    for tkr,lots in holdings.items():
        qty = sum(l["qty"] for l in lots)
        if qty<=0: continue
        cost = sum(l["qty"]*l["cps"] for l in lots)
        hold_rows.append({"代码":tkr,"持仓数量":qty,"持仓均价":cost/qty,"持仓批次":len(lots)})
    holdings_df = pd.DataFrame(hold_rows).sort_values("代码")

    realized_df = pd.DataFrame(realized)

    # 总体口径
    if not realized_df.empty:
        realized_df["投入本金"] = realized_df["卖出股数"] * realized_df["买入成本(每股)"]
        invested_total = realized_df["投入本金"].sum()
        total_profit_excl = realized_df["利润(不含费用)"].sum()
        total_roi_excl = (total_profit_excl / invested_total) if invested_total>0 else np.nan
        first_buy_date = trade_df.loc[trade_df["方向"]=="BUY","日期"].min()
        today = pd.Timestamp.today().normalize()
        span_days = max((today - first_buy_date).days, 1) if pd.notna(first_buy_date) else np.nan
        total_ann_excl = (1 + total_roi_excl)**(365.0/span_days) - 1 if (pd.notna(total_roi_excl) and pd.notna(span_days)) else np.nan

        per_ticker = realized_df.groupby("代码").apply(
            lambda g: pd.Series({
                "已卖出笔数": len(g),
                "已卖出股数": g["卖出股数"].sum(),
                "利润合计(不含费用)": g["利润(不含费用)"].sum(),
                "投入本金合计": g["投入本金"].sum(),
                "成本加权收益率(不含费用)": (g["利润(不含费用)"].sum()/g["投入本金"].sum()) if g["投入本金"].sum()>0 else np.nan,
                "成本加权年化收益率(不含费用)": ((g["年化收益率(不含费用)"]*g["投入本金"]).sum()/g["投入本金"].sum()) if g["投入本金"].sum()>0 else np.nan
            })
        ).reset_index()
    else:
        invested_total=0.0; total_profit_excl=0.0
        total_roi_excl=np.nan; total_ann_excl=np.nan; span_days=np.nan
        per_ticker = pd.DataFrame(columns=["代码","已卖出笔数","已卖出股数","利润合计(不含费用)","投入本金合计","成本加权收益率(不含费用)","成本加权年化收益率(不含费用)"])

    totals = {
        "总投入本金": invested_total,
        "总利润(不含费用)": total_profit_excl,
        "总体收益率(不含费用)": total_roi_excl,
        "总体年化收益率(不含费用)": total_ann_excl,
        "统计区间天数": span_days
    }
    return holdings_df, realized_df, per_ticker, totals

def apply_pct_format(df, cols, digits=4):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: fmt_pct(v, digits))
    return df

# ====================== 🟢 步骤 1｜导入 Fidelity 数据 ======================
st.subheader("🟢 步骤 1｜导入 Fidelity 数据")
st.caption("选择数据来源：从 Google 读取历史结果（默认）或上传新的 Fidelity CSV。")

holdings_df = pd.DataFrame(); realized_df = pd.DataFrame(); per_ticker = pd.DataFrame(); totals = None

src_choice = st.radio("选择数据来源：", options=["从 Google 表格读取", "上传新的 Fidelity CSV"], index=0, horizontal=True)

if src_choice == "上传新的 Fidelity CSV":
    raw_csv = st.file_uploader("选择 CSV 文件（Fidelity 导出）", type=["csv"], key="u_fid_csv")
    if raw_csv is not None:
        try:
            raw_df = pd.read_csv(raw_csv)
            trade_df = prepare_trades(raw_df)
            if trade_df is None or trade_df.empty:
                st.warning("未识别到有效的买入/卖出记录。")
            else:
                holdings_df, realized_df, per_ticker, totals = fifo_analyze(trade_df)
                st.success("Fidelity CSV 已解析。")
        except Exception as e:
            st.error(f"原始CSV解析失败：{e}")
else:
    # 直接自动从 Google 读取，无需按钮
    try:
        ws_h = ensure_worksheet(sh, "Fidelity_Holdings")
        ws_r = ensure_worksheet(sh, "Fidelity_Realized")
        ws_p = ensure_worksheet(sh, "Fidelity_PerTicker")
        ws_t = ensure_worksheet(sh, "Fidelity_Totals")
        holdings_df = get_as_dataframe(ws_h, evaluate_formulas=True).dropna(how="all")
        realized_df = get_as_dataframe(ws_r, evaluate_formulas=True).dropna(how="all")
        per_ticker = get_as_dataframe(ws_p, evaluate_formulas=True).dropna(how="all")
        totals_df = get_as_dataframe(ws_t, evaluate_formulas=True).dropna(how="all")
        if not totals_df.empty and "key" in totals_df.columns and "value" in totals_df.columns:
            totals = {row["key"]: row["value"] for _, row in totals_df.iterrows()}
        st.success("已从 Google 加载 Fidelity 结果。")
    except Exception as e:
        st.error(f"读取失败：{e}")

# ====================== 🔵 步骤 2｜浏览结果 ======================
st.subheader("🔵 步骤 2｜浏览结果")
st.caption("在下方各个 Tab 查看 Fidelity 结果；第 4 个 Tab 自动从 Google 读取手动条目。")

tab1, tab2, tab3, tab4 = st.tabs(["① 当前仍持有（Fidelity）", "② 已卖出总利润（Fidelity）", "③ 每只收益率与年化（Fidelity）", "④ 手动条目"])

with tab1:
    st.subheader("📌 当前仍持有（加权均价 & 数量）")
    if not holdings_df.empty:
        st.dataframe(holdings_df.round(6), use_container_width=True)
    else:
        st.write("（暂无数据）")

with tab2:
    st.subheader("💰 已卖出部分总利润（以及总体收益率 & 年化）")
    if totals is None or (isinstance(totals, dict) and len(totals)==0):
        st.write("（暂无数据）")
    else:
        try:
            c1, c2 = st.columns(2)
            c1.metric("总利润（不含费用）", f"{float(totals['总利润(不含费用)']):,.2f}")
            c2.metric("总投入本金", f"{float(totals['总投入本金']):,.2f}")
            def pct_fmt(v): 
                try: 
                    return f"{float(v)*100:.4f}%"
                except: 
                    return "—"
            c3, c4 = st.columns(2)
            c3.metric("总体收益率（不含费用）", pct_fmt(totals['总体收益率(不含费用)']))
            c4.metric("总体年化收益率（不含费用）", pct_fmt(totals['总体年化收益率(不含费用)']))
            span_days = int(float(totals["统计区间天数"])) if "统计区间天数" in totals and totals["统计区间天数"] not in [None, ""] else "—"
            st.caption(f"统计区间：自首次买入至今日，合计 {span_days} 天")
        except Exception:
            st.write("（Totals 字段格式不完整）")
        with st.expander("📄 卖出拆分至买入批次（FIFO 明细）"):
            if '收益率(不含费用)' in realized_df.columns and realized_df['收益率(不含费用)'].dtype != 'O':
                realized_df_fmt = realized_df.copy()
                realized_df_fmt["收益率(不含费用)"] = realized_df_fmt["收益率(不含费用)"].apply(lambda v: fmt_pct(v, 4))
                realized_df_fmt["年化收益率(不含费用)"] = realized_df_fmt["年化收益率(不含费用)"].apply(lambda v: fmt_pct(v, 4))
                st.dataframe(realized_df_fmt, use_container_width=True)
            else:
                st.dataframe(realized_df, use_container_width=True)

with tab3:
    st.subheader("📊 每只已卖出股票的收益率与年化（仅针对已卖出部分）")
    if per_ticker is not None and not per_ticker.empty:
        pt = per_ticker.copy()
        for col in ["成本加权收益率(不含费用)","成本加权年化收益率(不含费用)"]:
            if col in pt.columns and pt[col].dtype != 'O':
                pt[col] = pt[col].apply(lambda v: fmt_pct(v, 6))
        st.dataframe(pt.round(6), use_container_width=True)
        st.markdown("——")
        if totals and isinstance(totals, dict):
            try:
                st.write(f"**总投入本金**：{float(totals['总投入本金']):,.2f}")
                st.write(f"**总体收益率（不含费用）**：{float(totals['总体收益率(不含费用)'])*100:.6f}%")
                st.write(f"**总体年化收益率（不含费用）**：{float(totals['总体年化收益率(不含费用)'])*100:.6f}%")
            except Exception:
                pass
    else:
        st.write("（暂无数据）")

with tab4:
    st.subheader("✍️ 手动条目（只读，自动从 Google 读取）")
    st.caption("列：代码｜日期｜成本｜收益金额｜备注。页面自动计算每行收益率并显示合计。")
    try:
        ws_manual = ensure_worksheet(sh, "ManualEntries")
        dfm = get_as_dataframe(ws_manual, evaluate_formulas=True).dropna(how="all")
        expected = ["代码","日期","成本","收益金额","备注"]
        if not dfm.empty:
            for col in expected:
                if col not in dfm.columns:
                    dfm[col] = "" if col in ["代码","日期","备注"] else 0.0
            dfm = dfm[expected]
        else:
            dfm = pd.DataFrame(columns=expected)
        # 计算收益率列并展示
        def calc_roi(row):
            c = row.get("成本", 0.0)
            a = row.get("收益金额", 0.0)
            if pd.isna(c) or c == 0:
                return np.nan
            try:
                return float(a)/float(c)
            except:
                return np.nan
        if not dfm.empty:
            dfm["收益率"] = dfm.apply(calc_roi, axis=1)
            dfm_disp = dfm.copy()
            dfm_disp["收益率"] = dfm_disp["收益率"].apply(lambda v: fmt_pct(v, 4))
            st.dataframe(dfm_disp, use_container_width=True)
            st.metric("手动条目收益合计", f"{dfm['收益金额'].sum():,.2f}")
        else:
            st.write("（Google 上暂无手动条目）")
            st.metric("手动条目收益合计", f"{0.0:,.2f}")
    except Exception as e:
        st.error(f"读取手动条目失败：{e}")

st.markdown("---")

# ====================== 🟣 步骤 3｜保存到 Google（仅覆盖 Fidelity） ======================
st.subheader("🟣 步骤 3｜保存到 Google（仅覆盖 Fidelity 数据）")
st.caption("点击后只写入 Fidelity_Holdings / Fidelity_Realized / Fidelity_PerTicker / Fidelity_Totals 四张表；ManualEntries 不改动。")
if st.button("💾 保存最新 Fidelity 数据到 Google（覆盖旧数据）"):
    try:
        # 1) 写 Fidelity 四张表
        ws_h = ensure_worksheet(sh, "Fidelity_Holdings")
        ws_r = ensure_worksheet(sh, "Fidelity_Realized")
        ws_p = ensure_worksheet(sh, "Fidelity_PerTicker")
        ws_t = ensure_worksheet(sh, "Fidelity_Totals")
        def safe_df(df): 
            return df.reset_index(drop=True) if df is not None else pd.DataFrame()
        set_with_dataframe(ws_h, safe_df(holdings_df))
        set_with_dataframe(ws_r, safe_df(realized_df))
        set_with_dataframe(ws_p, safe_df(per_ticker))
        totals_map = {} if (not isinstance(totals, dict)) else totals
        tdf = pd.DataFrame({"key": list(totals_map.keys()), "value": list(totals_map.values())})
        set_with_dataframe(ws_t, tdf)
        st.success("已覆盖写入 Fidelity 四张表；ManualEntries 未改动。")
    except Exception as e:
        st.error(f"保存失败：{e}")
