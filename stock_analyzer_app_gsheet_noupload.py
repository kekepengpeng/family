import os
import json
import re
from datetime import date, datetime

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Investment收益分析", layout="wide")
st.title("📈 Investment收益分析")
st.caption("步骤 1 导入 Fidelity数据；步骤 2 浏览分析结果；步骤 3 可选择保存 新Fidelity 到 Google。")

# ====================== Google Sheets 连接工具（支持多种 secrets 写法） ======================
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def get_spreadsheet_id_from_url(url: str):
    if not url:
        return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return m.group(1) if m else None

def load_service_account_info_from_secrets():
    
    if "gcp_service_account" in st.secrets:
        v = st.secrets["gcp_service_account"]
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception as e:
                st.error(f"gcp_service_account 是字符串但不是有效 JSON：{e}")
                st.stop()
        else:
            try:
                return dict(v)
            except Exception as e:
                st.error(f"无法解析 gcp_service_account（应为表/字典或 JSON 字符串）：{e}")
                st.stop()

    if "gcp_service_account_json" in st.secrets:
        try:
            return json.loads(st.secrets["gcp_service_account_json"])
        except Exception as e:
            st.error(f"gcp_service_account_json 不是有效 JSON：{e}")
            st.stop()

    st.error("未在 st.secrets 中找到 gcp_service_account 或 gcp_service_account_json。请配置 Service Account JSON。")
    st.stop()

def load_google_sheet_url_from_secrets():
    url = st.secrets.get("google_sheet_url", "").strip() if hasattr(st, "secrets") else ""
    if not url:
        url = os.environ.get("GOOGLE_SHEET_URL", "").strip()
    if not url:
        st.error("未在 st.secrets 中找到 google_sheet_url（或环境变量 GOOGLE_SHEET_URL）。请填入你的 Google 表格链接。")
        st.stop()
    return url

@st.cache_resource(show_spinner=False)
def get_gspread_client_from_secrets():
    info = load_service_account_info_from_secrets()
    creds = Credentials.from_service_account_info(info, scopes=SCOPE)
    gc = gspread.authorize(creds)
    sa_email = info.get("client_email", "(unknown)")
    return gc, sa_email

def get_spreadsheet_from_secrets():
    gc, _ = get_gspread_client_from_secrets()
    gsheet_url = load_google_sheet_url_from_secrets()
    try:
        return gc.open_by_url(gsheet_url)
    except Exception:
        sid = get_spreadsheet_id_from_url(gsheet_url)
        if not sid:
            st.error(f"无法从 google_sheet_url 解析表格ID：{gsheet_url}")
            st.stop()
        try:
            return gc.open_by_key(sid)
        except Exception as ee:
            st.error(f"无法打开 Google 表格（请确认已将该表共享给 Service Account 的 client_email）：{ee}")
            st.stop()

def ensure_worksheet(sh, title: str, rows=1000, cols=26):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))

# ===== Helpers: 用 UNFORMATTED_VALUE 读取，避免数字被谷歌当作日期 =====
def _col_letter(n):
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def ws_to_dataframe_unformatted(ws, max_rows=5000):
    header = ws.row_values(1)
    if not header:
        return pd.DataFrame()
    ncols = len(header)
    last_col_letter = _col_letter(ncols)
    rng = f"A1:{last_col_letter}{max_rows}"
    vals = ws.get(rng, value_render_option='UNFORMATTED_VALUE')
    rows = [row + [""]*(ncols-len(row)) for row in vals[1:] if any(str(x).strip() != "" for x in row)]
    return pd.DataFrame(rows, columns=header)

def convert_google_serial_to_date(series):
    try:
        return pd.to_datetime(series, unit='D', origin='1899-12-30', errors='coerce').dt.date
    except Exception:
        return pd.to_datetime(series, errors='coerce').dt.date

# ===== 周/月汇总 helpers =====
def _to_datetime_safe(s):
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.to_datetime(pd.Series(s), errors="coerce")

def aggregat
