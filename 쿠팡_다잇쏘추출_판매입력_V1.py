import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials
import json

st.set_page_config(page_title="쿠팡 주문건 변환기", page_icon="📦")

# ------------------ Google Sheets 연결 ------------------
def _get_creds():
    # secrets에 JSON 전체를 문자열로 넣었든, 키-값(table)로 넣었든 모두 지원
    svc = st.secrets["gcp_service_account"]
    info = json.loads(svc) if isinstance(svc, str) else dict(svc)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

@st.cache_resource
def _get_client():
    return gspread.authorize(_get_creds())

@st.cache_data(ttl=600)
def load_mapping_from_sheet(sheet_id: str, worksheet_name: str) -> dict:
    gc = _get_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    rows = ws.get_all_records()
    mapping = {}
    for r in rows:
        k = str(r.get("옵션ID", "")).strip()
        v = str(r.get("ERP코드") or r.get("이플렉스코드") or r.get("코드") or "").strip()
        if k and v:
            mapping[k] = v
    return mapping

# ------------------ 변환 로직 ------------------
def build_ecount_sales_upload(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    df = df.copy().fillna("")
    pay = pd.to_numeric(df["결제액"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    qty = pd.to_numeric(df["구매수(수량)"], errors="coerce").fillna(0)
    unit = (pay / qty.replace(0, pd.NA)).fillna(0)
    total = (unit * qty).round().fillna(0)
    vat = (total / 11).fillna(0).astype(int)
    supply = (total - vat).fillna(0).astype(int)
    item_code = df["옵션ID"].map(mapping).fillna("")

    out = pd.DataFrame({
        "일자": pd.to_datetime(df["주문시 출고예정일"], errors="coerce").dt.strftime("%Y%m%d").fillna(""),
        "순번": "", "거래처코드": "", "거래처명": "쿠팡 주식회사", "담당자": "",
        "출하창고": "103", "거래유형": "", "통화": "", "환율": "", "잔액": "", "참고": "",
        "품목코드": item_code, "품목명": "", "규격": "", "수량": qty, "단가": unit,
        "외화금액": "", "공급가액": supply, "부가세": vat,
        "수집처": "쿠팡", "수취인": df["수취인이름"], "운송장번호": "", "적요": "", "생산전표생성": "Y",
    })
    cols = ["일자","순번","거래처코드","거래처명","담당자","출하창고","거래유형","통화","환율","잔액","참고",
            "품목코드","품목명","규격","수량","단가","외화금액","공급가액","부가세",
            "수집처","수취인","운송장번호","적요","생산전표생성"]
    return out[cols]

def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ UI ------------------
st.title("📦 쿠팡 주문건 변환기")
st.caption("Google Sheets의 매핑(옵션ID→ERP코드)을 사용합니다.")

SHEET_ID = st.secrets.get("GSHEETS_ID", "여기에_시트ID_없으면_하드코딩")
SHEET_WS  = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")

try:
    mapping = load_mapping_from_sheet(SHEET_ID, SHEET_WS)
except Exception as e:
    st.error(f"구글 시트 접근 실패: {e}")
    st.stop()

st.success(f"매핑 로드 완료 (건수: {len(mapping)}).")

uploaded = st.file_uploader("쿠팡 주문건 엑셀 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, dtype=str)
        need = {"옵션ID","결제액","구매수(수량)","주문시 출고예정일","수취인이름"}
        missing = list(need - set(df.columns))
        if missing:
            st.error(f"필수 컬럼이 없습니다: {missing}")
            st.stop()

        df_daitsso = df[df["옵션ID"].isin(mapping.keys())].copy()
        if df_daitsso.empty:
            st.warning("다잇쏘 관련 주문건이 없습니다.")
        else:
            out = build_ecount_sales_upload(df_daitsso, mapping)
            c1, c2 = st.columns(2)
            c1.download_button("✅ 이카운트 업로드 다운로드", data=to_excel(out),
                               file_name="다잇쏘_쿠팡판매입력.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            c2.download_button("📁 다잇쏘 주문건(필터) 다운로드", data=to_excel(df_daitsso),
                               file_name="다잇쏘_주문건_필터링결과.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.dataframe(out)
    except Exception as e:
        st.error(f"처리 중 오류: {e}")
