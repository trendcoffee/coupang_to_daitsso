import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

# ================== 1. Google Sheets에서 매핑 데이터 불러오기 ==================
def load_mapping_from_gsheet(sheet_url: str, worksheet_name: str = "Sheet1") -> dict:
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    gc = gspread.authorize(credentials)

    try:
        sh = gc.open_by_url(sheet_url)
        worksheet = sh.worksheet(worksheet_name)
        records = worksheet.get_all_records()

        mapping_dict = {}
        for row in records:
            key = str(row.get("옵션ID", "")).strip()
            value = str(row.get("이카운트품목코드", "")).strip()
            if key and value:
                mapping_dict[key] = value
        return mapping_dict

    except Exception as e:
        st.error(f"❌ 구글 시트에서 매핑 데이터를 불러오는 데 실패했습니다: {e}")
        return {}

# ================== 2. 매핑 데이터 로드 ==================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1o3ZW9tAnwmec8NjHHhHouofeBVgqoTA8QlMtzLJPaxM/edit"
DAITSSO_ERP_MAP = load_mapping_from_gsheet(SHEET_URL)

if not DAITSSO_ERP_MAP:
    st.stop()

# ================== 3. 이카운트 변환 함수 ==================
def build_ecount_sales_upload(df_daitsso: pd.DataFrame) -> pd.DataFrame:
    df = df_daitsso.copy()
    df.fillna("", inplace=True)

    pay = pd.to_numeric(df["결제액"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    qty = pd.to_numeric(df["구매수(수량)"], errors="coerce").fillna(0)

    unit = (pay / qty.replace(0, pd.NA)).fillna(0)
    total = (unit * qty).round().fillna(0)
    vat = (total / 11).fillna(0).astype(int)
    supply = (total - vat).fillna(0).astype(int)

    item_code = df["옵션ID"].map(DAITSSO_ERP_MAP).fillna("")

    res = pd.DataFrame({
        "일자": pd.to_datetime(df["주문시 출고예정일"], errors="coerce").dt.strftime("%Y%m%d").fillna(""),
        "순번": "",
        "거래처코드": "",
        "거래처명": "쿠팡 주식회사",
        "담당자": "",
        "출하창고": "103",
        "거래유형": "",
        "통화": "",
        "환율": "",
        "잔액": "",
        "참고": "",
        "품목코드": item_code,
        "품목명": "",
        "규격": "",
        "수량": qty,
        "단가": unit,
        "외화금액": "",
        "공급가액": supply,
        "부가세": vat,
        "수집처": "쿠팡",
        "수취인": df["수취인이름"],
        "운송장번호": "",
        "적요": "",
        "생산전표생성": "Y",
    })

    columns_order = [
        "일자", "순번", "거래처코드", "거래처명", "담당자", "출하창고", "거래유형",
        "통화", "환율", "잔액", "참고",
        "품목코드", "품목명", "규격", "수량", "단가", "외화금액",
        "공급가액", "부가세",
        "수집처", "수취인", "운송장번호", "적요", "생산전표생성"
    ]

    if not res.empty and res["품목코드"].iloc[-1] == "":
        res = res.iloc[:-1].copy()

    return res[columns_order]

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# ================== 4. Streamlit UI ==================
st.set_page_config(page_title="쿠팡 주문건 변환기", page_icon="📦")
st.title("📦 쿠팡 주문건 변환기")
st.markdown("### 1. 다잇쏘 주문건 추출과 ERP웹자료올리기로 변환합니다.")
st.markdown("---")
st.info("🗂 매핑 정보는 Google Sheets에서 자동으로 불러옵니다.")

uploaded_file = st.file_uploader("쿠팡 주문건 엑셀 파일을 업로드하세요", type=['xlsx'])

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file, dtype=str)
        st.success("파일 업로드 완료! 🎉")

        df_daitsso_original = df[df.get("옵션ID", "").isin(DAITSSO_ERP_MAP.keys())].copy()

        if df_daitsso_original.empty:
            st.warning("업로드된 파일에 다잇쏘 관련 주문건이 없습니다.")
        else:
            st.info("이카운트 업로드 파일을 생성 중입니다...")
            ecount_df = build_ecount_sales_upload(df_daitsso_original)

            col1, col2 = st.columns(2)

            col1.download_button(
                label="✅ 이카운트 업로드 파일 다운로드",
                data=to_excel(ecount_df),
                file_name="다잇쏘_쿠팡판매입력.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            col2.download_button(
                label="📁 다잇쏘 주문건 다운로드",
                data=to_excel(df_daitsso_original),
                file_name="다잇쏘_주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.success("파일 다운로드 준비가 완료되었습니다! 👍")
            st.dataframe(ecount_df)

    except Exception as e:
        st.error(f"파일 처리 중 오류가 발생했습니다: {e}")
