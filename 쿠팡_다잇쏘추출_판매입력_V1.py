import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== 1. Google Sheets에서 매핑 정보 불러오기 ==================
def load_mapping_from_gsheet(sheet_url: str) -> dict:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        st.secrets["gcp_service_account"], scope
    )
    gc = gspread.authorize(credentials)
    
    try:
        sheet = gc.open_by_url(sheet_url)
        worksheet = sheet.get_worksheet(0)  # 첫 번째 시트 사용
        data = worksheet.get_all_records()
    except Exception as e:
        st.error(f"❌ Google Sheets 로드 실패: {e}")
        return None

    mapping_dict = {}
    for row in data:
        option_id = str(row.get("옵션ID", "")).strip()
        item_code = str(row.get("ERP코드", "")).strip()
        if option_id and item_code:
            mapping_dict[option_id] = item_code
    return mapping_dict

# ================== 2. 이카운트 변환 함수 ==================
def build_ecount_sales_upload(df_daitsso: pd.DataFrame, mapping_dict: dict) -> pd.DataFrame:
    df = df_daitsso.copy()
    df.fillna("", inplace=True)

    pay = pd.to_numeric(df["결제액"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    qty = pd.to_numeric(df["구매수(수량)"], errors="coerce").fillna(0)

    unit = (pay / qty.replace(0, pd.NA)).fillna(0)
    total = (unit * qty).round().fillna(0)
    vat = (total / 11).fillna(0).astype(int)
    supply = (total - vat).fillna(0).astype(int)

    item_code = df["옵션ID"].map(mapping_dict).fillna("")

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

# ================== 3. Streamlit UI ==================
st.set_page_config(
    page_title="쿠팡 주문건 변환기",
    page_icon="📦"
)

st.title("📦 쿠팡 주문건 변환기")
st.markdown("### 1. 다잇쏘 주문건 추출과 ERP웹자료올리기로 변환합니다.")
st.markdown("---")
st.info("✅ Google Sheets에서 옵션ID → ERP코드 매핑 정보를 불러옵니다.")

# 1. 매핑용 구글시트 입력 받기
sheet_url = st.text_input("📄 매핑용 Google Sheets URL을 입력하세요:", placeholder="https://docs.google.com/spreadsheets/d/...")
mapping_dict = {}

if sheet_url:
    mapping_dict = load_mapping_from_gsheet(sheet_url)
    if mapping_dict:
        st.success(f"🟢 매핑 정보 {len(mapping_dict)}건 로드 완료")

# 2. 쿠팡 엑셀 업로드
uploaded_file = st.file_uploader(
    "📂 쿠팡 주문건 엑셀 파일을 업로드하세요",
    type=['xlsx']
)

if uploaded_file and mapping_dict:
    try:
        df = pd.read_excel(uploaded_file, dtype=str)
        st.success("파일 업로드 완료! 🎉")

        df_daitsso_original = df[df.get("옵션ID", "").isin(mapping_dict.keys())].copy()

        if df_daitsso_original.empty:
            st.warning("업로드된 파일에 매핑된 다잇쏘 관련 주문건이 없습니다.")
        else:
            st.info("🛠️ 이카운트 업로드 파일 생성 중...")
            ecount_df = build_ecount_sales_upload(df_daitsso_original, mapping_dict)

            col1, col2 = st.columns(2)

            col1.download_button(
                label="✅ 이카운트 업로드 파일 다운로드",
                data=to_excel(ecount_df),
                file_name="다잇쏘_쿠팡판매입력.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="ecount_download"
            )

            col2.download_button(
                label="📁 다잇쏘 주문건 다운로드",
                data=to_excel(df_daitsso_original),
                file_name="다잇쏘_주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="original_download"
            )

            st.success("🎉 파일 다운로드 준비 완료!")
            st.dataframe(ecount_df)

    except Exception as e:
        st.error(f"❌ 파일 처리 중 오류 발생: {e}")

elif uploaded_file and not mapping_dict:
    st.warning("⚠️ 매핑 정보를 먼저 입력하고 로드해주세요.")
