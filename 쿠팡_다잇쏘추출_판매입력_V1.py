import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

# ================== 1. 구글 시트에서 매핑 데이터 로드 ==================
def load_mapping_from_google_sheet(sheet_url, worksheet_name):
    try:
        # 구글 서비스 계정 인증
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_file(
            "/etc/secrets/client_secret.json",  # ✅ Render용 절대 경로
            scopes=scope
        )
        gc = gspread.authorize(credentials)

        spreadsheet = gc.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)

        data = worksheet.get_all_records()
        mapping_dict = {
            str(row['옵션ID']).strip(): str(row['이플렉스코드']).strip()
            for row in data if row['옵션ID'] and row['이플렉스코드']
        }
        return mapping_dict

    except Exception as e:
        st.error(f"❌ 구글 시트에서 매핑 데이터를 불러오는 데 실패했습니다: {e}")
        return {}

# ================== 2. 이카운트 변환 함수 ==================
def build_ecount_sales_upload(df_daitsso: pd.DataFrame, mapping_dict) -> pd.DataFrame:
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

    return res[[  # 열 순서
        "일자", "순번", "거래처코드", "거래처명", "담당자", "출하창고", "거래유형",
        "통화", "환율", "잔액", "참고",
        "품목코드", "품목명", "규격", "수량", "단가", "외화금액",
        "공급가액", "부가세", "수집처", "수취인", "운송장번호", "적요", "생산전표생성"
    ]]

# ================== 3. 엑셀 다운로드용 함수 ==================
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# ================== 4. Streamlit 앱 시작 ==================
st.set_page_config(page_title="쿠팡 주문건 변환기", page_icon="📦")
st.title("📦 쿠팡 주문건 변환기")
st.markdown("### 1. 다잇쏘 주문건 추출과 ERP웹자료올리기로 변환합니다.")
st.markdown("---")

# 구글 시트 URL과 시트 이름
SHEET_URL = "https://docs.google.com/spreadsheets/d/1o3ZW9tAnwmec8NjHHhHouofeBVgqoTA8QlMtzLJPaxM/edit"
SHEET_NAME = "Sheet1"

mapping_dict = load_mapping_from_google_sheet(SHEET_URL, SHEET_NAME)

if not mapping_dict:
    st.stop()

uploaded_file = st.file_uploader("쿠팡 주문건 엑셀 파일을 업로드하세요", type=['xlsx'])

if uploaded_file:
    try:
        df = pd.read_excel(uploaded_file, dtype=str)
        st.success("✅ 파일 업로드 완료!")

        df_daitsso = df[df["옵션ID"].isin(mapping_dict.keys())].copy()

        if df_daitsso.empty:
            st.warning("❗ 다잇쏘 관련 주문건이 없습니다.")
        else:
            result_df = build_ecount_sales_upload(df_daitsso, mapping_dict)

            col1, col2 = st.columns(2)

            col1.download_button(
                label="✅ 이카운트 업로드 파일 다운로드",
                data=to_excel(result_df),
                file_name="다잇쏘_쿠팡판매입력.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            col2.download_button(
                label="📁 다잇쏘 주문건 다운로드",
                data=to_excel(df_daitsso),
                file_name="다잇쏘_주문건_필터링결과.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.success("🎉 이카운트 변환 완료! 아래 표는 미리보기입니다.")
            st.dataframe(result_df)

    except Exception as e:
        st.error(f"❌ 변환 중 오류 발생: {e}")
