import streamlit as st
import pandas as pd
from io import BytesIO

# ================== 1. 파일에서 매핑 데이터 로드 함수 ==================
def load_mapping_from_file(filename="mapping.txt"):
    """
    지정된 파일에서 매핑 데이터를 읽어 딕셔너리로 반환합니다.
    파일 형식: '키:값'
    """
    mapping_dict = {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # 빈 줄이나 주석 (#)으로 시작하는 줄은 건너뛰기
                if not line or line.startswith("#"):
                    continue
                
                try:
                    key, value = line.split(":")
                    mapping_dict[key.strip()] = value.strip()
                except ValueError:
                    st.warning(f"매핑 파일 오류: 유효하지 않은 형식 '{line}'입니다. 이 줄은 무시됩니다.")
    except FileNotFoundError:
        st.error(f"오류: 매핑 파일 '{filename}'을 찾을 수 없습니다. 파일을 생성하거나 경로를 확인하세요.")
        return None
    return mapping_dict

# ================== 2. 매핑 데이터 로드 ==================
DAITSSO_ERP_MAP = load_mapping_from_file()

# 매핑 데이터 로드 실패 시 앱 종료
if not DAITSSO_ERP_MAP:
    st.stop()

# ================== 3. 이카운트 변환 함수 (기존과 동일) ==================
def build_ecount_sales_upload(df_daitsso: pd.DataFrame) -> pd.DataFrame:
    """
    다잇쏘 주문건 DataFrame을 이카운트 '판매입력 웹자료올리기' 양식으로 변환합니다.
    """
    df = df_daitsso.copy()
    df.fillna("", inplace=True)

    pay = pd.to_numeric(df["결제액"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    qty = pd.to_numeric(df["구매수(수량)"], errors="coerce").fillna(0)

    unit = (pay / qty.replace(0, pd.NA)).fillna(0)
    total = (unit * qty).round().fillna(0)
    vat = (total / 11).fillna(0).astype(int)
    supply = (total - vat).fillna(0).astype(int)

    # 품목코드 매핑 - 외부 파일에서 로드한 딕셔너리 사용
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
    """DataFrame을 BytesIO 객체로 변환하여 다운로드 준비"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data

# ================== 4. 스트림릿 앱 레이아웃 (기존과 동일) ==================
st.set_page_config(
    page_title="쿠팡 주문건 변환기",
    page_icon="📦"
)

st.title("📦 쿠팡 주문건 변환기")
st.markdown("### 1. 다잇쏘 주문건 추출과 ERP웹자료올리기로 변환합니다.")
st.markdown("---")
st.info("매핑 정보는 'mapping.txt' 파일에 있습니다.")

# 파일 업로더
uploaded_file = st.file_uploader(
    "쿠팡 주문건 엑셀 파일을 여기에 드래그하거나 클릭하여 업로드하세요.",
    type=['xlsx']
)

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

            ecount_excel = to_excel(ecount_df)
            col1.download_button(
                label="✅ 이카운트 업로드 파일 다운로드",
                data=ecount_excel,
                file_name="다잇쏘_쿠팡판매입력.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="ecount_download"
            )

            original_excel = to_excel(df_daitsso_original)
            col2.download_button(
                label="📁 다잇쏘 주문건 다운로드",
                data=original_excel,
                file_name="다잇쏘_주문건.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="original_download"
            )

            st.success("파일 다운로드 준비가 완료되었습니다! 👍")
            st.dataframe(ecount_df)

    except Exception as e:
        st.error(f"파일 처리 중 오류가 발생했습니다: {e}")

