import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="쿠팡 주문건 변환기", page_icon="📦")

# ------------------ 1. Google Sheets 매핑 불러오기 ------------------
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=600)
def load_mapping():
    try:
        gc = get_gspread_client()
        sheet_id = st.secrets["GSHEETS_ID"]
        worksheet_name = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")

        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)

        records = ws.get_all_records()
        mapping = {
            str(row.get("옵션ID", "")).strip(): str(
                row.get("ERP 품목코드") or ""
            ).strip()
            for row in records
            if row.get("옵션ID") and row.get("ERP 품목코드")
        }
        return mapping
    except Exception as e:
        st.error("❌ 구글 시트 로드 실패")
        st.exception(e)
        return {}

# ------------------ 2. 이카운트 변환 함수 ------------------
def build_ecount_sales_upload(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    df = df.copy().fillna("")

    pay = pd.to_numeric(df["결제액"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    qty = pd.to_numeric(df["구매수(수량)"], errors="coerce").fillna(0)

    unit = (pay / qty.replace(0, pd.NA)).fillna(0)
    total = (unit * qty).round().fillna(0)
    vat = (total / 11).fillna(0).astype(int)
    supply = (total - vat).fillna(0).astype(int)

    item_code = df["옵션ID"].map(mapping).fillna("")

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

    cols = [
        "일자","순번","거래처코드","거래처명","담당자","출하창고","거래유형",
        "통화","환율","잔액","참고",
        "품목코드","품목명","규격","수량","단가","외화금액",
        "공급가액","부가세",
        "수집처","수취인","운송장번호","적요","생산전표생성"
    ]
    return res[cols]

# ------------------ 3. Excel 변환 함수 ------------------
def to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buf.getvalue()

# ------------------ 4. Streamlit UI ------------------
st.title("📦 쿠팡 주문건 변환기")
st.markdown("Google Sheets 매핑을 사용하여 쿠팡 주문건을 ERP 업로드용으로 변환합니다.")
st.markdown("---")

# 매핑 불러오기
mapping_dict = load_mapping()
st.write("불러온 매핑 데이터 (일부):", dict(list(mapping_dict.items())[:5]))

if not mapping_dict:
    st.warning("⚠️ 매핑 데이터를 불러오지 못했습니다. 그래도 파일 업로드 기능은 사용할 수 있습니다.")

# 파일 업로더 (드래그앤드롭 지원)
uploaded = st.file_uploader("📂 쿠팡 주문건 Excel 업로드 (.xlsx)", type=["xlsx"])
if uploaded:
    try:
        df = pd.read_excel(uploaded, dtype=str)
        st.success("✅ 파일 업로드 완료!")

        # 매핑된 옵션ID만 필터링 → 매핑되지 않은 건 자동 무시
        df_daitsso = df[df["옵션ID"].isin(mapping_dict.keys())].copy()

        if df_daitsso.empty:
            st.warning("❗ 매핑된 다잇쏘 주문건이 없습니다.")
        else:
            result = build_ecount_sales_upload(df_daitsso, mapping_dict)

            c1, c2 = st.columns(2)
            c1.download_button(
                "✅ 이카운트 업로드 파일 다운로드",
                data=to_excel(result),
                file_name="다잇쏘_쿠팡판매입력.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            c2.download_button(
                "📁 다잇쏘 주문건 필터 다운로드",
                data=to_excel(df_daitsso),
                file_name="다잇쏘_주문건_필터링결과.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            st.success("🎉 변환 완료! 아래에서 결과 미리보기 확인하세요.")
            st.dataframe(result)

    except Exception as e:
        st.error("❌ 변환 중 오류 발생")
        st.exception(e)

# ------------------ 5. 매핑 데이터 추가 UI ------------------
st.markdown("---")
st.markdown("### ✏️ 새로운 매핑 추가하기")

new_option = st.text_input("옵션ID 입력")
new_code = st.text_input("ERP 품목코드 입력")

if st.button("➕ 매핑 추가"):
    if new_option and new_code:
        try:
            gc = get_gspread_client()
            sheet_id = st.secrets["GSHEETS_ID"]
            worksheet_name = st.secrets.get("GSHEETS_WORKSHEET", "Sheet1")
            sh = gc.open_by_key(sheet_id)
            ws = sh.worksheet(worksheet_name)

            # 시트에 새로운 매핑 추가
            ws.append_row([new_option, new_code])

            st.success(f"✅ 매핑 추가됨: {new_option} → {new_code}")

            # 캐시 갱신
            load_mapping.clear()
            mapping_dict = load_mapping()

            st.write("📊 최신 매핑 데이터 (일부):", dict(list(mapping_dict.items())[:5]))

        except Exception as e:
            st.error("❌ 매핑 추가 중 오류 발생")
            st.exception(e)
    else:
        st.warning("⚠️ 옵션ID와 ERP 품목코드를 모두 입력하세요.")
