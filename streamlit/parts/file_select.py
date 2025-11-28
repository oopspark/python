import streamlit as st
import os
import pandas as pd


def file_select():
    DATA_DIR = "/home/user/GoogleDrive/data/"

    # 폴더 안의 .parquet 파일 목록 수집
    files = [
        f[:-8]   # 확장자 .parquet 제거
        for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet")
    ]

    # 스트림릿 드롭다운
    file_select = st.selectbox("데이터 선택", files)

    # 선택된 parquet 로드
    df = pd.read_parquet(os.path.join(DATA_DIR, file_select + ".parquet"))

    st.write(f"📌 선택된 데이터: `{file_select}`")
    st.write(df.head())

    return df