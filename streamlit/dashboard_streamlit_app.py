import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

st.set_page_config(page_title="Kafka 대시보드", layout="wide")
st.title("🔥 Kafka 실시간 인기 상품 분석")

def consume_messages(max_messages=50):
    consumer = KafkaConsumer(
        "popular-products-by-age",
        bootstrap_servers="localhost:9094",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000,  # 🔥 이거 꼭 넣어야 함: 2초 기다리고 종료
        enable_auto_commit=False
    )
    messages = []
    try:
        for i, msg in enumerate(consumer):
            messages.append(msg.value)
            print("📦 Kafka Message:", msg.value)
            if i >= max_messages - 1:
                break
    except Exception as e:
        print("❌ Kafka consume 에러:", e)
    finally:
        consumer.close()
    return messages

# 버튼으로 수동 트리거
if st.button("🔄 Kafka 데이터 불러오기"):
    with st.spinner("Kafka에서 데이터를 수신 중..."):
        data = consume_messages()
        if not data:
            st.warning("⚠️ Kafka에서 데이터를 받지 못했습니다.")
        else:
            df = pd.DataFrame(data)
            st.subheader("📋 수신된 Kafka 메시지")
            st.dataframe(df)

            st.subheader("📊 상품별 클릭 수 합계 (product_id 기준)")
            click_sum = df.groupby("product_id")["click_count"].sum().reset_index()
            click_sum = click_sum.sort_values(by="click_count", ascending=False)
            st.bar_chart(click_sum.set_index("product_id"))
else:
    st.info("👆 버튼을 눌러 Kafka 데이터를 불러오세요.")
