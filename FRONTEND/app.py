import streamlit as st
import psycopg2
import pandas as pd
import time

st.set_page_config(page_title="ROSBD Operational Trading & Sentiment Dashboard", layout="wide")

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="rosbd_trading_db",  
        user="postgres",
        password="Naya110212"  
    )

st.title("📈 ROSBD Operational Trading & Sentiment Dashboard")
st.markdown("Sistem Terdistribusi: Pemantauan Harga Kripto Real-Time, Sinyal ML, dan Berita Makro Geopolitik.")
st.write("---")

col_market, col_news = st.columns([3, 2])

with col_market:
    st.subheader("📊 Pemantauan Harga & Insight Pasar")
    
    try:
        conn = get_db_connection()
        query_signals = """
            SELECT DISTINCT ON (token) token, price, probability, signal_status, take_profit, stop_loss, created_at 
            FROM v_crypto_signals 
            ORDER BY token, created_at DESC;
        """
        df_signals = pd.read_sql(query_signals, conn)
        conn.close()
        
        if not df_signals.empty:
            for idx, row in df_signals.iterrows():
                with st.container():
                    c1, c2 = st.columns([2, 1])
                    with c1:
                        st.metric(
                            label=f"Token: {row['token']} (Waktu Sinkronisasi: {row['created_at'].strftime('%H:%M:%S')})",
                            value=f"${float(row['price']):,.4f}"
                        )
                    with c2:
                        status = row['signal_status']
                        prob = float(row['probability'])
                        
                        if prob > 0.0 and "LONG" in status:
                            st.success(f"🔥 Sinyal Model: {status} ({prob}%)")
                            st.caption(f"TP: ${float(row['take_profit']):,.2f} | SL: ${float(row['stop_loss']):,.2f}")
                        else:
                            st.info("⚪ ML Status: Idle")
                            st.caption("Menunggu kalkulasi stream dari Apache Spark...")
                st.write("-" * 30)
        else:
            st.info("💡 Menunggu aliran data masuk... Jalankan run_price_pipeline.py di terminal backend Anda.")
            
    except Exception as e:
        st.error(f"Gagal mengambil data dari database: {str(e)}")

with col_news:
    st.subheader("📰 Berita Global & Sentimen Makro")
    
    try:
        conn = get_db_connection()
        query_news = "SELECT source_name, title, description, url, published_at FROM v_market_news ORDER BY created_at DESC LIMIT 10;"
        df_news = pd.read_sql(query_news, conn)
        conn.close()
        
        if not df_news.empty:
            for idx, row in df_news.iterrows():
                with st.expander(f"{idx+1}. [{row['source_name']}] - {row['title'][:60]}..."):
                    st.markdown(f"**Waktu Publikasi:** `{row['published_at']}`")
                    st.write(row['description'])
                    st.markdown(f"[Baca Berita Selengkapnya]({row['url']})")
        else:
            st.info("💡 Menunggu data berita masuk ke database...")
            
    except Exception as e:
        st.error(f"Gagal mengambil data berita: {str(e)}")

time.sleep(10)
st.rerun()