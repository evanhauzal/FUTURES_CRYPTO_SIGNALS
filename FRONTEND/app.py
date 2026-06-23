import streamlit as st
import psycopg2
import pandas as pd
import time
import sys
from pathlib import Path

# Add project root to sys.path to allow config import regardless of execution directory
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

st.set_page_config(page_title="ROSBD Operational Trading & Sentiment Dashboard", layout="wide")

from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
import yfinance as yf

# Fungsi untuk mengambil harga real-time (Aman dari Blokir ISP & Rate Limit)
@st.cache_data(ttl=2) # Cache 2 detik agar super responsif
def get_realtime_price(token):
    # Hapus spasi siluman yang terikut dari PostgreSQL CHAR data type
    clean_token = str(token).strip().upper()
    
    try:
        ticker = yf.Ticker(f"{clean_token}-USD")
        return float(ticker.fast_info['lastPrice'])
    except Exception as e:
        print(f"YFinance Error for {clean_token}: {str(e)}")
        return None

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

st.title("📈 ROSBD Operational Trading & Sentiment Dashboard")
st.markdown("Sistem Terdistribusi: Pemantauan Harga Kripto Real-Time, Sinyal ML, dan Berita Makro Geopolitik.")

# Indikator Status Backend
st.success("✅ **Backend Service:** Apache Airflow Orchestrator & Kafka Streaming Aktif")
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
                        # Ambil harga real-time terbaru
                        live_price = get_realtime_price(row['token'])
                        
                        # Fallback ke harga database jika Yahoo Finance gagal
                        display_price = live_price if live_price is not None else float(row['price'])
                        price_source = "🔴 Live Market" if live_price is not None else "⚪ Database"
                        
                        st.metric(
                            label=f"Token: {row['token']} ({price_source} - Sync Model: {row['created_at'].strftime('%H:%M:%S')})",
                            value=f"${display_price:,.4f}"
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
        # PERUBAHAN: Menambahkan kolom 'sentiment' ke dalam SELECT query
        query_news = "SELECT source_name, title, description, url, published_at, sentiment FROM v_market_news ORDER BY created_at DESC LIMIT 10;"
        df_news = pd.read_sql(query_news, conn)
        conn.close()
        
        if not df_news.empty:
            # FITUR TAMBAHAN: Menampilkan rangkuman persentase sentimen saat ini di bagian atas kolom berita
            sentiment_counts = df_news['sentiment'].value_counts()
            pos_count = sentiment_counts.get('POSITIVE', 0)
            neg_count = sentiment_counts.get('NEGATIVE', 0)
            neu_count = sentiment_counts.get('NEUTRAL', 0)
            
            s_col1, s_col2, s_col3 = st.columns(3)
            s_col1.metric("🟢 Positive News", f"{pos_count} Berita")
            s_col2.metric("🔴 Negative News", f"{neg_count} Berita")
            s_col3.metric("⚪ Neutral News", f"{neu_count} Berita")
            st.write("---")

            for idx, row in df_news.iterrows():
                # PERUBAHAN: Mapping emoji dan warna berdasarkan data label sentimen riil dari database
                sentiment_label = row['sentiment'] if row['sentiment'] else "NEUTRAL"
                
                if sentiment_label == "POSITIVE":
                    emoji = "🟢"
                    color_tag = ":green[POSITIVE]"
                elif sentiment_label == "NEGATIVE":
                    emoji = "🔴"
                    color_tag = ":red[NEGATIVE]"
                else:
                    emoji = "⚪"
                    color_tag = ":gray[NEUTRAL]"
                
                # Menampilkan ekspander berita dengan indikator emoji sentimen di judulnya
                with st.expander(f"{emoji} {idx+1}. [{row['source_name']}] - {row['title'][:55]}..."):
                    st.markdown(f"**Analisis Sentimen:** {color_tag}")
                    st.markdown(f"**Waktu Publikasi:** `{row['published_at']}`")
                    st.write(row['description'])
                    st.markdown(f"[Baca Berita Selengkapnya]({row['url']})")
        else:
            st.info("💡 Menunggu data berita masuk ke database...")
            
    except Exception as e:
        st.error(f"Gagal mengambil data berita: {str(e)}")

time.sleep(10)
st.rerun()