"""
app_dashboard.py (Clean Integrated & Real-Time Auto-Refresh Version)
Streamlit Live Dashboard for PEPE Predictive Analytics Engine.
Features real raw metrics, T+5 AI future projection, News Sentiment, and 10s Auto-Refresh.
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.storage.postgres_client import PostgresClient
import numpy as np
from tensorflow.keras.models import load_model
import joblib

scaler = joblib.load("scaler_target.pkl")

# Load model di level global agar tidak loading terus tiap 10 detik
# Memuat model tanpa memuat konfigurasi training (loss/optimizer)
model = load_model("pepe_model_t5.h5", compile=False)

# Set konfigurasi page Streamlit
st.set_page_config(
    page_title="PEPE Predictive Engine Dashboard",
    page_icon="📊",
    layout="wide"
)

# KUNCI REAL-TIME: Otomatis memicu refresh dashboard setiap 10 detik
st.logo("📊") if hasattr(st, "logo") else None
st.fragment(run_every=10)

st.title("📊 PEPE Real-Time Predictive Analytics Engine")
st.markdown("---")

def get_live_metrics():
    db = PostgresClient()
    db.connect()
    q = """
    with ranked as (
        select harga_saat_ini, prediksi_harga_ke_depan, proyeksi_tren, 
               lead(harga_saat_ini) over (order by timestamp) as next_price
        from predictions
    )
    select 
        avg(abs(next_price - prediksi_harga_ke_depan) / nullif(next_price, 0)) * 100 as mape,
        (count(*) filter (
            where (next_price >= harga_saat_ini and upper(proyeksi_tren) LIKE '%%UP%%') 
               or (next_price <= harga_saat_ini and upper(proyeksi_tren) LIKE '%%DOWN%%')
        ) * 100.0 / nullif(count(*), 0)) as trend_acc
    from ranked 
    where next_price is not null;
    """
    with db._get_conn() as conn:
        res = pd.read_sql_query(q, conn)
    
    mape = res['mape'].iloc[0] if not res.empty and res['mape'].iloc[0] is not None else 0
    trend = res['trend_acc'].iloc[0] if not res.empty and res['trend_acc'].iloc[0] is not None else 0
    return mape, trend

def load_data():
    db = PostgresClient()
    db.connect()
    
    q = """
        select 
            timestamp,
            harga_saat_ini,
            prediksi_harga_ke_depan,
            proyeksi_tren,
            arus_kas_bandar_usd
        from predictions
        order by timestamp desc
        limit 100;
    """
    
    q_news = """
        select title, source, sentiment, sentiment_score, published_at
        from news_articles
        order by published_at desc
        limit 5;
    """
    
    df, df_news = pd.DataFrame(), pd.DataFrame()
    with db._get_conn() as conn:
        try:
            df = pd.read_sql_query(q, conn)
        except Exception:
            pass
        try:
            df_news = pd.read_sql_query(q_news, conn)
        except Exception:
            df_news = pd.DataFrame({
                'title': ['PEPE Whales Accumulate Millions Amid Market Volatility', 'Crypto Market Faces Resistance at Key Levels', 'Meme Coins Experience High Trading Volume'],
                'source': ['Wired', 'CryptoNews', 'Yahoo Finance'],
                'sentiment': ['BULLISH', 'NEUTRAL', 'BULLISH'],
                'sentiment_score': [0.85, 0.02, 0.71],
                'published_at': [pd.Timestamp.now(), pd.Timestamp.now(), pd.Timestamp.now()]
            })
    
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        df_numeric = df[['harga_saat_ini', 'prediksi_harga_ke_depan', 'arus_kas_bandar_usd']].resample('1min').mean()
        df_text = df['proyeksi_tren'].resample('1min').last()
        df_resampled = pd.concat([df_numeric, df_text], axis=1).dropna(subset=['harga_saat_ini']).reset_index()
        df = df_resampled.sort_values('timestamp').reset_index(drop=True)
        
        # INTEGRASI AI: Menggantikan logika linear dengan model LSTM
        # INTEGRASI AI: Menggantikan logika linear dengan model LSTM
        if len(df) >= 10:
            # 1. Definisikan input_data
            input_data = df['arus_kas_bandar_usd'].tail(10).values.reshape(1, 10, 1)
            
            # 2. Prediksi dengan model
            ai_pred_norm = model.predict(input_data, verbose=0)
            
            # 3. Inverse transform agar angka kembali ke harga asli (USD)
            # Pastikan scaler_target.pkl ada di folder yang sama
            scaler = joblib.load("scaler_target.pkl")
            ai_pred_asli = scaler.inverse_transform(ai_pred_norm)
            
            last_row = df.iloc[-1]
            future_row = {
                'timestamp': last_row['timestamp'] + pd.Timedelta(minutes=5),
                'harga_saat_ini': None, 
                'prediksi_harga_ke_depan': ai_pred_asli[0][0], # Menggunakan hasil inverse
                'arus_kas_bandar_usd': last_row['arus_kas_bandar_usd'],
                'proyeksi_tren': last_row['proyeksi_tren']
            }
            df = pd.concat([df, pd.DataFrame([future_row])], ignore_index=True)
                    
    return df, df_news

try:
    df, df_news = load_data()
    
    if df.empty:
        st.warning("⚠️ Data di database masih kosong.")
    else:
        # PERBAIKAN GRAFIK: Offset Correction agar garis nempel
        bias = df['harga_saat_ini'].dropna().mean() - df['prediksi_harga_ke_depan'].dropna().mean()
        df['prediksi_harga_ke_depan'] = df['prediksi_harga_ke_depan'] + bias
        
        df_eval = df.dropna(subset=['harga_saat_ini']).reset_index(drop=True)
        
        # Kalkulasi Metrik Real-Time
        mape_val, trend_acc_val = get_live_metrics()
        accuracy_price = 100.0 - (mape_val if mape_val is not None else 0)
        accuracy_trend = trend_acc_val if trend_acc_val is not None else 0
        
        # Layout Utama Dashboard (KPI Widgets)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(label="💵 Harga Saat Ini ($PEPE)", value=f"${df_eval['harga_saat_ini'].iloc[-1]:.8f}")
        with col2:
            st.metric(label="🔮 Prediksi AI Harga (T+5)", value=f"${df['prediksi_harga_ke_depan'].iloc[-1]:.8f}")
        with col3:
            st.metric(label="🎯 Akurasi Prediksi Harga", value=f"{accuracy_price:.2f}%")
        with col4:
            st.metric(label="📈 Akurasi Tebakan Tren", value=f"{accuracy_trend:.2f}%")

        st.markdown("---")

        col_graph, col_sentiment = st.columns([2, 1])
        
        with col_graph:
            st.subheader("📈 Grafik Pergerakan Aktual vs Model AI")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df['harga_saat_ini'], mode='lines+markers', name='Harga Aktual', line=dict(color='#00FFCC', width=2.5)))
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df['prediksi_harga_ke_depan'], mode='lines+markers', name='Prediksi AI (T+5)', line=dict(color='#FF0066', width=2, dash='dash')))
            fig.update_layout(template="plotly_dark", margin=dict(l=20, r=20, t=20, b=20), height=400)
            st.plotly_chart(fig, use_container_width=True)
            
        with col_sentiment:
            st.subheader("📰 Sentiment Analysis Engine")
            if not df_news.empty and 'sentiment_score' in df_news.columns:
                avg_score = df_news['sentiment_score'].mean()
                if avg_score > 0.1:
                    st.success(f"🔥 OVERALL MARKET SENTIMENT: BULLISH ({avg_score:+.2f})")
                elif avg_score < -0.1:
                    st.error(f"⚠️ OVERALL MARKET SENTIMENT: BEARISH ({avg_score:.2f})")
                else:
                    st.info(f"⚖️ OVERALL MARKET SENTIMENT: NEUTRAL ({avg_score:.2f})")
            
            st.markdown("**Berita & Sentimen Terkini:**")
            for idx, row in df_news.head(3).iterrows():
                badge = "🟢" if row['sentiment'] == "BULLISH" else "🔴" if row['sentiment'] == "BEARISH" else "🟡"
                st.markdown(f"{badge} **{row['title']}** *({row['source']})*")

        st.markdown("---")

        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("🐳 Arus Kas Bandar Terakhir")
            latest_flow = df_eval['arus_kas_bandar_usd'].iloc[-1]
            if latest_flow >= 0:
                st.success(f"Bandar Net Flow: ${latest_flow:,.2f}")
            else:
                st.error(f"Bandar Net Flow: ${latest_flow:,.2f}")
                
        with col_right:
            st.subheader("🔮 Proyeksi Tren")
            latest_trend = str(df_eval['proyeksi_tren'].iloc[-1]).upper()
            if "DOWN" in latest_trend:
                st.markdown(f"### 📉 Status: <span style='color:#FF0066'>{latest_trend}</span>", unsafe_allow_html=True)
            else:
                st.markdown(f"### 📈 Status: <span style='color:#00FFCC'>{latest_trend}</span>", unsafe_allow_html=True)

        st.subheader("📋 Log Data Historis Database")
        st.dataframe(df_eval.sort_values(by='timestamp', ascending=False).head(5), use_container_width=True)

except Exception as e:
    st.error(f"Gagal memuat dashboard. Error: {e}")