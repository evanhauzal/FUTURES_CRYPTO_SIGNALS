import pandas as pd
import numpy as np
import joblib # Tambahkan ini untuk menyimpan scaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import StandardScaler

# 1. Load Data
df = pd.read_csv("data_training_pepe.csv")
data = df[['arus_kas_bandar_usd', 'target_harga_t5']].values

# 2. Normalisasi menggunakan StandardScaler
scaler_input = StandardScaler()
scaler_target = StandardScaler()

# Kita normalisasi secara terpisah agar mudah di-inverse nanti
data_input_scaled = scaler_input.fit_transform(data[:, 0].reshape(-1, 1))
data_target_scaled = scaler_target.fit_transform(data[:, 1].reshape(-1, 1))

# Gabungkan kembali untuk sequence
data_scaled = np.hstack((data_input_scaled, data_target_scaled))

# Simpan scaler agar bisa dipakai di dashboard untuk inverse_transform
joblib.dump(scaler_target, "scaler_target.pkl")

# 3. Buat Sequence
def create_sequences(data, seq_length=10):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i+seq_length, 0]) # Input: Arus kas
        y.append(data[i+seq_length, 1])   # Target: Harga T+5
    return np.array(X), np.array(y)

X, y = create_sequences(data_scaled)
X = X.reshape(X.shape[0], X.shape[1], 1)

# 4. Arsitektur Model
model = Sequential([
    LSTM(64, activation='tanh', input_shape=(10, 1), return_sequences=True),
    LSTM(32, activation='tanh'),
    Dense(1)
])

model.compile(optimizer='adam', loss='mse')
model.fit(X, y, epochs=50, batch_size=16, verbose=1)

# 5. Simpan Model
model.save("pepe_model_t5.h5")
print("✅ Model dan scaler berhasil disimpan.")