import os
import time
import subprocess
from pathlib import Path

def run_daemon():
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "DATA"
    data_dir.mkdir(exist_ok=True)
    
    trigger_file = data_dir / "trigger_train.txt"
    success_file = data_dir / "train_success.txt"
    failed_file = data_dir / "train_failed.txt"
    
    print("========================================================")
    print("[*] AIRFLOW-SPARK TRIGGER DAEMON BERJALAN")
    print("========================================================")
    print(f"[*] Memantau direktori: {data_dir}")
    print("[*] Menunggu trigger dari Airflow DAG...")
    
    while True:
        if trigger_file.exists():
            print("\n[!] Trigger dari Airflow terdeteksi! Memulai proses Distributed Training...")
            
            if success_file.exists(): success_file.unlink()
            if failed_file.exists(): failed_file.unlink()
            
            try:
                cmd = [
                    "spark-submit", 
                    "--master", "spark://192.168.1.10:7077", 
                    "src/models/train_model.py"
                ]
                
                print(f"[*] Mengeksekusi: {' '.join(cmd)}")
                result = subprocess.run(cmd, cwd=str(project_root), shell=True)
                
                if result.returncode == 0:
                    print("\n[+] Training berhasil diselesaikan!")
                    success_file.touch()
                else:
                    print(f"\n[-] Training gagal dengan kode {result.returncode}")
                    failed_file.touch()
                    
            except Exception as e:
                print(f"[-] Terjadi error pada sistem: {e}")
                failed_file.touch()
            finally:
                # Hapus trigger untuk memberitahu Airflow bahwa eksekusi selesai
                trigger_file.unlink()
                print("[*] Menunggu trigger berikutnya...")
                
        time.sleep(3)

if __name__ == "__main__":
    run_daemon()
