import os
import time
import subprocess
from pathlib import Path

def run_daemon():
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "DATA"
    data_dir.mkdir(exist_ok=True)
    
    trigger_file = data_dir / "trigger_train.txt"
    trigger_scan = data_dir / "trigger_scan.txt"
    success_file = data_dir / "train_success.txt"
    failed_file = data_dir / "train_failed.txt"
    
    print("========================================================")
    print("[*] AIRFLOW-SPARK TRIGGER DAEMON BERJALAN")
    print("========================================================")
    print(f"[*] Memantau direktori: {data_dir}")
    print("[*] Menunggu trigger dari Airflow DAG...")
    
    while True:
        # --- BLOK TRIGGER TRAINING (SPARK) ---
        if trigger_file.exists():
            print("\n[!] Trigger dari Airflow terdeteksi! Memulai proses Distributed Training...")
            
            if success_file.exists(): success_file.unlink()
            if failed_file.exists(): failed_file.unlink()
            
            try:
                # Gunakan spark-submit ke cluster Master
                python311_driver = r"C:\Users\asus Pc\AppData\Local\Programs\Python\Python311\python.exe"
                spark_submit = r"C:\Users\asus Pc\AppData\Local\Programs\Python\Python311\Lib\site-packages\pyspark\bin\spark-submit.cmd"
                
                env = os.environ.copy()
                # Diubah ke 'python' agar laptop teman Anda (worker) bisa mencarinya di PATH mereka sendiri
                # karena username laptopnya berbeda ('asus' vs 'asus Pc')
                env["PYSPARK_PYTHON"] = "python" 
                env["PYSPARK_DRIVER_PYTHON"] = python311_driver
                
                cmd = [
                    spark_submit, 
                    "--master", "spark://192.168.1.10:7077", 
                    "--py-files", "dist/project.zip",
                    "src/models/train_model.py"
                ]
                
                print(f"[*] Mengeksekusi: {' '.join(cmd)}")
                result = subprocess.run(cmd, cwd=str(project_root), env=env, shell=True)
                
                spark_flag = data_dir / "spark_success.flag"
                if spark_flag.exists():
                    print("\n[+] Training berhasil diselesaikan secara penuh (Flag terdeteksi)!")
                    success_file.touch()
                    spark_flag.unlink()
                else:
                    print(f"\n[-] Training gagal atau terputus (Kode Exit: {result.returncode})")
                    failed_file.touch()
                    
            except BaseException as e:
                print(f"[-] Terjadi error atau interupsi pada sistem: {e}")
                failed_file.touch()
            finally:
                time.sleep(2)
                if trigger_file.exists():
                    trigger_file.unlink()
                print("\n[*] Menunggu trigger berikutnya...")
                
        # --- BLOK TRIGGER SCAN SIGNALS (SUPABASE IPv6) ---
        elif trigger_scan.exists():
            print("\n[!] Trigger Airflow: Menjalankan Scanner Sinyal (Host Mode untuk IPv6)...")
            
            if success_file.exists(): success_file.unlink()
            if failed_file.exists(): failed_file.unlink()
            
            try:
                python311 = r"C:\Users\asus Pc\AppData\Local\Programs\Python\Python311\python.exe"
                cmd = [python311, "-m", "src.signals.generator"]
                print(f"[*] Mengeksekusi: {' '.join(cmd)}")
                
                # Setup environment var untuk Cassandra local
                env = os.environ.copy()
                env["CASSANDRA_HOST"] = "127.0.0.1"
                env["CASSANDRA_PORT"] = "9042"
                
                result = subprocess.run(cmd, cwd=str(project_root), env=env)
                
                if result.returncode == 0:
                    print("\n[+] Scan signals berhasil dieksekusi!")
                    success_file.touch()
                else:
                    print(f"\n[-] Scan signals gagal (Kode Exit: {result.returncode})")
                    failed_file.touch()
            except BaseException as e:
                print(f"[-] Terjadi error saat scan signals: {e}")
                failed_file.touch()
            finally:
                time.sleep(2)
                if trigger_scan.exists():
                    trigger_scan.unlink()
                print("\n[*] Menunggu trigger berikutnya...")
                
        time.sleep(3)

if __name__ == "__main__":
    run_daemon()
