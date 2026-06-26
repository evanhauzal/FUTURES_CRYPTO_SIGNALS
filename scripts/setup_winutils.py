import os
import urllib.request
from pathlib import Path

def setup_hadoop():
    hadoop_dir = Path("C:/hadoop/bin")
    hadoop_dir.mkdir(parents=True, exist_ok=True)
    
    # Menggunakan binary Hadoop 3.2.2 yang paling stabil untuk Spark 3.x di Windows
    base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/"
    
    files = ["winutils.exe", "hadoop.dll"]
    for file in files:
        target_path = hadoop_dir / file
        if not target_path.exists():
            print(f"[*] Mendownload {file}...")
            try:
                urllib.request.urlretrieve(base_url + file, target_path)
                print(f"[+] Berhasil disimpan di {target_path}")
            except Exception as e:
                print(f"[!] Gagal mendownload {file}: {e}")
        else:
            print(f"[+] {file} sudah ada di {target_path}")

if __name__ == "__main__":
    setup_hadoop()
