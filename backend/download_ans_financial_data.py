import os
import requests
from datetime import datetime

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis"
OUTPUT_DIR = os.path.join("output", "raw_data")

def download_last_quarters(limit=3):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    
    current_quarter = (month - 1) // 3
    if current_quarter == 0:
        current_quarter = 4
        year -= 1

    downloaded = 0
    attempts = 0
    
    while downloaded < limit and attempts < 12:
        file_name = f"{current_quarter}T{year}.zip"
        url = f"{BASE_URL}/{year}/{file_name}"
        dir_path = os.path.join(OUTPUT_DIR, str(year), f"Q{current_quarter}")
        local_path = os.path.join(dir_path, file_name)
        
        if os.path.exists(local_path):
            print(f"Arquivo ja existe: {file_name}")
            downloaded += 1
        else:
            print(f"Baixando: {url}")
            try:
                response = requests.get(url, stream=True, timeout=60)
                if response.status_code == 200:
                    os.makedirs(dir_path, exist_ok=True)
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Sucesso: {local_path}")
                    downloaded += 1
                else:
                    print(f"Nao disponivel: {file_name}")
            except Exception as e:
                print(f"Erro ao baixar {url}: {e}")

        attempts += 1
        current_quarter -= 1
        if current_quarter == 0:
            current_quarter = 4
            year -= 1

if __name__ == "__main__":
    download_last_quarters()