import os
import requests
from datetime import datetime

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis"
OUTPUT_DIR = "raw_data"

def download_last_available_quarters(limit=3):
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    
    current_quarter = (month - 1) // 3
    if current_quarter == 0:
        current_quarter = 4
        year -= 1

    downloaded_count = 0
    
    while downloaded_count < limit:
        file_name = f"{current_quarter}T{year}.zip"
        url = f"{BASE_URL}/{year}/{file_name}"
        dir_path = f"{OUTPUT_DIR}/{year}/Q{current_quarter}"
        
        print(f"Tentando buscar: {url}")
        
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                os.makedirs(dir_path, exist_ok=True)
                local_path = os.path.join(dir_path, file_name)
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print(f"Sucesso: {local_path}")
                downloaded_count += 1
            else:
                print(f"Não disponível (Status {response.status_code}). Tentando anterior...")
        except Exception as e:
            print(f"Erro ao acessar {url}: {e}")

        current_quarter -= 1
        if current_quarter == 0:
            current_quarter = 4
            year -= 1

def main():
    download_last_available_quarters()

if __name__ == "__main__":
    main()