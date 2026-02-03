import os
import zipfile
import pandas as pd
import shutil

RAW_DIR = "raw_data"
STAGING_DIR = "staging_data"

def normalize_columns(df):
    df.columns = [c.upper().strip() for c in df.columns]
    
    col_map = {
        'DATA': 'Trimestre',
        'DT_FIM_EXERCICIO': 'Trimestre',
        'REG_ANS': 'RegistroANS',
        'CD_CONTA_CONTABIL': 'Conta',
        'DESCRICAO': 'Descricao',
        'VL_SALDO_FINAL': 'ValorDespesas'
    }
    
    df.rename(columns=col_map, inplace=True)
    return df

def clean_currency(value):
    try:
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value).replace('.', '').replace(',', '.'))
    except:
        return 0.0

def process_dataframe(df, file_name, relative_path):
    df = normalize_columns(df)
    
    required_cols = ['RegistroANS', 'Conta', 'ValorDespesas', 'Descricao']
    if not all(col in df.columns for col in required_cols):
        return None

    # Filtro para despesas com eventos/sinistros (Classe 4 geralmente)
    mask = df['Descricao'].astype(str).str.contains('EVENTO|SINISTRO|DESPESA', case=False, na=False)
    filtered_df = df[mask].copy()

    if filtered_df.empty:
        return None

    filtered_df['ValorDespesas'] = filtered_df['ValorDespesas'].apply(clean_currency)
    
    if 'Trimestre' not in filtered_df.columns:
        # Tenta extrair do caminho do arquivo (ex: raw_data/2025/Q3/...)
        parts = relative_path.split(os.sep)
        if len(parts) >= 3:
            year = parts[-3]
            quarter_str = parts[-2] 
            # Normaliza para data aproximada do fim do trimestre para consistência
            q_map = {'Q1': '03-31', 'Q2': '06-30', 'Q3': '09-30', 'Q4': '12-31'}
            suffix = q_map.get(quarter_str, '01-01')
            filtered_df['Trimestre'] = f"{year}-{suffix}"

    return filtered_df[['RegistroANS', 'Conta', 'Descricao', 'ValorDespesas', 'Trimestre']]

def extract_and_process():
    if os.path.exists(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)
    os.makedirs(STAGING_DIR)

    for root, _, files in os.walk(RAW_DIR):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        for member in z.namelist():
                            if member.lower().endswith(('.csv', '.txt', '.xlsx')):
                                with z.open(member) as f:
                                    try:
                                        if member.lower().endswith('.xlsx'):
                                            df = pd.read_excel(f)
                                        else:
                                            df = pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')
                                        
                                        processed_df = process_dataframe(df, member, root)
                                        
                                        if processed_df is not None:
                                            output_name = f"{member.replace('/', '_').split('.')[0]}_processed.csv"
                                            output_path = os.path.join(STAGING_DIR, output_name)
                                            processed_df.to_csv(output_path, index=False, encoding='utf-8')
                                            print(f"Processado: {output_name}")
                                    except Exception as e:
                                        print(f"Erro ao ler {member} em {file}: {e}")
                except Exception as e:
                    print(f"Erro ao abrir zip {file}: {e}")

def main():
    extract_and_process()

if __name__ == "__main__":
    main()