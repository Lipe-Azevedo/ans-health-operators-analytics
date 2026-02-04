import os
import zipfile
import pandas as pd
import shutil
import io

RAW_DIR = "raw_data"
STAGING_DIR = "staging_data"

def normalize_columns(df):
    df.columns = [c.upper().strip() for c in df.columns]
    col_map = {
        'DATA': 'Trimestre', 'DT_FIM_EXERCICIO': 'Trimestre',
        'REG_ANS': 'RegistroANS', 'CD_CONTA_CONTABIL': 'Conta',
        'DESCRICAO': 'Descricao', 'VL_SALDO_FINAL': 'ValorDespesas'
    }
    df.rename(columns=col_map, inplace=True)
    return df

def clean_currency(value):
    try:
        if isinstance(value, (int, float)): return float(value)
        val_str = str(value).replace('R$', '').strip()
        return float(val_str.replace('.', '').replace(',', '.'))
    except:
        return 0.0

def read_csv_force_utf8(f):
    content = f.read()
    text_content = content.decode('utf-8-sig', errors='replace')
    
    try:
        return pd.read_csv(io.StringIO(text_content), sep=';', on_bad_lines='skip')
    except:
        pass
    
    try:
        return pd.read_csv(io.StringIO(text_content), sep=',', on_bad_lines='skip')
    except:
        return None

def process_dataframe(df, file_name, relative_path):
    if df is None: return None
    df = normalize_columns(df)
    
    required = ['RegistroANS', 'Conta', 'ValorDespesas', 'Descricao']
    if not all(c in df.columns for c in required): return None

    mask = df['Descricao'].astype(str).str.contains('EVENTO|SINISTRO|DESPESA|PROVIS', case=False, na=False)
    filtered = df[mask].copy()
    if filtered.empty: return None

    filtered['ValorDespesas'] = filtered['ValorDespesas'].apply(clean_currency)
    
    if 'Trimestre' not in filtered.columns:
        parts = relative_path.split(os.sep)
        if len(parts) >= 3:
            year, quarter = parts[-3], parts[-2]
            q_map = {'Q1':'03-31', 'Q2':'06-30', 'Q3':'09-30', 'Q4':'12-31'}
            filtered['Trimestre'] = f"{year}-{q_map.get(quarter, '01-01')}"

    return filtered[['RegistroANS', 'Conta', 'Descricao', 'ValorDespesas', 'Trimestre']]

def main():
    if os.path.exists(STAGING_DIR): shutil.rmtree(STAGING_DIR)
    os.makedirs(STAGING_DIR)

    for root, _, files in os.walk(RAW_DIR):
        for file in files:
            if file.endswith('.zip'):
                try:
                    with zipfile.ZipFile(os.path.join(root, file), 'r') as z:
                        for member in z.namelist():
                            if member.lower().endswith(('.csv', '.txt')):
                                with z.open(member) as f:
                                    try:
                                        print(f"Processando: {member}")
                                        df = read_csv_force_utf8(f)
                                        processed = process_dataframe(df, member, root)
                                        
                                        if processed is not None:
                                            out_name = f"{member.replace('/', '_').split('.')[0]}_processed.csv"
                                            processed.to_csv(os.path.join(STAGING_DIR, out_name), index=False, encoding='utf-8')
                                    except Exception as e:
                                        print(f"Erro {member}: {e}")
                except Exception as e:
                    print(f"Erro zip {file}: {e}")

if __name__ == "__main__":
    main()