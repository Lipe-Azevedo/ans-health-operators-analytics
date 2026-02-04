import os
import zipfile
import pandas as pd
import shutil
import io

RAW_DIR = "raw_data"
STAGING_DIR = "staging_data"

def setup_directories():
    if os.path.exists(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)
    os.makedirs(STAGING_DIR)

def read_file_content(f):
    content = f.read()
    for encoding in ['utf-8-sig', 'utf-8', 'latin1']:
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode('latin1', errors='replace')

def parse_csv(text_content):
    if not text_content: return None
    
    for sep in [';', ',']:
        try:
            df = pd.read_csv(io.StringIO(text_content), sep=sep, on_bad_lines='skip')
            if len(df.columns) > 1: return df
        except:
            continue
    return None

def clean_currency(value):
    try:
        if isinstance(value, (int, float)): return float(value)
        val_str = str(value).replace('R$', '').strip()
        return float(val_str.replace('.', '').replace(',', '.'))
    except:
        return 0.0

def transform_dataframe(df, relative_path):
    df.columns = [c.upper().strip() for c in df.columns]
    col_map = {
        'DATA': 'Trimestre', 'DT_FIM_EXERCICIO': 'Trimestre',
        'REG_ANS': 'RegistroANS', 'CD_CONTA_CONTABIL': 'Conta',
        'DESCRICAO': 'Descricao', 'VL_SALDO_FINAL': 'ValorDespesas'
    }
    df.rename(columns=col_map, inplace=True)

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

def process_zip_member(z, member, root):
    if not member.lower().endswith(('.csv', '.txt')): return

    with z.open(member) as f:
        try:
            print(f"Processando: {member}")
            text_content = read_file_content(f)
            df = parse_csv(text_content)
            
            if df is None: return

            processed_df = transform_dataframe(df, root)
            
            if processed_df is not None:
                safe_name = member.replace('/', '_').replace('\\', '_').split('.')[0]
                out_path = os.path.join(STAGING_DIR, f"{safe_name}_processed.csv")
                processed_df.to_csv(out_path, index=False, encoding='utf-8')
        except Exception as e:
            print(f"Erro em {member}: {e}")

def main():
    setup_directories()
    
    for root, _, files in os.walk(RAW_DIR):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        for member in z.namelist():
                            process_zip_member(z, member, root)
                except Exception as e:
                    print(f"Erro no ZIP {file}: {e}")

if __name__ == "__main__":
    main()