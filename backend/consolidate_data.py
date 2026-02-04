import os
import pandas as pd

STAGING_DIR = os.path.join("output", "staging_data")
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(STAGING_DIR, "consolidado_despesas.csv")

def process_consolidation():
    if not os.path.exists(STAGING_DIR): 
        return
    
    all_files = [os.path.join(STAGING_DIR, f) for f in os.listdir(STAGING_DIR) if f.endswith('.csv') and 'consolidado' not in f]
    
    if not all_files: 
        return

    df_list = []
    for f in all_files:
        try:
            df = pd.read_csv(f, encoding='utf-8')
            df_list.append(df)
        except: continue

    if not df_list: return

    full_df = pd.concat(df_list, ignore_index=True)
    full_df.drop_duplicates(inplace=True)
    full_df = full_df[full_df['ValorDespesas'] != 0]

    full_df['Trimestre'] = pd.to_datetime(full_df['Trimestre'], errors='coerce').dt.strftime('%Y-%m-%d')
    full_df['Ano'] = pd.to_datetime(full_df['Trimestre']).dt.year

    if 'CNPJ' not in full_df.columns: full_df['CNPJ'] = ''
    if 'RazaoSocial' not in full_df.columns: full_df['RazaoSocial'] = ''

    cols = ['RegistroANS', 'CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas', 'Descricao', 'Conta']
    
    full_df[cols].to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Gerado: {OUTPUT_FILE}")

if __name__ == "__main__":
    process_consolidation()