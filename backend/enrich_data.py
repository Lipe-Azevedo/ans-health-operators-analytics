import pandas as pd
import requests
import io
import os
import sys
import re

BASE_DIR_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
INPUT_FILE = "consolidado_despesas.csv"
OUTPUT_FILE = "consolidado_despesas_enriquecido.csv"

def get_csv_url():
    try:
        r = requests.get(BASE_DIR_URL, timeout=30)
        r.raise_for_status()
        match = re.search(r'href="([^"]+\.csv)"', r.text, re.IGNORECASE)
        if match:
            return BASE_DIR_URL + match.group(1)
    except:
        pass
    return None

def download_cadastre():
    url = get_csv_url()
    if not url:
        url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
        
    print(f"Baixando: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), sep=';', encoding='iso-8859-1', on_bad_lines='skip', dtype=str)

def normalize_cadastre_columns(df):
    df.columns = [c.strip().replace('"', '').replace(' ', '_') for c in df.columns]
    
    rename_map = {
        'REGISTRO_OPERADORA': 'RegistroANS',
        'Registro_ANS': 'RegistroANS',
        'Reg_ANS': 'RegistroANS',
        'CNPJ': 'CNPJ_Cadastre',
        'Razao_Social': 'RazaoSocial_Cadastre',
        'Razão_Social': 'RazaoSocial_Cadastre',
        'Modalidade': 'Modalidade',
        'UF': 'UF'
    }
    df.rename(columns=rename_map, inplace=True)
    return df

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Erro: Arquivo {INPUT_FILE} nao encontrado.")
        sys.exit(1)

    df_financial = pd.read_csv(INPUT_FILE, dtype={'RegistroANS': str, 'CNPJ': str})
    
    try:
        df_cadastre = download_cadastre()
    except Exception as e:
        print(f"Erro no download: {e}")
        sys.exit(1)
        
    if df_cadastre is None or df_cadastre.empty:
        sys.exit(1)

    df_cadastre = normalize_cadastre_columns(df_cadastre)

    if 'RegistroANS' not in df_cadastre.columns:
        print(f"Erro: Coluna RegistroANS nao encontrada. Colunas: {list(df_cadastre.columns)}")
        sys.exit(1)

    df_financial['RegistroANS'] = pd.to_numeric(df_financial['RegistroANS'], errors='coerce')
    df_cadastre['RegistroANS'] = pd.to_numeric(df_cadastre['RegistroANS'], errors='coerce')

    merged_df = pd.merge(df_financial, 
                         df_cadastre[['RegistroANS', 'CNPJ_Cadastre', 'RazaoSocial_Cadastre', 'Modalidade', 'UF']], 
                         on='RegistroANS', 
                         how='left')

    merged_df['CNPJ'] = merged_df['CNPJ_Cadastre'].fillna(merged_df['CNPJ'])
    merged_df['RazaoSocial'] = merged_df['RazaoSocial_Cadastre'].fillna(merged_df['RazaoSocial'])

    merged_df['StatusCadastro'] = merged_df['CNPJ_Cadastre'].apply(lambda x: 'ENCONTRADO' if pd.notna(x) else 'NAO_ENCONTRADO')

    merged_df['UF'].fillna('DESCONHECIDO', inplace=True)
    merged_df['Modalidade'].fillna('DESCONHECIDO', inplace=True)

    final_cols = ['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF', 'Trimestre', 'Ano', 'ValorDespesas', 'Descricao', 'Conta', 'StatusCadastro']
    
    merged_df[final_cols].to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Sucesso: {OUTPUT_FILE} gerado.")

if __name__ == "__main__":
    main()