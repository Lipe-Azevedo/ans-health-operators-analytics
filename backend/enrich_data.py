import pandas as pd
import requests
import io
import os
import sys
import re

BASE_DIR_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
STAGING_DIR = os.path.join("output", "staging_data")
OUTPUT_DIR = "output"
INPUT_FILE = os.path.join(STAGING_DIR, "consolidado_despesas.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "consolidado_despesas_enriquecido.csv")

def setup_output():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def get_csv_url():
    try:
        r = requests.get(BASE_DIR_URL, timeout=30)
        r.raise_for_status()
        match = re.search(r'href="([^"]+\.csv)"', r.text, re.IGNORECASE)
        if match: return BASE_DIR_URL + match.group(1)
    except: pass
    return None

def download_cadastre():
    url = get_csv_url() or "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    print(f"Baixando: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    try:
        return pd.read_csv(io.BytesIO(r.content), sep=';', encoding='utf-8', on_bad_lines='skip', dtype=str)
    except:
        return pd.read_csv(io.BytesIO(r.content), sep=';', encoding='iso-8859-1', on_bad_lines='skip', dtype=str)

def normalize_cadastre_columns(df):
    df.columns = [c.strip().replace('"', '').replace(' ', '_') for c in df.columns]
    rename = {'REGISTRO_OPERADORA': 'RegistroANS', 'Registro_ANS': 'RegistroANS', 'Reg_ANS': 'RegistroANS',
              'CNPJ': 'CNPJ_Cadastre', 'Razao_Social': 'RazaoSocial_Cadastre', 'Razão_Social': 'RazaoSocial_Cadastre',
              'Modalidade': 'Modalidade', 'UF': 'UF'}
    df.rename(columns=rename, inplace=True)
    return df

def main():
    setup_output()
    if not os.path.exists(INPUT_FILE):
        print(f"Erro: {INPUT_FILE} nao encontrado.")
        sys.exit(1)

    df_fin = pd.read_csv(INPUT_FILE, dtype={'RegistroANS': str, 'CNPJ': str}, encoding='utf-8')
    
    try: df_cad = download_cadastre()
    except Exception as e:
        print(f"Erro download: {e}"); sys.exit(1)

    if df_cad is None or df_cad.empty: sys.exit(1)
    df_cad = normalize_cadastre_columns(df_cad)

    df_fin['RegistroANS'] = pd.to_numeric(df_fin['RegistroANS'], errors='coerce')
    df_cad['RegistroANS'] = pd.to_numeric(df_cad['RegistroANS'], errors='coerce')

    merged = pd.merge(df_fin, df_cad[['RegistroANS', 'CNPJ_Cadastre', 'RazaoSocial_Cadastre', 'Modalidade', 'UF']], on='RegistroANS', how='left')

    merged['CNPJ'] = merged['CNPJ_Cadastre'].fillna(merged['CNPJ'])
    merged['RazaoSocial'] = merged['RazaoSocial_Cadastre'].fillna(merged['RazaoSocial'])
    merged['StatusCadastro'] = merged['CNPJ_Cadastre'].apply(lambda x: 'ENCONTRADO' if pd.notna(x) else 'NAO_ENCONTRADO')
    merged['UF'].fillna('DESCONHECIDO', inplace=True)
    merged['Modalidade'].fillna('DESCONHECIDO', inplace=True)

    cols = ['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF', 'Trimestre', 'Ano', 'ValorDespesas', 'Descricao', 'Conta', 'StatusCadastro']
    
    merged[cols].to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Sucesso: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()