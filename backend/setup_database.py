import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

DB_URL = "postgresql://postgres:ans_password@db:5432/postgres"
DATA_FILE = os.path.join("output", "consolidado_despesas_enriquecido.csv")
AGG_FILE = os.path.join("output", "despesas_agregadas.csv")

def get_engine():
    try:
        return create_engine(DB_URL)
    except Exception as e:
        print(f"Erro ao conectar no banco: {e}")
        sys.exit(1)

def clean_cnpj(value):
    try:
        if pd.isna(value) or str(value).strip() == '':
            return None
        return str(int(float(value)))
    except:
        return None

def import_data(engine):
    if not os.path.exists(DATA_FILE):
        print(f"Erro: {DATA_FILE} nao encontrado. O ETL rodou corretamente?")
        sys.exit(1)

    print("1. Limpando tabelas (TRUNCATE)...")
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE despesas, operadoras, despesas_agregadas CASCADE;"))
        conn.commit()

    try:
        print("2. Lendo arquivos CSV...")
        df_full = pd.read_csv(DATA_FILE, encoding='utf-8')
        
        if os.path.exists(AGG_FILE):
            df_agg = pd.read_csv(AGG_FILE, encoding='utf-8')
        else:
            df_agg = pd.DataFrame()
            
    except Exception as e:
        print(f"Erro leitura CSV: {e}")
        sys.exit(1)

    print("3. Inserindo Operadoras...")
    df_ops = df_full[['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']].drop_duplicates('RegistroANS').copy()
    df_ops.columns = ['registro_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']
    
    df_ops['registro_ans'] = pd.to_numeric(df_ops['registro_ans'], errors='coerce').fillna(0).astype(int)
    df_ops['cnpj'] = df_ops['cnpj'].apply(clean_cnpj)
    
    df_ops = df_ops[df_ops['registro_ans'] > 0]
    df_ops = df_ops.dropna(subset=['cnpj'])
    
    df_ops.to_sql('operadoras', engine, if_exists='append', index=False)

    print("4. Inserindo Despesas...")
    df_fin = df_full[['RegistroANS', 'Trimestre', 'Ano', 'Conta', 'Descricao', 'ValorDespesas']].copy()
    df_fin.columns = ['registro_ans', 'trimestre', 'ano', 'conta', 'descricao', 'valor_despesas']
    
    df_fin['registro_ans'] = pd.to_numeric(df_fin['registro_ans'], errors='coerce').fillna(0).astype(int)
    df_fin = df_fin[df_fin['registro_ans'].isin(df_ops['registro_ans'])] 
    
    df_fin.to_sql('despesas', engine, if_exists='append', index=False)

    if not df_agg.empty:
        print("5. Inserindo Agregados...")
        df_agg.columns = ['razao_social', 'uf', 'total_despesas', 'media_trimestral', 'desvio_padrao']
        df_agg.to_sql('despesas_agregadas', engine, if_exists='append', index=False)

if __name__ == "__main__":
    engine = get_engine()
    import_data(engine)
    print(">>> Sucesso! Importação concluída.")