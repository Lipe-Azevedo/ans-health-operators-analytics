import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

DB_URL = "postgresql://postgres:ans_password@db:5432/postgres"
DATA_FILE = os.path.join("output", "consolidado_despesas_enriquecido.csv")
AGG_FILE = "despesas_agregadas.csv" 

def get_engine():
    try:
        return create_engine(DB_URL)
    except Exception as e:
        print(f"Erro: {e}")
        sys.exit(1)

def create_schema(engine):
    ddl_ops = "CREATE TABLE IF NOT EXISTS operadoras (registro_ans INT PRIMARY KEY, cnpj VARCHAR(20), razao_social VARCHAR(255), modalidade VARCHAR(100), uf VARCHAR(50));"
    ddl_desp = "CREATE TABLE IF NOT EXISTS despesas (id SERIAL PRIMARY KEY, registro_ans INT, trimestre DATE, ano INT, conta VARCHAR(50), descricao VARCHAR(255), valor_despesas DECIMAL(15,2), FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans));"
    ddl_agg = "CREATE TABLE IF NOT EXISTS despesas_agregadas (id SERIAL PRIMARY KEY, razao_social VARCHAR(255), uf VARCHAR(50), total_despesas DECIMAL(15,2), media_trimestral DECIMAL(15,2), desvio_padrao DECIMAL(15,2));"

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS despesas CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS operadoras CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS despesas_agregadas CASCADE;"))
        conn.execute(text(ddl_ops))
        conn.execute(text(ddl_desp))
        conn.execute(text(ddl_agg))
        conn.commit()

def clean_cnpj(value):
    try:
        if pd.isna(value) or str(value).strip() == '':
            return None
        return str(int(float(value)))
    except:
        return None

def import_data(engine):
    if not os.path.exists(DATA_FILE):
        print(f"Erro: {DATA_FILE} nao encontrado. Rode enrich_data.py primeiro.")
        sys.exit(1)

    try:
        df_full = pd.read_csv(DATA_FILE, encoding='utf-8')
        if os.path.exists(AGG_FILE):
            df_agg = pd.read_csv(AGG_FILE, encoding='utf-8')
        else:
            df_agg = pd.DataFrame()
    except Exception as e:
        print(f"Erro leitura CSV: {e}")
        sys.exit(1)

    df_ops = df_full[['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']].drop_duplicates('RegistroANS').copy()
    df_ops.columns = ['registro_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']
    
    df_ops['registro_ans'] = pd.to_numeric(df_ops['registro_ans'], errors='coerce').fillna(0).astype(int)
    df_ops['cnpj'] = df_ops['cnpj'].apply(clean_cnpj)
    
    df_ops = df_ops[df_ops['registro_ans'] > 0]
    df_ops = df_ops.dropna(subset=['cnpj'])
    
    df_ops.to_sql('operadoras', engine, if_exists='append', index=False)

    df_fin = df_full[['RegistroANS', 'Trimestre', 'Ano', 'Conta', 'Descricao', 'ValorDespesas']].copy()
    df_fin.columns = ['registro_ans', 'trimestre', 'ano', 'conta', 'descricao', 'valor_despesas']
    
    df_fin['registro_ans'] = pd.to_numeric(df_fin['registro_ans'], errors='coerce').fillna(0).astype(int)
    df_fin = df_fin[df_fin['registro_ans'].isin(df_ops['registro_ans'])] 
    
    df_fin.to_sql('despesas', engine, if_exists='append', index=False)

    if not df_agg.empty:
        df_agg.columns = ['razao_social', 'uf', 'total_despesas', 'media_trimestral', 'desvio_padrao']
        df_agg.to_sql('despesas_agregadas', engine, if_exists='append', index=False)

if __name__ == "__main__":
    engine = get_engine()
    create_schema(engine)
    import_data(engine)
    print("Sucesso")