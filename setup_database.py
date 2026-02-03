import pandas as pd
from sqlalchemy import create_engine, text
import sys

DB_URL = "postgresql://postgres:ans_password@localhost:5432/postgres"

def get_engine():
    try:
        return create_engine(DB_URL)
    except Exception as e:
        sys.exit(1)

def create_schema(engine):
    ddl_operadoras = """
    CREATE TABLE IF NOT EXISTS operadoras (
        registro_ans INT PRIMARY KEY,
        cnpj VARCHAR(20),
        razao_social VARCHAR(255),
        modalidade VARCHAR(100),
        uf VARCHAR(2)
    );
    """

    ddl_despesas = """
    CREATE TABLE IF NOT EXISTS despesas (
        id SERIAL PRIMARY KEY,
        registro_ans INT,
        trimestre DATE,
        ano INT,
        conta VARCHAR(50),
        descricao VARCHAR(255),
        valor_despesas DECIMAL(15,2),
        FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans)
    );
    """

    ddl_agregadas = """
    CREATE TABLE IF NOT EXISTS despesas_agregadas (
        id SERIAL PRIMARY KEY,
        razao_social VARCHAR(255),
        uf VARCHAR(2),
        total_despesas DECIMAL(15,2),
        media_trimestral DECIMAL(15,2),
        desvio_padrao DECIMAL(15,2)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS despesas CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS operadoras CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS despesas_agregadas CASCADE;"))
        
        conn.execute(text(ddl_operadoras))
        conn.execute(text(ddl_despesas))
        conn.execute(text(ddl_agregadas))
        conn.commit()

def import_data(engine):
    try:
        df_full = pd.read_csv("consolidado_despesas_enriquecido.csv")
        df_agg = pd.read_csv("despesas_agregadas.csv")
    except FileNotFoundError:
        sys.exit(1)

    # Tabela Operadoras (Dimensão)
    df_ops = df_full[['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']].drop_duplicates('RegistroANS').copy()
    df_ops.columns = ['registro_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']
    
    # Tratamento de nulos/tipos
    df_ops['registro_ans'] = pd.to_numeric(df_ops['registro_ans'], errors='coerce').fillna(0).astype(int)
    df_ops = df_ops[df_ops['registro_ans'] > 0]
    
    df_ops.to_sql('operadoras', engine, if_exists='append', index=False)

    # Tabela Despesas (Fato)
    df_fin = df_full[['RegistroANS', 'Trimestre', 'Ano', 'Conta', 'Descricao', 'ValorDespesas']].copy()
    df_fin.columns = ['registro_ans', 'trimestre', 'ano', 'conta', 'descricao', 'valor_despesas']
    
    df_fin['registro_ans'] = pd.to_numeric(df_fin['registro_ans'], errors='coerce').fillna(0).astype(int)
    df_fin = df_fin[df_fin['registro_ans'].isin(df_ops['registro_ans'])] # Integridade referencial
    
    df_fin.to_sql('despesas', engine, if_exists='append', index=False)

    # Tabela Agregada
    df_agg.columns = ['razao_social', 'uf', 'total_despesas', 'media_trimestral', 'desvio_padrao']
    df_agg.to_sql('despesas_agregadas', engine, if_exists='append', index=False)

def main():
    engine = get_engine()
    create_schema(engine)
    import_data(engine)
    print("Sucesso: Banco de dados configurado e populado.")

if __name__ == "__main__":
    main()