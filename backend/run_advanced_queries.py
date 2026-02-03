import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:ans_password@db:5432/postgres"

def main():
    try:
        engine = create_engine(DB_URL)
        
        
        print("\n=== QUERY 1: Top 5 Crescimento de Despesas ===")
        q1 = """
        WITH periodos AS (
            SELECT MIN(trimestre) as inicio, MAX(trimestre) as fim FROM despesas
        ),
        despesas_inicio AS (
            SELECT registro_ans, SUM(valor_despesas) as total_ini
            FROM despesas, periodos
            WHERE trimestre = periodos.inicio
            GROUP BY registro_ans
        ),
        despesas_fim AS (
            SELECT registro_ans, SUM(valor_despesas) as total_fim
            FROM despesas, periodos
            WHERE trimestre = periodos.fim
            GROUP BY registro_ans
        )
        SELECT 
            o.razao_social,
            d_ini.total_ini as despesa_inicial,
            d_fim.total_fim as despesa_final,
            ROUND(((d_fim.total_fim - d_ini.total_ini) / d_ini.total_ini) * 100, 2) as crescimento_pct
        FROM despesas_inicio d_ini
        JOIN despesas_fim d_fim ON d_ini.registro_ans = d_fim.registro_ans
        JOIN operadoras o ON d_ini.registro_ans = o.registro_ans
        ORDER BY crescimento_pct DESC
        LIMIT 5;
        """
        print(pd.read_sql(q1, engine).to_string(index=False))

        
        print("\n=== QUERY 2: Top 5 Estados (Total e Media por Operadora) ===")
        q2 = """
        SELECT 
            o.uf,
            SUM(d.valor_despesas) as despesas_totais,
            COUNT(DISTINCT d.registro_ans) as qtd_operadoras,
            ROUND(SUM(d.valor_despesas) / COUNT(DISTINCT d.registro_ans), 2) as media_por_operadora
        FROM despesas d
        JOIN operadoras o ON d.registro_ans = o.registro_ans
        GROUP BY o.uf
        ORDER BY despesas_totais DESC
        LIMIT 5;
        """
        print(pd.read_sql(q2, engine).to_string(index=False))


        print("\n=== QUERY 3: Operadoras Acima da Media em >= 2 Trimestres ===")
        q3 = """
        WITH metricas_trimestrais AS (
            SELECT 
                registro_ans,
                trimestre,
                SUM(valor_despesas) as despesa_operadora,
                AVG(SUM(valor_despesas)) OVER(PARTITION BY trimestre) as media_geral_trimestre
            FROM despesas
            GROUP BY registro_ans, trimestre
        ),
        operadoras_acima AS (
            SELECT registro_ans
            FROM metricas_trimestrais
            WHERE despesa_operadora > media_geral_trimestre
            GROUP BY registro_ans
            HAVING COUNT(*) >= 2
        )
        SELECT COUNT(*) as qtd_operadoras_encontradas 
        FROM operadoras_acima;
        """
        print(pd.read_sql(q3, engine).to_string(index=False))

    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()