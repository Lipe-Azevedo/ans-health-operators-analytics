from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import Optional

app = FastAPI()

origins = ["*"]  
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = "postgresql://postgres:ans_password@db:5432/postgres"
engine = create_engine(DB_URL)

@app.get("/api/operadoras")
def list_operadoras(page: int = 1, limit: int = 10, search: Optional[str] = None):
    offset = (page - 1) * limit
    params = {'limit': limit, 'offset': offset}
    
    base_where = "WHERE cnpj IS NOT NULL AND cnpj != ''"
    
    if search:
        where_clause = f"{base_where} AND (razao_social ILIKE :search OR cnpj ILIKE :search)"
        params['search'] = f"%{search}%"
    else:
        where_clause = base_where
    
    query_total = text(f"SELECT COUNT(*) FROM operadoras {where_clause}")
    query_data = text(f"""
        SELECT registro_ans, cnpj, razao_social, modalidade, uf 
        FROM operadoras 
        {where_clause}
        ORDER BY registro_ans
        LIMIT :limit OFFSET :offset
    """)
    
    with engine.connect() as conn:
        total = conn.execute(query_total, params).scalar()
        result = conn.execute(query_data, params).mappings().all()
        
    return {
        "data": result,
        "page": page,
        "limit": limit,
        "total": total
    }

@app.get("/api/operadoras/{cnpj}")
def get_operadora(cnpj: str):
    query = text("SELECT * FROM operadoras WHERE cnpj = :cnpj")
    with engine.connect() as conn:
        result = conn.execute(query, {'cnpj': cnpj}).mappings().one_or_none()
    
    if not result:
        raise HTTPException(status_code=404, detail="Operadora nao encontrada")
    return result

@app.get("/api/operadoras/{cnpj}/despesas")
def get_operadora_despesas(cnpj: str):
    q_reg = text("SELECT registro_ans FROM operadoras WHERE cnpj = :cnpj")
    
    with engine.connect() as conn:
        reg = conn.execute(q_reg, {'cnpj': cnpj}).scalar()
        
        if not reg:
            raise HTTPException(status_code=404, detail="Operadora nao encontrada")
            
        q_desp = text("""
            SELECT ano, trimestre, conta, descricao, valor_despesas 
            FROM despesas 
            WHERE registro_ans = :reg
            ORDER BY ano DESC, trimestre DESC
        """)
        despesas = conn.execute(q_desp, {'reg': reg}).mappings().all()
        
    return despesas

@app.get("/api/estatisticas")
def get_estatisticas():
    q_total = text("SELECT SUM(valor_despesas) FROM despesas")
    q_media = text("SELECT AVG(valor_despesas) FROM despesas")
    q_top5 = text("""
        SELECT razao_social, total_despesas 
        FROM despesas_agregadas 
        ORDER BY total_despesas DESC 
        LIMIT 5
    """)
    
    with engine.connect() as conn:
        total = conn.execute(q_total).scalar()
        media = conn.execute(q_media).scalar()
        top5 = conn.execute(q_top5).mappings().all()
        
    return {
        "total_despesas": total,
        "media_despesas": media,
        "top_5_operadoras": top5
    }