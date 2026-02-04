DROP TABLE IF EXISTS despesas CASCADE;
DROP TABLE IF EXISTS operadoras CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;

CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans INT PRIMARY KEY,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    modalidade VARCHAR(100),
    uf VARCHAR(50)
);

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

CREATE TABLE IF NOT EXISTS despesas_agregadas (
    id SERIAL PRIMARY KEY,
    razao_social VARCHAR(255),
    uf VARCHAR(50),
    total_despesas DECIMAL(15,2),
    media_trimestral DECIMAL(15,2),
    desvio_padrao DECIMAL(15,2)
);