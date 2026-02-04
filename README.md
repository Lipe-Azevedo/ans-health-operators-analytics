# ANS Health Operators Analytics

## Tecnologias Utilizadas

* **Python 3.9**
* **Pandas / NumPy** – Processamento e análise de dados
* **FastAPI** – Backend e API
* **PostgreSQL 15** – Persistência de dados
* **Docker & Docker Compose** – Orquestração do ambiente
* **Nginx** – Servidor estático do frontend
* **Vue.js + Chart.js + Bootstrap** – Interface web

---

## Estrutura do Projeto

```
ans-health-operators-analytics/
├── backend/
│   ├── api.py
│   ├── run_pipeline.py
│   ├── download_ans_financial_data.py
│   ├── process_data.py
│   ├── consolidate_data.py
│   ├── enrich_data.py
│   ├── aggregate_data.py
│   ├── setup_database.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── frontend/
│   ├── index.html
│   ├── js/
│   │   └── app.js
│   ├── css/
│   │   └── style.css
│   └── Dockerfile
│
├── db/
│   └── init.sql
│
├── output/
│   ├── staging_data/
│   ├── consolidado_despesas_enriquecido.csv
│   ├── despesas_agregadas.csv
│   └── Teste_LuizFelipe.zip
│
├── answers/
│   ├── ans_api_collection.json
│   └── run_advanced_queries.py
│
├── docker-compose.yml
└── README.md
```

---

## Como Executar o Projeto

### Pré-requisitos

* Docker
* Docker Compose

### Passo 1 – Subir a infraestrutura

Na raiz do projeto, execute:

```bash
sudo docker-compose up -d
```

Isso irá iniciar:

* Banco PostgreSQL
* Backend (FastAPI)
* Frontend (Nginx)

### Passo 2 – Executar a pipeline completa

Com os containers em execução:

```bash
sudo docker exec -it ans_backend python run_pipeline.py
```

Este comando executa todas as etapas exigidas pelo desafio, na ordem correta.

---

## Pipeline de Processamento (Detalhado)

### 1. Baixar dados financeiros da ANS

**Script:** `download_ans_financial_data.py`

* Realiza o download automático dos arquivos trimestrais disponíveis
* Ignora arquivos inexistentes
* Evita re-download de arquivos já existentes

**Saída:**

```
output/raw_data/
```

### 2. Processar dados brutos

**Script:** `process_data.py`

* Extrai os arquivos CSV dos ZIPs
* Filtra apenas eventos de despesas relevantes
* Normaliza colunas e tipos de dados

**Saída:**

```
output/processed_data/
```

### 3. Consolidar trimestres

**Script:** `consolidate_data.py`

* Une todos os trimestres processados
* Gera um único arquivo consolidado

**Saída:**

```
output/staging_data/consolidado_despesas.csv
```

### 4. Enriquecer dados com CADOP

**Script:** `enrich_data.py`

* Baixa automaticamente o cadastro de operadoras (CADOP)
* Associa informações como:

  * Razão Social
  * UF
  * Modalidade
* Trata valores ausentes com preenchimento padrão

**Saída:**

```
output/consolidado_despesas_enriquecido.csv
```

### 5. Gerar agregações estatísticas

**Script:** `aggregate_data.py`

* Agrupa os dados por:

  * Razão Social
  * UF
* Calcula:

  * Total de despesas
  * Média trimestral
  * Desvio padrão
* Gera o arquivo final exigido no desafio

**Saídas:**

```
output/despesas_agregadas.csv
answers/Teste_LuizFelipe.zip
```

### 6. Configurar banco de dados

**Script:** `setup_database.py`

* Cria a estrutura de tabelas
* Limpa dados existentes (TRUNCATE)
* Importa:

  * Operadoras
  * Despesas
  * Agregações

**Banco:** PostgreSQL

---

## Frontend

O frontend é servido via Nginx e acessível em:

```
http://localhost:8080
```

### Funcionalidades

* Listagem de operadoras
* Busca por Razão Social ou CNPJ
* Visualização de indicadores financeiros
* Dashboard com gráficos

O frontend consome exclusivamente a API do backend, sem acesso direto ao banco.

---

## Backend / API

O backend é executado com FastAPI e acessível em:

```
http://localhost:8000
```

Inclui:

* Endpoints REST
* Conexão com PostgreSQL
* Suporte ao frontend

---

## Padronização e Boas Práticas

* Mensagens de execução 100% em português (pt-BR)
* Pipeline automatizada e reexecutável
* Separação clara entre backend, frontend e banco
* Uso de Docker para garantir reprodutibilidade

---

## Observações Importantes para Avaliação

* Toda a execução solicitada no PDF está documentada e automatizada
* O avaliador precisa executar apenas dois comandos
* Nenhuma etapa manual adicional é necessária
* Todos os arquivos de resposta estão organizados conforme solicitado

---

## Respostas ao Teste – Intuitive Care

### 1. Teste de Integração com API Pública

#### 1.1 Acesso à API de Dados Abertos da ANS

**Como foi feito:**

O projeto acessa diretamente o repositório público da ANS em:

```
https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/
```

O script identifica automaticamente os últimos 3 trimestres disponíveis, navegando pela estrutura `YYYY/QQ/`, tentando baixar os arquivos ZIP de cada trimestre de forma resiliente.

**Tratamento de variações:**

* Caso um trimestre não esteja disponível, ele é ignorado sem interromper o pipeline
* Caso o arquivo já exista localmente, o download não é repetido
* O pipeline continua mesmo quando algum trimestre não existe (ex: `4T2025.zip`)

Essa abordagem garante robustez contra inconsistências do repositório público, conforme solicitado.

#### 1.2 Processamento de Arquivos

**Decisões técnicas:**

* Todos os arquivos ZIP são baixados e extraídos automaticamente
* Apenas arquivos relacionados a Despesas com Eventos/Sinistros são processados
* O código identifica automaticamente o formato dos arquivos (CSV, TXT ou XLSX)
* As colunas são normalizadas para um formato comum antes do processamento

**Trade-off técnico – Processamento em memória vs incremental:**

* **Escolha:** processamento incremental (arquivo a arquivo)
* **Justificativa:**

  * Evita alto consumo de memória
  * Facilita o tratamento de erros por arquivo
  * Escala melhor para volumes maiores de dados
  * Mantém o código simples e legível (KISS)

#### 1.3 Consolidação e Análise de Inconsistências

**Resultado:**

* Os dados dos 3 trimestres são consolidados em um único CSV

**Colunas finais:**

* CNPJ
* RazaoSocial
* Trimestre
* Ano
* ValorDespesas

**Tratamento de inconsistências:**

* **CNPJs duplicados com razões sociais diferentes**

  * Mantidos, pois refletem inconsistências reais da fonte. O enriquecimento posterior resolve conflitos.
* **Valores zerados ou negativos**

  * Mantidos no dataset para análise posterior, não descartados silenciosamente.
* **Trimestres com formatos inconsistentes**

  * Normalizados para um padrão único (Ano + Trimestre).

**Entrega:**

* Arquivo final compactado em `consolidado_despesas.zip`

---

### 2. Teste de Transformação e Validação de Dados

#### 2.1 Validação de Dados

**Validações implementadas:**

* CNPJ válido (formato e dígitos verificadores)
* Valores numéricos
* Razão social não vazia

**Trade-off técnico – CNPJs inválidos:**

* **Escolha:** manter registros inválidos, mas normalizar e marcar inconsistências
* **Justificativa:**

  * Dados públicos frequentemente possuem inconsistências
  * Excluir registros pode gerar perda de informação
  * Manter permite auditoria, análise crítica e rastreabilidade

#### 2.2 Enriquecimento com Dados Cadastrais (CADOP)

**Como foi feito:**

* Download automático do CSV de operadoras ativas
* Join realizado pelo CNPJ

**Colunas adicionadas:**

* RegistroANS
* Modalidade
* UF

**Tratamento de falhas:**

* Registros sem correspondência no CADOP recebem valores padrão (`DESCONHECIDO`)
* CNPJs duplicados no cadastro utilizam o primeiro registro válido encontrado

**Trade-off técnico – Estratégia de join:**

* **Escolha:** join em memória com pandas
* **Justificativa:**

  * Volume de dados moderado
  * Código simples e legível
  * Execução rápida para o contexto do teste

#### 2.3 Agregação de Dados

**Agregações realizadas:**

* Total de despesas por Razão Social e UF
* Média de despesas por trimestre
* Desvio padrão das despesas

**Ordenação:**

* Ordenado por valor total de despesas (decrescente)

**Trade-off técnico – Ordenação:**

* **Escolha:** ordenação em memória
* **Justificativa:**

  * Dataset consolidado já reduzido
  * Evita complexidade desnecessária
  * Boa performance para o volume esperado

**Entrega:**

* Arquivo `despesas_agregadas.csv`
* Compactado no ZIP final do projeto

---

### 3. Teste de Banco de Dados e Análise

#### 3.2 Estrutura das Tabelas (DDL)

**Abordagem escolhida:** tabelas normalizadas

**Justificativa:**

* Facilita manutenção
* Evita redundância
* Permite queries analíticas mais claras
* Volume de dados compatível com normalização

**Tipos de dados:**

* Valores monetários: `DECIMAL` (precisão financeira)
* Datas/trimestres: campos separados (ano, trimestre)
* CNPJ: `VARCHAR`

#### 3.3 Importação dos CSVs

**Tratamento de inconsistências:**

* Valores nulos: substituídos quando possível
* Strings em campos numéricos: tentativa de conversão
* Datas inconsistentes: normalizadas previamente no pipeline

#### 3.4 Queries Analíticas

* **Query 1 – Crescimento percentual entre trimestres**

  * Considera apenas operadoras com dados nos dois períodos
  * Crescimento calculado com base no primeiro e último trimestre disponível

* **Query 2 – Distribuição por UF**

  * Soma total por UF
  * Cálculo adicional de média por operadora/UF

* **Query 3 – Operadoras acima da média**

  * Conta operadoras que superaram a média geral em pelo menos 2 dos 3 trimestres

**Trade-off técnico:**

* Queries priorizam legibilidade e clareza
* Uso de CTEs para facilitar manutenção

---

### 4. Teste de API e Interface Web

#### 4.2 Backend

* **Framework escolhido:** FastAPI
* **Justificativa:**

  * Melhor performance
  * Validação automática
  * Documentação OpenAPI nativa
  * Menor boilerplate

**Paginação:**

* Offset-based
* Simples e suficiente para o volume atual

**Cache:**

* Estatísticas calculadas via query direta
* Evita inconsistência entre dados e cache

**Formato de resposta:**

```json
{
  "data": [...],
  "page": 1,
  "limit": 10,
  "total": 100
}
```

#### 4.3 Frontend (Vue.js)

**Decisões:**

* Busca feita no servidor
* Gerenciamento simples de estado
* Tratamento explícito de loading, erro e dados vazios

**Justificativa:**

* Dataset potencialmente grande
* Melhor UX
* Código mais simples

---

## Considerações Finais

O projeto prioriza:

* Robustez frente a dados públicos inconsistentes
* Clareza de código
* Decisões técnicas justificadas
* Execução simples via pipeline único
