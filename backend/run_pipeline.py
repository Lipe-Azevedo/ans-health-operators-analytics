import subprocess
import sys

STEPS = [
    ("Baixando dados financeiros da ANS", "download_ans_financial_data.py"),
    ("Processando dados brutos", "process_data.py"),
    ("Consolidando trimestres", "consolidate_data.py"),
    ("Enriquecendo dados com CADOP", "enrich_data.py"),
    ("Gerando agregações e respostas", "aggregate_data.py"),
    ("Configurando banco de dados", "setup_database.py"),
]

def run():
    for label, script in STEPS:
        print(f"\n{label}")
        subprocess.run(
            [sys.executable, script],
            check=True
        )
    print("\nPipeline finalizado com sucesso")

if __name__ == "__main__":
    run()
