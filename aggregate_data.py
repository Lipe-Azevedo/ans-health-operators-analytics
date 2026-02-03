import pandas as pd
import zipfile
import sys

INPUT_FILE = "consolidado_despesas_enriquecido.csv"
OUTPUT_FILE = "despesas_agregadas.csv"
ZIP_FILE = "Teste_LuizFelipe.zip"

def main():
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Erro: Arquivo {INPUT_FILE} nao encontrado.")
        sys.exit(1)

    agg_df = df.groupby(['RazaoSocial', 'UF'])['ValorDespesas'].agg(
        TotalDespesas='sum',
        MediaTrimestral='mean',
        DesvioPadrao='std'
    ).reset_index()

    agg_df.sort_values(by='TotalDespesas', ascending=False, inplace=True)

    agg_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')

    with zipfile.ZipFile(ZIP_FILE, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(OUTPUT_FILE)
    
    print(f"Sucesso. Arquivos gerados: {OUTPUT_FILE} e {ZIP_FILE}")

if __name__ == "__main__":
    main()