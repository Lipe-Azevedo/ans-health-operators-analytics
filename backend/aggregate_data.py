import pandas as pd
import zipfile
import sys
import os

INPUT_FILE = os.path.join("output", "consolidado_despesas_enriquecido.csv")
OUTPUT_FILE = os.path.join("output", "despesas_agregadas.csv")
ZIP_FILE = os.path.join("output", "Teste_LuizFelipe.zip")

def main():
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        sys.exit(1)

    agg_df = df.groupby(['RazaoSocial', 'UF'])['ValorDespesas'].agg(
        TotalDespesas='sum',
        MediaTrimestral='mean',
        DesvioPadrao='std'
    ).reset_index()

    agg_df.sort_values(by='TotalDespesas', ascending=False, inplace=True)
    agg_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')

    if os.path.exists(ZIP_FILE):
        os.remove(ZIP_FILE)

    with zipfile.ZipFile(ZIP_FILE, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(OUTPUT_FILE, arcname="despesas_agregadas.csv")

if __name__ == "__main__":
    main()