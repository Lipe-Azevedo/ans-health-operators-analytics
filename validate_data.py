import pandas as pd
import re

INPUT_FILE = "consolidado_despesas.csv"
OUTPUT_FILE = "consolidado_despesas_validado.csv"

def is_valid_cnpj(cnpj):
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    
    if len(cnpj) != 14:
        return False
        
    if len(set(cnpj)) == 1:
        return False
        
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    
    def calculate_digit(weights, body):
        s = sum(w * int(d) for w, d in zip(weights, body))
        rem = s % 11
        return 0 if rem < 2 else 11 - rem

    digit1 = calculate_digit(weights1, cnpj[:12])
    digit2 = calculate_digit(weights2, cnpj[:12] + str(digit1))
    
    return cnpj[-2:] == f"{digit1}{digit2}"

def validate_row(row):
    issues = []
    
    if not is_valid_cnpj(row['CNPJ']):
        issues.append('CNPJ_INVALIDO')
        
    try:
        val = float(row['ValorDespesas'])
        if val <= 0:
            issues.append('VALOR_NAO_POSITIVO')
    except:
        issues.append('VALOR_INVALIDO')
        
    if pd.isna(row['RazaoSocial']) or str(row['RazaoSocial']).strip() == '':
        issues.append('RAZAO_SOCIAL_VAZIA')
        
    return ';'.join(issues) if issues else 'VALIDO'

def main():
    try:
        df = pd.read_csv(INPUT_FILE, dtype={'CNPJ': str})
    except FileNotFoundError:
        return

    df['StatusValidacao'] = df.apply(validate_row, axis=1)
    
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')

if __name__ == "__main__":
    main()