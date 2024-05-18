import pandas as pd
import os
import glob

# caminho da pasta em que a equipe de desenvolvimento grava os dados diariamente
caminho_arquivos = 'src\\data\\raw'

# listar os arquivos carregados
arquivos_de_data = glob.glob(os.path.join(caminho_arquivos , '*.xlsx'))

# tratamento caso não haja arquivos compativéis
if not arquivos_de_data:
    print("Arquivo compatível não encontrado.")


# cria uma lista que se tornará um dataframe, uma tabela na memória
dataframe = []

# percorrendo cada arquivo de excel dentro da lista de arquivos criada anteriormente
for tabela in arquivos_de_data:

    try:
        # coloca todas as tabelas dentro de uma variável temporária
        df_temp = pd.read_excel(tabela)

        # para criar uma coluna de rastreabilidade
        nome_arquivo = os.path.basename(tabela)
        df_temp['Arquivo_origem'] = nome_arquivo

        # grava as tabelas tratadas no dataframe principal
        dataframe.append(df_temp)
        
    # tratamento caso não consiga ler o arquivo
    except Exception as e:
        print(f'Erro ao ler o arquivo {tabela} : {e}')


# tratamento no dataframe para concatenar todas as tabelas, eliminar os espaços
if dataframe:
    dataframe_final = pd.concat(dataframe, ignore_index=True)

    # agora para salvar um backup do dataframe pronto na pasta ready
    caminho_saida = os.path.join('src', 'data', 'ready', 'tempo_uso_tratado.xlsx')
    gravar = pd.ExcelWriter(caminho_saida)
    dataframe_final.to_excel(gravar, index=False)
    gravar._save()

else:
    print('Nenhum dado para ser salvo.')



# agora é montada a conexão com o SQL Server

# pyodbc é uma biblioteca responsável pela conexão
import pyodbc

def conecta_ao_banco(driver='SQL Server', server='BRUNOSORATI', database='DW_PTI', username=None, password=None, trusted_connection='yes'):

    # é preciso passar todas as informações necessárias para conexão, assim como seria feito no CMD, por exemplo
    string_conexao = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TRUSTED_CONNECTION={trusted_connection}"

    conexao = pyodbc.connect(string_conexao)
    cursor = conexao.cursor()

    return conexao, cursor

conexao, cursor = conecta_ao_banco()




df_tempo_uso = pd.read_excel('src\\data\\apoio\\tempo_uso_tratado.xlsx')


for index,row in df_tempo_uso.iterrows():
    sql = "INSERT INTO TABELA_CLIENTE (id_pessoal, dia, mes, ano, bairro, cidade, estado, arquivo_origem, tempo_uso_minutos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    consolidado = (row['ID'],row['Dia'],row['Mês'],row['Ano'],row['Bairro'],row['Cidade'], row['Estado'], row['Arquivo_origem'],row['Tempo de Uso (min)'])    
    cursor.execute(sql, consolidado)
    conexao.commit()














