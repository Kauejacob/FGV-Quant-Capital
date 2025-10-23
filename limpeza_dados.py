# Arquivo deve ser utilizado para a criação da base primaria e para puxar dados externo de outras bases.

#--------------------------------------------------------------------------

#Bibliotecas
import pandas as pd
import re #Funções regulares, ou seja, deixar um nome regular
from functools import reduce

#-------------------------------------------------------------------------

#Importando a base de dados
df = pd.read_excel("C:/Users/alvar/Documentos/FGV/QUANT CAPITAL/economatica_inovador.xlsx", skiprows=3) #Usei o skiprows, para que se pulasse 3 linhas do excel, objetivando que o título com os nomes bonitinho aparecesse
df.tail()

#-------------------------------------------------------------------------

                                                              #OBSERVAÇÃO
                                             
#Usamos o preço Máximo e Mínimo das ações como sendo não ajustados.
    #Isso foi feito, pois estamos trabalhando em um período de 1.5 ano, ou seja, aproximadamente, 548 dias. 
        #Esse período é considerado como sendo de médio prazo. Então, o fechamento_ajustado é recomendado, mas não necessário 
    #O fechamento_ajustado DEVE ser usado em períodos longos e NÃO DEVE ser usado para períodos curtos
    #Em período médio ele é RECOMENDADO, mas NÃO NECESSÁRIO 

#-------------------------------------------------------------------------
#Arrumando o nome de todas as colunas e agrupando por empresa (OBJETIVO PRINIPAL)

    #Queremos deixar os nomes das colunas padronizados e mais objetivos, claros
    #A função a seguir serve para renomear colunas, trocando os nomes grandes da economática (separados por espaços) por nomes no formato <tipo_da_informação>_<nome_da_empresa> (separação por underscore para facilitar funções futuras) 
    
    
    #explicação linha por linha:
    #col.strip() remove espaços em branco no início e no final da string.
    #.split(" ") divide a string sempre que houver dois ou mais espaços consecutivos
    #partes = [p.strip() for p in partes if p.strip()] Como há muitos espaços no nome da coluna, essa função faz com que os espaços extras sejam removidos. Como a função de cima quebrou esses espaços extras em strings, o if p.strip() assegura que somente strings com algo escrito permanecerão no nome, o que remove espaços adicionais. (se a string é vazia, ela é False e por isso é removida, já que o if mantém apenas o que é True)
    #Como "partes" em geral é formada por duas strings (o nome da informação e o nome da empresa), se há mais de 1 string (len(partes) > 1 ), a última (`partes[-1]`) é o nome da empresa. Caso contrário, a empresa não foi identificada, e empresa = ''.
    # A mesma lógica é usada com as informações, que consistem na primeira string. Por isso, tipo_info = partes[0] if len(partes) > 1 else partes[0]
    # A função re.sub() usa 3 argumentos: 1º - O que você deseja substituir, 2º - pelo que, 3º - em qual texto. r'\s+' indica um ou mais espaços, que serão substituidos por "_", ou seja, os espaços serão trocados por underscores nas strings. 
    # A função armazena o nome da coluna como {tipo_info}_{empresa}, o objetivo inicial descrito anteriormente

def renomear_coluna(col):
    
    partes = col.strip().split("  ")
    partes = [p.strip() for p in partes if p.strip()]
    
    empresa = partes[-1] if len(partes) > 1 else ''
    tipo_info = partes[0] if len(partes) > 1 else partes[0]
    
       
    tipo_info = re.sub(r'\s+', '_', tipo_info)
    empresa = re.sub(r'\s+', '_', empresa)
    
    return f"{tipo_info}_{empresa}"

        # Aplicando a função
df.columns = [renomear_coluna(col) for col in df.columns]

        # Visualizando as novas colunas
print(df.columns)

        #Vendo se no dataframe funcionou 
df.head(10)


    #Deixando o nome mais objetivo, uma vez que ele ainda está muito longo, com muitos espaços e caracteres desnecessários
    #A função replace substituirá os nomes longos padrão da economática pelos nomes escolhidos abaixo, além de remover dois underscores e caracteres especiais.

def limpar_nome(col):
    col = col.strip()

        # Substituições específicas
    col = col.replace("não_aj_p/_prov", "nao_ajustado")
    col = col.replace("ajust_p/_prov", "ajustado")
    col = col.replace("Volume$_Em_moeda_orig_em_milhares", "Volume_em_milhar")
    col = col.replace("Em_moeda_orig", "")  # remover se sobrar
    col = col.replace("__", "_")  # dois underlines seguidos
    col = re.sub(r"_+$", "", col)  # remove underscore no final
    col = col.replace("Máximo", "Maximo") #Tirando o sinal
    col = col.replace("Mínimo", "Minimo") #Tirando o sinal

    return col

        # Aplicando a função
df.columns = [limpar_nome(col) for col in df.columns]

        #Vendo o resultado na base de dados
df.head(10)

#-------------------------------------------------------------------------

#Precisamos tirar as 5 colunas que não possuem nenhuma empresa no nome. Isso ficou visível quando se abriu a planilha do excel.
#Isso ocorre, pois quando se importou os dados do economatica, a empresa que estava aparecendo na parte superior da tela teve os seus dados postos na primeira coluna de cada informação 
#Por isso que está sem nome, porque no economatica, meio que está explícito a qual ação estamos nos referindo

    #Aparentemente, não existe mais esse problema. Mas, esse trecho de código continua aqui, por segurança 
df = df.drop(columns=['Fechamento_nao_ajustado', 'Fechamento_ajustado', 'Volume_em_milhar', 'Minimo_nao_ajustado', 'Maximo_nao_ajustado']) 
df

#Como sabemos os nomes dessas colunas, apenas retiramos elas especificamente, ao invés de termos que fazer uma função para retirá-las

#-------------------------------------------------------------------------

    #Queremos agora que todos os dados da mesma empresa permaneçam juntos, ou seja, que as linhas sejam agrupadas por empresa
        #Lista com os tipos de dados esperados
tipos_dado = ['Fechamento_nao_ajustado', 'Fechamento_ajustado', 'Volume_em_milhar', 'Minimo_nao_ajustado', 'Maximo_nao_ajustado']

        #Função para extrair o tipo de dado com base no início da string
def extrair_tipo(col):
    for tipo in tipos_dado:
        if col.startswith(tipo):
            return tipo
    return 'Outro'

        #Função para extrair o nome da empresa: tudo após o tipo
def extrair_empresa(col):
    tipo = extrair_tipo(col)
    if tipo != 'Outro':
        return col[len(tipo)+1:]  # +1 para pular o underline
    return col

        #Montar dicionário: empresa -> colunas
empresa_dict = {}
for col in df.columns:
    empresa = extrair_empresa(col)
    empresa_dict.setdefault(empresa, []).append(col)

        #Reordenar as colunas agrupadas por empresa
colunas_ordenadas = []
for empresa, cols in empresa_dict.items():
    # Ordena tipo Fechamento primeiro, depois Volume
    cols.sort(key=lambda x: (0 if 'Fechamento' in x else 1, x))
    colunas_ordenadas.extend(cols)

        #Reordenar o DataFrame
df = df[colunas_ordenadas]

        #Visualizar resultado
print(df.columns)

        #Vendo o resultado na base de dados
df.head(10)

primeiras_6_colunas = df.iloc[:, :6]
print("\nPrimeiras 6 colunas:\n", primeiras_6_colunas)

        # Exportar para Excel, visando visualizar o resultado das modificações acima 
#df.to_excel(r"C:/Users/alvar/Documentos/FGV/QUANT CAPITAL/dados_reorganizados.xlsx", index=False)

#--------------------------------------------------------------------------

#Agora, retiraremos da base de dados, todas as ações que possuem NA em mais de ou igual a 50% dos dados, ou seja, que não possuem dados suficientes para serem analisadas
    #OBS: OLHAREMOS APENAS PARA UMA COLUNA, POIS SE UMA TEM DADO AS OUTRAS DUAS TAMBÉM E, VICE-VERSA

    #Calculando o percentual de NA em todas as linhas de Fechamento_ajustado 
    # Passo 1: Identificar as colunas de "fechamento ajustado"
cols_fechamento_ajustado = [col for col in df.columns if col.startswith('Fechamento_ajustado')]

    # Passo 2: Verificar valores vazios (considerando "-" como vazio, além de strings vazias, None e NaN)
df_vazio = df[cols_fechamento_ajustado].apply(lambda x: (x == '') | (x.isna()) | (x == None) | (x == '-'))

    # Passo 3: Calcular percentual de dados vazios por coluna
percentuais_vazio = df_vazio.mean() * 100

    # Passo 4: Filtrar as colunas com mais de 50% de dados vazios
colunas_com_mais_de_50_vazio = percentuais_vazio[percentuais_vazio >= 50].index
print(f"Total de colunas com >= 50% de valores vazios: {len(colunas_com_mais_de_50_vazio)}") #Vemos aqui que isso ocorre com 84 linhas de fechamento ajustado

    # Passo 5: Para cada uma dessas colunas, pegar ela e as 4 colunas à direita
colunas_a_remover = set()  # usamos set para evitar duplicatas
colunas_lista = list(df.columns)

for col in colunas_com_mais_de_50_vazio:
    try:
        idx = colunas_lista.index(col)
        colunas_a_remover.update(colunas_lista[idx:idx+5])  #col atual + 4 seguintes. 
#OBS: se tivesse mais dados para cada ação, aí é so aumentar o número de colunas que deleta a direita. Nesse caso, a direita temos, apenas, fechamento não ajustado e volume (em milhar)
    except IndexError:
        pass  # evita erro caso estejamos no final do DataFrame

    # Passo 6: Remover essas colunas
df = df.drop(columns=colunas_a_remover)

    #Apenas conferindo se o número de colunas está certo e, realmente, está
        #Temos 5 colunas relacionadas a cada ação. Tiramos 84 colunas de fechamento ajustado de ações; então, 84*5 = 420 colunas
print(f"Total de colunas: {len(df.columns)}") 
print(f"Total de linhas: {len(df)}") 

#Depois dessa modificações, ficamos com 1461 colunas, ou seja, 292 empresas e 1 coluna referente a data

#--------------------------------------------------------------------------

#Agora, removeremos as linhas que não contém informações, que correspondem a datas de feriados e emendas 
import numpy as np

# 1. Substituir '-' por NaN
df.replace('-', np.nan, inplace=True)

# 2. Identificar a coluna de data (geralmente a primeira)
coluna_data = df.columns[0]

# 3. Converter a coluna de data para datetime
df[coluna_data] = pd.to_datetime(df[coluna_data], errors='coerce')

# 4. Remover linhas onde todos os dados (exceto a data) são NaN
linhas_vazias = df.drop(columns=coluna_data).isna().all(axis=1)
df = df[~linhas_vazias].copy()

#--------------------------------------------------------------------------

#Agora, devemos remover colunas com 3 ou mais linhas seguidas vazias, para isso: 
def remover_colunas_com_3_nans_consecutivos(df):
    colunas_para_remover = []

    for col in df.columns:
        contador = 0
        for val in df[col]:
            if pd.isna(val):
                contador += 1
                if contador >= 3:
                    colunas_para_remover.append(col)
                    break  # já atingiu 3 seguidos, não precisa continuar
            else:
                contador = 0  # zera se interrompe sequência de NaN

    return df.drop(columns=colunas_para_remover)
df = remover_colunas_com_3_nans_consecutivos(df)
df
    #Visualizando no Excel
#df.to_excel(r"C:/Users/alvar/Documentos/FGV/QUANT CAPITAL/base_limpa_final.xlsx", index=False)

    #O código acima me disse que tenho 1326 colunas, mas uma é a data. Então, na verdade, eu tenho 1325 colunas. Cada empresa possui 5 colunas, então eu tenho que ter 265 empresas, para que esteja certo 
        #O código abaixo vai extamento fazer essa contagem de emporesas, ou seja, verá se, realmente, temos 265 empresas.

    # Prefixos conhecidos
prefixos = [
    'Fechamento_ajustado',
    'Fechamento_nao_ajustado',
    'Minimo_nao_ajustado',
    'Maximo_nao_ajustado',
    'Volume_em_milhar'
]

    # Conjunto para guardar nomes únicos
nomes_empresas = set()

for coluna in df.columns:
    nome_empresa = None
    for prefixo in prefixos:
        if coluna.startswith(prefixo + '_'):
            nome_empresa = coluna[len(prefixo) + 1:]  # Pega tudo depois do prefixo + "_"
            break  # Parou no primeiro prefixo que bater

    if nome_empresa:  # Se achou nome de empresa
        nomes_empresas.add(nome_empresa)

print(f'Você tem {len(nomes_empresas)} empresas diferentes.')
print(sorted(nomes_empresas)) #Nome de todas as empresas

#--------------------------------------------------------------------------

#Agora queremos preencher os últimos dados faltantes, os quais são bem específicos
#Iremos preencher eles da seguinte maneira, caso seja uma célula vazia, faremos a média do dia anterior e posterior 
#Caso sejam duas células vazias consecutivas, faremos a média da 1° e 4° célula e inputaremos na 2° célula, depois faremos a média da 2° e 4° célula e inputaremos na 3° célula

def preencher_com_media_vizinha(df):
    df = df.copy()

    for col_idx in range(df.shape[1]):
        i = 0
        while i < len(df):
            if pd.isna(df.iloc[i, col_idx]):
                # Procurar valor acima (mais próximo)
                acima = None
                for j in range(i - 1, -1, -1):
                    if not pd.isna(df.iloc[j, col_idx]):
                        acima = df.iloc[j, col_idx]
                        break

                # Procurar valor abaixo (mais próximo)
                abaixo = None
                for j in range(i + 1, len(df)):
                    if not pd.isna(df.iloc[j, col_idx]):
                        abaixo = df.iloc[j, col_idx]
                        break

                # Lógica de preenchimento
                if acima is not None and abaixo is not None:
                    df.iloc[i, col_idx] = (acima + abaixo) / 2
                elif acima is not None:
                    df.iloc[i, col_idx] = acima
                elif abaixo is not None:
                    df.iloc[i, col_idx] = abaixo
            i += 1

    return df


# Aplicar a limpeza nos dados (mantendo a coluna de data intacta)
df.iloc[:, 1:] = preencher_com_media_vizinha(df.iloc[:, 1:])

#df.to_excel(r"C:/Users/alvar/Documentos/FGV/QUANT CAPITAL/blabla.xlsx", index=False)

# Verifica se existe algum NaN na base
faltantes = df.isna().any().any()

if faltantes:
    print("⚠️ Existem dados faltantes na base de dados.")
else:
    print("✅ Nenhum dado faltante  foi encontrado.")

#--------------------------------------------------------------------------

#Montando a base de dados com a Data, Ticker, Retorno Diário, Volume de Transação Diário, Fechamento não Ajustado, Máximo, Mínimo 

    #Pegando a primeira coluna da base,que representa a data
coluna_data = df.columns[0]

    #Separando duas listas de colunas da base que são usadas para poder calcular o retorno diário
colunas_fechamento = [col for col in df.columns if col.startswith("Fechamento_ajustado")]
colunas_volume = [col for col in df.columns if col.startswith("Volume_em_milhar")]
colunas_maximo = [col for col in df.columns if col.startswith("Maximo")]
colunas_minimo = [col for col in df.columns if col.startswith("Minimo")]

    #Transformação da base de formato wide para formato long tanto para o fechamento ajustado quanto para o volume 
df_fechamento = df[[coluna_data] + colunas_fechamento].melt(
    id_vars=[coluna_data], #esse parâmetro será mantido constante, as outras colunas seão transformadas em duas colunas: nome da variável e valor
    var_name="coluna", #o nome da nova coluna que vai conter o nome da variável original
    value_name="fechamento_ajustado" #o nome da nova coluna que vai conter os valores numéricos que estavam na coluna original 
)
df_fechamento["Ticker"] = df_fechamento["coluna"].str.replace("Fechamento_ajustado_", "", regex=False)
#regex=False, indica que estamos usando expressões regulares, ou seja, apenas susbtituições literais 


df_volume = df[[coluna_data] + colunas_volume].melt(
    id_vars=[coluna_data],
    var_name="coluna",
    value_name="volume"
)
df_volume["Ticker"] = df_volume["coluna"].str.replace("Volume_em_milhar_", "", regex=False)


df_maximo = df[[coluna_data] + colunas_maximo].melt(
    id_vars=[coluna_data],
    var_name="coluna",
    value_name="maximo"
)
df_maximo["Ticker"] = df_maximo["coluna"].str.replace("Maximo_nao_ajustado_","", regex=False)


df_minimo = df[[coluna_data] + colunas_minimo].melt(
    id_vars=[coluna_data],
    var_name="coluna",
    value_name="minimo"
)
df_minimo["Ticker"] = df_minimo["coluna"].str.replace("Minimo_nao_ajustado_","", regex=False)


    #Juntando o volume e o fechamento ajustado em uma tabela
df_merged = [
    df_fechamento[[coluna_data, "Ticker", "fechamento_ajustado"]],
    df_volume[[coluna_data, "Ticker", "volume"]],
    df_maximo[[coluna_data, "Ticker", "maximo"]],
    df_minimo[[coluna_data, "Ticker", "minimo"]],
]

df_merged = reduce(lambda left, right: pd.merge(left, right, on=[coluna_data, "Ticker"]), df_merged)
#Usamos a função reduce(), pois a função merge() só aceita duas colunas, dataframes por vez. A função reduce() faz o trabalho da fução merge(), quantas vezes forem necessárias, ou seja, de 2 em 2. Assim, não temos que escrever a função merge() várias vezes 
#O lambda é uma função anônima para que seja possível fazer os merges. As colunas Data e Ticker são usadas como base
    #left = representa o dataframe acumulado até o momento 
    #right = representa o próximo dataframe da lista df_merged
    #pd.merge(left, right, on=[coluna_data, "Ticker"]) = faz sucessivos merges, com base nas colunas Data e Ticker

    #Organizacao dos dados por ticker e data
df_merged = df_merged.sort_values(by=["Ticker", coluna_data])

    # Calculo do retorno diário
df_merged["Retorno_Diario"] = df_merged.groupby("Ticker")["fechamento_ajustado"].diff()

    # Formatacao da tabela final
df_final = df_merged[[coluna_data, "Ticker", "fechamento_ajustado", "Retorno_Diario", "volume", "maximo", "minimo"]]
df_final.columns = ["Data", "Ticker", "Adj_close", "Daily_return", "Volume", "High", "Low"]

df_final.head()

    # Confirmar se está certo o retorno 
df_final[df_final["Ticker"] == "3tentos_ON"].head(10)

    #Exportacao da base final 
df_final.to_excel(r"C:/Users/alvar/Documentos/FGV/QUANT CAPITAL/base_primaria_inovador.xlsx", index=False)



