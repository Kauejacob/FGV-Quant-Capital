# FGV Quant Capital - ETL Pairs Trading

# Autor: Hugo Villanova

# Data: 26/07/2025

# Objetivo: Código destinado a estruturação da estratégia de pairs trading

# 1. BIBLIOTECAS ----------------------------------------------------------------------------------------------------

# Importando bibliotecas

# Manipulação e análise de dados
import pandas as pd
import numpy as np

# Estatística e séries temporais
from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm
from scipy.spatial.distance import euclidean

# Visualização
import matplotlib.pyplot as plt
import seaborn as sns

# Leitura de arquivos Excel (necessário para xlsx)
import openpyxl

# Barra de progresso (opcional, mas ajuda muito em loops grandes)
from tqdm import tqdm

# 2. DOWNLOAD DAS BASES ----------------------------------------------------------------------------------------------------

# === CDI ===
# Lendo a partir da linha 39 (ou seja, skiprows=38 porque o primeiro índice é 0)
# Selecionando apenas as colunas 1 e 5 (em Python, índice começa em 0, então são as colunas 0 e 4)
cdi = pd.read_excel(
    "CDI_FGV_Quant_Capital.xlsx",
    skiprows=38,
    usecols=[0, 4],      # 0 = Data, 4 = Fator Diário
    engine='openpyxl'
)
cdi.columns = ['Data', 'Fator_DI']  # Renomeando para facilitar

# === Preço de Fechamento Ajustado ===
# Lendo a partir da linha 4 (skiprows=3), ignorando a segunda coluna
pfa = pd.read_excel(
    "PFA_FGV_Quant_Capital.xlsx",
    skiprows=3,
    engine='openpyxl'
)
pfa = pfa.drop(pfa.columns[1], axis=1)   # Apagando a segunda coluna

# === Volume Negociado ===
# Mesmo esquema do PFA
vol = pd.read_excel(
    "VOL_FGV_Quant_Capital.xlsx",
    skiprows=3,
    engine='openpyxl'
)
vol = vol.drop(vol.columns[1], axis=1)   # Apagando a segunda coluna

# 3. PREPARAÇÃO DAS BASES ----------------------------------------------------------------------------------------------------

# Função para limpar as bases de PFA e VOL
def clean_bases(df):
    # Ajusta a coluna Data para datetime
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
    # Renomeia colunas pegando só o ticker
    df.columns = ['Data'] + [col.split('\n')[-1] for col in df.columns[1:]]
    # Converte todas as colunas, exceto "Data", para numérico
    cols_ticker = df.columns[1:]
    df[cols_ticker] = df[cols_ticker].apply(pd.to_numeric, errors='coerce')
    return df

# CDI
cdi['Data'] = pd.to_datetime(cdi['Data'], errors='coerce')
cdi['Fator_DI'] = pd.to_numeric(cdi['Fator_DI'], errors='coerce')

# PFA e VOL usando função única
pfa = clean_bases(pfa)
vol = clean_bases(vol)

# Apagando linhas com NaN em todas as colunas (dias sem negociação)
# Para PFA
pfa.dropna(subset=pfa.columns[1:], how='all', inplace=True)
# Para VOL
vol.dropna(subset=vol.columns[1:], how='all', inplace=True)

# Apagando ações ilíquidas (pelo menos 1 dia sem negociação)
# Também tomando cuidado com ações com 0 no volume
def limpa_iliquidez(vol, pfa):
    """
    Troca zeros por NaN na base vol e filtra tickers ilíquidos.
    Mantém apenas colunas sem nenhum NaN (inclusive resultantes de zeros).
    Aplica o filtro também à base pfa.
    """
    vol = vol.copy()
    pfa = pfa.copy()
    # Troca zeros por NaN nas colunas de tickers (menos 'Data')
    vol.iloc[:, 1:] = vol.iloc[:, 1:].replace(0, np.nan)
    # Filtra colunas que não têm nenhum NA
    tickers = vol.columns[1:]
    cols_sem_na = [col for col in tickers if not vol[col].isna().any()]
    cols_para_manter = ['Data'] + cols_sem_na
    vol = vol[cols_para_manter]
    pfa = pfa[cols_para_manter]
    return vol, pfa

# Aplicando a limpeza de iliquidez:
vol, pfa = limpa_iliquidez(vol, pfa)

# Filtrando tickers de uma mesma empresa (selecionando com maior volume médio))
def seleciona_ticker_maior_volume(vol):
    """
    Recebe o DataFrame de volume negociado (coluna Data + tickers).
    Retorna lista dos tickers principais: maior volume médio por grupo de prefixo de 4 letras,
    e todos os tickers únicos.
    """
    tickers = vol.columns[1:]
    prefixos = {}
    for ticker in tickers:
        pref = ticker[:4]
        if pref not in prefixos:
            prefixos[pref] = []
        prefixos[pref].append(ticker)

    tickers_selecionados = []
    for pref, lista in prefixos.items():
        if len(lista) == 1:
            # Só um ticker, entra direto
            tickers_selecionados.append(lista[0])
        else:
            # Mais de um: pega o de maior volume médio
            medias = {t: vol[t].mean(skipna=True) for t in lista}
            ticker_top = max(medias, key=medias.get)
            tickers_selecionados.append(ticker_top)

    return tickers_selecionados

# Aplicando a seleção de tickers principais:
tickers_principais = seleciona_ticker_maior_volume(vol)

# Agora pode manter apenas esses na base de preços (e no próprio vol)
colunas_para_manter = ['Data'] + tickers_principais
vol = vol[colunas_para_manter]
pfa = pfa[colunas_para_manter]

# Calulando o retorno logarítmico diário das ações
def calc_log_returns(df):
    df_ret = df.copy()
    for col in df_ret.columns[1:]:
        df_ret[col] = np.log(df_ret[col] / df_ret[col].shift(1))
    df_ret = df_ret.iloc[1:].reset_index(drop=True)
    return df_ret

# Aplicando à base de preços
ret_acoes = calc_log_returns(pfa)

# Transformando os fatores diários do CDI em log-retornos
cdi['CDI'] = np.log(cdi['Fator_DI'])
cdi.drop(columns=['Fator_DI'], inplace=True)

# Criando as bases para os próximos passos
def monta_df_pt_ret(cdi, ret_acoes):
    """
    Une os retornos do CDI e das ações, baseando-se na coluna 'Data'.
    Saída: DataFrame com 'Data', 'ret_cdi' e retornos das ações.
    """
    # Seleciona apenas as colunas de interesse na base CDI
    cdi_sel = cdi[['Data', 'CDI']]
    # Faz o merge com a base de retornos das ações
    df_pt_ret = pd.merge(cdi_sel, ret_acoes, on='Data', how='inner')
    return df_pt_ret

# Usando a função:
df_pt_ret = monta_df_pt_ret(cdi, ret_acoes)

def retorna_acumulado(df_pt_ret):
    """Calcula o acumulado exponencial dos retornos logarítmicos (base 1)."""
    df_pt_acum = df_pt_ret.copy()
    cols_ret = df_pt_acum.columns[1:]  # Todas exceto Data
    df_pt_acum[cols_ret] = np.exp(df_pt_acum[cols_ret].cumsum())
    # Normaliza para começar exatamente em 1 na primeira linha
    df_pt_acum[cols_ret] = df_pt_acum[cols_ret] / df_pt_acum[cols_ret].iloc[0]
    return df_pt_acum

# Exemplo de uso:
df_pt_acum = retorna_acumulado(df_pt_ret)

# 4. DIVIDINDO FORMAÇÃO X NEGOCIAÇÃO ----------------------------------------------------------------------------------------------------

# Define os limites de datas
data_form_fim = pd.to_datetime('2025-06-30')
data_neg_inicio = pd.to_datetime('2025-07-01')

# Retornos
df_pt_ret_form = df_pt_ret[df_pt_ret['Data'] <= data_form_fim].reset_index(drop=True)
df_pt_ret_neg  = df_pt_ret[df_pt_ret['Data'] >= data_neg_inicio].reset_index(drop=True)

# Preços acumulados
df_pt_acum_form = df_pt_acum[df_pt_acum['Data'] <= data_form_fim].reset_index(drop=True)
df_pt_acum_neg  = df_pt_acum[df_pt_acum['Data'] >= data_neg_inicio].reset_index(drop=True)

# # Número de colunas
# print(f"Número de colunas form: {len(df_pt_ret_form.columns)}")
# print(f"Número de colunas neg: {len(df_pt_ret_neg.columns)}")


# 5. FILTRAGEM DE SÉRIES I(1) ----------------------------------------------------------------------------------------------------

def filtra_tickers_I1(df_pt_acum_form, df_pt_ret_form):
    """
    Filtra tickers I(1): preços acumulados não estacionários, retornos estacionários.
    Mantém CDI e tickers I(1) nas duas bases.
    """
    tickers = df_pt_acum_form.columns[1:]  # Ignora 'Data'
    tickers_I1 = []
    for ticker in tickers:
        if ticker == 'CDI':  # Pula CDI, só filtra ações
            continue
        # Teste ADF nos preços acumulados
        adf_p = adfuller(df_pt_acum_form[ticker].dropna())[1]
        # Teste ADF nos retornos
        adf_r = adfuller(df_pt_ret_form[ticker].dropna())[1]
        # Critério: preços não estacionários (p>0.05), retornos estacionários (p<0.05)
        if adf_p > 0.05 and adf_r < 0.05:
            tickers_I1.append(ticker)
    # Mantém 'Data' e 'CDI' + tickers I(1)
    cols_keep = ['Data', 'CDI'] + tickers_I1
    df_pt_acum_form_filtrado = df_pt_acum_form[cols_keep].copy()
    df_pt_ret_form_filtrado  = df_pt_ret_form[cols_keep].copy()
    return df_pt_acum_form_filtrado, df_pt_ret_form_filtrado

# Aplicando a filtragem:
df_pt_acum_form, df_pt_ret_form = filtra_tickers_I1(df_pt_acum_form, df_pt_ret_form)

# # Nomes das colunas
# print(df_pt_acum_form.columns)

# # Número de colunas
# print(f"Número de colunas: {len(df_pt_acum_form.columns)}")

# 6. FILTRAGEM PARES COINTEGRADOS ----------------------------------------------------------------------------------------------------

def testa_cointegracao(df, pvalue_thresh=0.01):
    """
    Testa cointegração (Engle-Granger) em todos os pares possíveis.
    Retorna DataFrame com pares cointegrados (ticker1, ticker2, pvalor, beta, alpha).
    """
    tickers = [col for col in df.columns if col not in ['Data', 'CDI']]
    resultados = []

    for i in range(len(tickers)):
        for j in range(i+1, len(tickers)):
            t1, t2 = tickers[i], tickers[j]
            # Regressão t1 ~ t2
            y = df[t1]
            x = sm.add_constant(df[t2])
            model = sm.OLS(y, x, missing='drop').fit()
            resid = model.resid.dropna()
            # Teste ADF nos resíduos
            pvalor = adfuller(resid)[1]
            if pvalor < pvalue_thresh:
                resultados.append({
                    'ticker1': t1,
                    'ticker2': t2,
                    'pvalor': pvalor,
                    'beta': model.params[t2],
                    'alpha': model.params['const']
                })

    # Retorna DataFrame dos pares cointegrados
    return pd.DataFrame(resultados)

# Aplicando o teste de cointegração:
pares_cointegrados = testa_cointegracao(df_pt_acum_form)

n_tickers = len([col for col in df_pt_acum_form.columns if col not in ['Data', 'CDI']])
total_pares = n_tickers * (n_tickers - 1) // 2

print(f"Total de pares testados: {total_pares}")
print(f"Pares cointegrados: {len(pares_cointegrados)}")
print(f"Percentual cointegrado: {100 * len(pares_cointegrados) / total_pares:.2f}%")

# 7. FILTRAGEM DISTÂNCIA EUCLIDIANA ----------------------------------------------------------------------------------------------------

# Crie uma lista para armazenar os pares com suas distâncias
distancias = []

# Use o DataFrame de preços acumulados do período de formação (df_pt_acum_form)
for _, row in pares_cointegrados.iterrows():
    t1, t2 = row['ticker1'], row['ticker2']
    serie1 = df_pt_acum_form[t1].values
    serie2 = df_pt_acum_form[t2].values
    dist = euclidean(serie1, serie2)
    distancias.append(dist)

# Adiciona a coluna 'distancia' ao DataFrame de pares cointegrados
pares_cointegrados['distancia'] = distancias

# Seleciona os 20 pares de menor distância
pares_finais = pares_cointegrados.sort_values('distancia').head(20).reset_index(drop=True)

# 8. PARÂMETROS PARA O PERÍODO DE NEGOCIAÇÃO ----------------------------------------------------------------------------------------------------

# Pega todos os tickers únicos dos pares finais
tickers_usados = set(pares_finais['ticker1']).union(set(pares_finais['ticker2']))

# Monta a lista de colunas para manter
colunas_para_manter = ['Data', 'CDI'] + list(tickers_usados)

# Filtra as bases completas, não só as de negociação!
df_pt_ret = df_pt_ret[colunas_para_manter].copy()
df_pt_acum = df_pt_acum[colunas_para_manter].copy()

# Define a lista de datas do período de negociação
datas_neg = df_pt_acum[df_pt_acum['Data'] >= pd.to_datetime('2025-07-01')]['Data'].values

# Lista para armazenar resultados
resultados = []

# Loop por data de negociação
for data in datas_neg:
    # Índice da data no DataFrame completo
    idx = df_pt_acum.index[df_pt_acum['Data'] == data][0]
    # Índices da janela de 252 dias imediatamente anteriores
    start_idx = idx - 249
    if start_idx < 0:
        continue  # Pula datas onde não há 252 dias de histórico
    janela = df_pt_acum.iloc[start_idx:idx]
    # Loop por cada par
    for _, row in pares_finais.iterrows():
        t1, t2 = row['ticker1'], row['ticker2']
        y = janela[t1]
        x = sm.add_constant(janela[t2])
        model = sm.OLS(y, x, missing='drop').fit()
        spread = y - (model.params['const'] + model.params[t2]*janela[t2])
        resultados.append({
            'Data': data,
            'ticker1': t1,
            'ticker2': t2,
            'alpha': model.params['const'],
            'beta': model.params[t2],
            'spread_mean': spread.mean(),
            'spread_std': spread.std()
        })

# Constrói o DataFrame final
param_neg = pd.DataFrame(resultados)

# 9. SPREADS PARA O PERÍODO DE NEGOCIAÇÃO ----------------------------------------------------------------------------------------------------

spread_neg = []

# Indexa df_pt_acum por Data para facilitar acesso rápido
df_pt_acum_indexado = df_pt_acum.set_index('Data')

for _, row in param_neg.iterrows():
    data = row['Data']
    t1 = row['ticker1']
    t2 = row['ticker2']
    alpha = row['alpha']
    beta = row['beta']
    spread_mean = row['spread_mean']
    spread_std = row['spread_std']
    
    # Preços dos tickers nesse dia
    preco_t1 = df_pt_acum_indexado.at[data, t1]
    preco_t2 = df_pt_acum_indexado.at[data, t2]
    
    # Calcula spread
    spread = preco_t1 - (alpha + beta * preco_t2)
    
    spread_neg.append({
        'Data': data,
        'ticker1': t1,
        'ticker2': t2,
        'spread': spread,
        'spread_mean': spread_mean,
        'spread_std': spread_std
    })

spread_neg = pd.DataFrame(spread_neg)

# 10. Z-SCORES PARA O PERÍODO DE NEGOCIAÇÃO ----------------------------------------------------------------------------------------------------

# Calcula z-score diário para cada par
z_scores_neg = spread_neg.copy()
z_scores_neg['z_score'] = (z_scores_neg['spread'] - z_scores_neg['spread_mean']) / z_scores_neg['spread_std']

# Mantém só as colunas solicitadas
z_scores_neg = z_scores_neg[['Data', 'ticker1', 'ticker2', 'z_score']]

# 11. DEFININDO AS ORDENS ----------------------------------------------------------------------------------------------------

# Inicializa as novas colunas com zeros
z_scores_neg['entrada_long'] = 0
z_scores_neg['entrada_short'] = 0
z_scores_neg['long'] = 0
z_scores_neg['short'] = 0
z_scores_neg['saida_long'] = 0
z_scores_neg['saida_short'] = 0

# Vamos processar par a par!
for (t1, t2), df_par in z_scores_neg.groupby(['ticker1', 'ticker2']):
    idxs = df_par.index
    n = len(df_par)
    # Flags para o estado da posição
    em_long = 0
    em_short = 0
    for i in range(n):
        idx = idxs[i]
        z = df_par.iloc[i]['z_score']
        
        # No primeiro dia, não pode estar posicionado
        if i == 0:
            z_scores_neg.at[idx, 'long'] = 0
            z_scores_neg.at[idx, 'short'] = 0
            em_long = 0
            em_short = 0
            continue
        
        # Pega o status do dia anterior
        idx_ant = idxs[i-1]
        long_ant = z_scores_neg.at[idx_ant, 'long']
        short_ant = z_scores_neg.at[idx_ant, 'short']
        entrada_long_ant = z_scores_neg.at[idx_ant, 'entrada_long']
        entrada_short_ant = z_scores_neg.at[idx_ant, 'entrada_short']
        saida_long_ant = z_scores_neg.at[idx_ant, 'saida_long']
        saida_short_ant = z_scores_neg.at[idx_ant, 'saida_short']

        # ENTRADA LONG
        if z <= -2 and long_ant == 0 and short_ant == 0 and entrada_long_ant == 0:
            z_scores_neg.at[idx, 'entrada_long'] = 1
            em_long = 0
            em_short = 0

        # ENTRADA SHORT
        elif z >= 2 and long_ant == 0 and short_ant == 0 and entrada_short_ant == 0:
            z_scores_neg.at[idx, 'entrada_short'] = 1
            em_short = 0
            em_long = 0

        # MANUTENÇÃO DE LONG
        if entrada_long_ant == 1 or long_ant == 1:
            em_long = 1

        # MANUTENÇÃO DE SHORT
        if entrada_short_ant == 1 or short_ant == 1:
            em_short = 1

        # SAÍDA LONG
        if z >= -0.5 and long_ant == 1 and saida_long_ant == 0:
            z_scores_neg.at[idx, 'saida_long'] = 1
            em_long = 1

        # SAÍDA SHORT
        if z <= 0.5 and short_ant == 1 and saida_short_ant == 0:
            z_scores_neg.at[idx, 'saida_short'] = 1
            em_short = 1

        # ENCERRAMENTO DE POSIÇÕES APÓS SAÍDA
        if saida_long_ant == 1:
            em_long = 0
        if saida_short_ant == 1:
            em_short = 0

        # Atualiza flags finais do dia
        z_scores_neg.at[idx, 'long'] = em_long
        z_scores_neg.at[idx, 'short'] = em_short

# Salvar em xlsx
z_scores_neg.to_excel("z_scores_neg.xlsx", index=False)

# 12. RETORNOS DIÁRIOS POR PAR ----------------------------------------------------------------------------------------------------

# Primeiro, indexa df_pt_ret_neg por Data (para acesso rápido)
df_pt_ret_neg_indexado = df_pt_ret_neg.set_index('Data')

# Função auxiliar para buscar os retornos na tabela
def pega_retorno(row, ticker_col):
    data = row['Data']
    ticker = row[ticker_col]
    return df_pt_ret_neg_indexado.at[data, ticker]

# Adiciona as colunas de retorno do ticker 1 e ticker 2
z_scores_neg['ret_ticker1'] = z_scores_neg.apply(lambda row: pega_retorno(row, 'ticker1'), axis=1)
z_scores_neg['ret_ticker2'] = z_scores_neg.apply(lambda row: pega_retorno(row, 'ticker2'), axis=1)
# Adiciona a coluna do CDI
z_scores_neg['ret_cdi'] = z_scores_neg.apply(lambda row: df_pt_ret_neg_indexado.at[row['Data'], 'CDI'], axis=1)

# CALCULANDO OS RETORNOS DO PARES
# Copia o DataFrame original
retornos_finais_neg = z_scores_neg.copy()

# Cria colunas de flags do dia anterior (shift por grupo de par)
for flag in ['entrada_long', 'entrada_short', 'saida_long', 'saida_short']:
    retornos_finais_neg[f'{flag}_ant'] = retornos_finais_neg.groupby(['ticker1', 'ticker2'])[flag].shift(1).fillna(0)

# Inicializa ret_par como NaN
retornos_finais_neg['ret_par'] = np.nan

# Função para determinar ret_par linha a linha
def calcula_ret_par(row):
    # Todas as flags do dia e do dia anterior
    el = row['entrada_long']
    es = row['entrada_short']
    l = row['long']
    s = row['short']
    sl = row['saida_long']
    ss = row['saida_short']
    el_ant = row['entrada_long_ant']
    es_ant = row['entrada_short_ant']
    sl_ant = row['saida_long_ant']
    ss_ant = row['saida_short_ant']
    r1 = row['ret_ticker1']
    r2 = row['ret_ticker2']
    rcdi = row['ret_cdi']

    # 1. Nenhuma posição ou flag ativada
    if el == 0 and es == 0 and l == 0 and s == 0 and sl == 0 and ss == 0:
        return rcdi
    
    # 2. Entrada em long/short (no dia do sinal)
    if el == 1 or es == 1:
        return rcdi

    # 3. Entrada realizada (no dia seguinte ao sinal, posição aberta e entrada no dia anterior)
    if l == 1 and el_ant == 1:
        return rcdi + r1 - r2 - 0.001
    if s == 1 and es_ant == 1:
        return rcdi - r1 + r2 - 0.001
    
    # 4. Saída realizada (no dia seguinte ao sinal, posição zerada)
    if sl_ant == 1 and l == 0:
        return rcdi - 0.001
    if ss_ant == 1 and s == 0:
        return rcdi - 0.001
    
    # 5. Saída sinalizada (ainda mantém a posição até o fim do dia)
    if sl == 1 and l == 1:
        return rcdi + r1 - r2
    if ss == 1 and s == 1:
        return rcdi - r1 + r2
    
    # 6. Manutenção normal de posição long/short
    if l == 1:
        return rcdi + r1 - r2
    if s == 1:
        return rcdi - r1 + r2

    # Fallback de segurança
    return rcdi

# Aplica a função linha a linha
retornos_finais_neg['ret_par'] = retornos_finais_neg.apply(calcula_ret_par, axis=1)

# Mantém apenas as colunas solicitadas
retornos_finais_neg = retornos_finais_neg[['Data', 'ticker1', 'ticker2', 'ret_ticker1', 'ret_ticker2', 'ret_cdi', 'ret_par']]

# 13. RETORNOS ACUMULADOS DIÁRIOS ----------------------------------------------------------------------------------------------------

# Copia apenas Data e CDI da base de retornos de negociação
retorno_acumulado_pt = df_pt_ret_neg[['Data', 'CDI']].copy()

# Calcula retorno acumulado do CDI (começando em 1)
retorno_acumulado_pt['cdi_acum'] = np.exp(retorno_acumulado_pt['CDI'].cumsum())

# Calcula média dos log-retornos dos pares para cada dia
# (retornos_finais_neg tem: Data, ticker1, ticker2, ret_par)
media_pt = retornos_finais_neg.groupby('Data')['ret_par'].mean().reset_index(name='ret_pt_medio')

# Junta média dos pares com o DataFrame principal
retorno_acumulado_pt = retorno_acumulado_pt.merge(media_pt, on='Data', how='left')

# Calcula acumulado dos retornos do portfólio de pairs trading (começando em 1)
retorno_acumulado_pt['pt_acum'] = np.exp(retorno_acumulado_pt['ret_pt_medio'].cumsum())

# Mantém apenas as 3 colunas finais
retorno_acumulado_pt = retorno_acumulado_pt[['Data', 'cdi_acum', 'pt_acum']]

# Salvar em xlsx
retorno_acumulado_pt.to_excel("retorno_acumulado_pt.xlsx", index=False)

# 14. VISUALIZAÇÃO DOS RESULTADOS ----------------------------------------------------------------------------------------------------

plt.figure(figsize=(16, 9))
plt.plot(retorno_acumulado_pt['Data'], retorno_acumulado_pt['cdi_acum'],
         label='CDI', color='#9361C2', linewidth=2)
plt.plot(retorno_acumulado_pt['Data'], retorno_acumulado_pt['pt_acum'],
         label='Pairs Trading', color='#220042', linewidth=2)

plt.xlabel('Data', fontsize=16)
plt.ylabel('Retorno Acumulado', fontsize=16)
plt.title('Retorno Acumulado: CDI vs Pairs Trading', fontsize=18)
plt.legend(fontsize=14)
plt.grid(alpha=0.3)
plt.gcf().set_facecolor('white')
plt.tight_layout()

# Obtendo os valores finais das colunas em percentual
cdi_final = (retorno_acumulado_pt['cdi_acum'].iloc[-1] - 1) * 100
retorno_final = (retorno_acumulado_pt['pt_acum'].iloc[-1] - 1) * 100

# Pega a última data
data_final = retorno_acumulado_pt['Data'].iloc[-1]

# Adicionando retângulos ao final das linhas (usa data_final, não index)
plt.text(data_final, retorno_acumulado_pt['cdi_acum'].iloc[-1],
         f"{cdi_final:.2f}%", fontsize=12, va='center',
         bbox=dict(facecolor='#9361C2', alpha=0.3, edgecolor='none'))

plt.text(data_final, retorno_acumulado_pt['pt_acum'].iloc[-1],
         f"{retorno_final:.2f}%", fontsize=12, va='center',
         bbox=dict(facecolor='#220042', alpha=0.3, edgecolor='none'), color='white')

plt.savefig('acumulado_cdi_vs_pairs.png', dpi=300, bbox_inches='tight', facecolor='white')

plt.show()
