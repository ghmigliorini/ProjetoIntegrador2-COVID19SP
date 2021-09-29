#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  9 14:56:21 2021

@author: grupo_p
"""
import pandas as pd
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt
import psycopg2

###################################
#
# Postgres
#
###################################


#Vars postgres
banco='projetointegradorcovid'
usuario = 'utfpr'
senha = 'senhautfpr'
host='localhost'
porta='5432'


# Conexão
conn = psycopg2.connect(database=banco,
                        user=usuario, 
                        password=senha,
                        host=host,
                        port=porta)
cursor = conn.cursor()
print("Cursor OK!".center(70,"#"))


#############--------------div--------------#############

###################################
#
# constantes
#
###################################

color_dict = {'25-':'#ff595e',
              '25 a 39':'#ffca3a',
              '40 a 54':'#8ac926',
              '55 a 69':'#1982c4',
              '70 a 84':'#6a4c93',
              '85+':'#244530'}
cats = ['25-', '25 a 39', '40 a 54',
        '55 a 69', '70 a 84', '85+']

#############--------------div--------------#############

###################################
#
# Funções
#
###################################

def sql_casos_cidades(lista_cidades):
    
    if len(lista_cidades) > 0:
        
        sql = '''
        select 
         	paciente as casos,
         	data_inicio_sintomas as data,
         	obito,
        	nome as cidade
        from 
         	caso
         	join paciente on caso.codigo = paciente.codigo
         	join municipio on paciente.municipio = municipio.ibge
        where
        	diagnostico = 'CONFIRMADO' and 
        	nome in ('''
        
        for c in lista_cidades:
            sql+='\''+c+'\','
        
        # Tirar ultima virgula
        sql = sql[:-1]
        sql += ') order by ibge, data_inicio_sintomas;'
    
    else:
        sql = '''
        select 
         	paciente as casos,
         	data_inicio_sintomas as data,
         	obito,
        	nome as cidade
        from 
         	caso
         	join paciente on caso.codigo = paciente.codigo
         	join municipio on paciente.municipio = municipio.ibge
        where
        	diagnostico = 'CONFIRMADO';
        '''
    
    return sql

def sql_isolamento_cidades(lista_cidades):
         
    sql = '''
    select 
     	ibge,
        nome as cidade,
        data_isolamento as data,
        media_isolamento as media
    from
     	isolamento
        join municipio on isolamento.municipio = municipio.ibge
    where
    	nome in ('''
    
    for c in lista_cidades:
        sql+='\''+c+'\','
    
    # Tirar ultima virgula
    sql = sql[:-1]
    sql += ') order by cidade, data'
    
    return sql


def num_casos(lista_cidades,media_movel=7,suavizar=3):

    sql = sql_casos_cidades(lista_cidades)
    dfCid = pd.read_sql(sql, conn)
        
    data_unique = set(dfCid.data.unique())
    dfCasoCid = pd.DataFrame({'data':pd.to_datetime(list(data_unique), format='%Y-%m-%d')})
    dfCasoCid = dfCasoCid.sort_values(by='data')
    dfCasoCid.reset_index(drop=True, inplace=True)
        
    dfCasos = dfCid.groupby(['cidade','data']).count()
    dfCasos.reset_index(inplace=True)
    dfCasos['data'] = pd.to_datetime(dfCasos['data'], format='%Y-%m-%d')
        
    for c in dfCasos.cidade.unique():
        aux_ca = dfCasos[dfCasos.cidade == c].copy()
        aux_ca = aux_ca.rename(columns={'casos':c})
            
        dfCasoCid = pd.merge(dfCasoCid,
                             aux_ca[['data',c]],
                             on='data',
                             how='left')
        
    dfCasoCid = dfCasoCid.set_index('data')
        
    for i in range(suavizar):
        dfCasoCid = dfCasoCid.rolling(media_movel,1).mean()
    
    return dfCasoCid.copy()


def num_obtos(lista_cidades, media_movel=7, suavizar=3):

    sql = sql_casos_cidades(lista_cidades)
    dfCid = pd.read_sql(sql, conn)
    
    data_unique = set(dfCid.data)
    dfObtCid = pd.DataFrame({'data':pd.to_datetime(list(data_unique), format='%Y-%m-%d')})
    dfObtCid = dfObtCid.sort_values(by='data')
    dfObtCid.reset_index(drop=True, inplace=True)
    
    
    dfObitos = dfCid[dfCid.obito == True].groupby(['cidade','data']).count()
    dfObitos.reset_index(inplace=True)
    dfObitos['data'] = pd.to_datetime(dfObitos['data'], format='%Y-%m-%d')
    
    for c in dfObitos.cidade.unique():
        aux_ob = dfObitos[dfObitos.cidade == c].copy()
        aux_ob = aux_ob.rename(columns={'casos':c})
        
        dfObtCid = pd.merge(dfObtCid,
                            aux_ob[['data',c]],
                            on='data',
                            how='left')
    
    dfObtCid = dfObtCid.set_index('data')
        
    for i in range(suavizar):
        dfObtCid = dfObtCid.rolling(media_movel,1).mean()
    
    return dfObtCid.copy()



def taxa_mort(lista_cidades, media_movel=7, suavizar=3, cumulativo=True):

    sql = sql_casos_cidades(lista_cidades)
    dfCid = pd.read_sql(sql, conn)
    
    data_unique = set(dfCid.data)
    dfTaxa = pd.DataFrame({'data':pd.to_datetime(list(data_unique), format='%Y-%m-%d')})
    dfTaxa = dfTaxa.sort_values(by='data')
    dfTaxa.reset_index(drop=True, inplace=True)
    
    dfCasos = dfCid.groupby(['cidade','data']).count()
    dfCasos.reset_index(inplace=True)
    dfCasos['data'] = pd.to_datetime(dfCasos['data'], format='%Y-%m-%d')
    
    dfObitos = dfCid[dfCid.obito == True].groupby(['cidade','data']).count()
    dfObitos.reset_index(inplace=True)
    dfObitos['data'] = pd.to_datetime(dfObitos['data'], format='%Y-%m-%d')
    
    for c in dfCasos.cidade.unique():
        aux_ca = dfCasos[dfCasos.cidade == c].copy()
        aux_ob = dfObitos[dfObitos.cidade == c].copy()
        
        
        aux_ca = aux_ca.rename(columns={'casos':c+'_casos'})
        aux_ob = aux_ob.rename(columns={'casos':c+'_obitos'})
        
        if cumulativo:
            aux_ca[c+'_casos'] = aux_ca[c+'_casos'].cumsum()
            aux_ob[c+'_obitos'] = aux_ob[c+'_obitos'].cumsum()
            
        
        dfTaxa = pd.merge(dfTaxa,
                          aux_ca[['data',c+'_casos']],
                          on='data',
                          how='left')
    
        dfTaxa = pd.merge(dfTaxa,
                          aux_ob[['data',c+'_obitos']],
                          on='data',
                          how='left')
    
    dfTaxa = dfTaxa.set_index('data')
        
    dfTaxa = dfTaxa.rolling(media_movel,1).mean()
    
    for c in lista_cidades:
        dfTaxa[c] = 100*(dfTaxa[c+'_obitos']/dfTaxa[c+'_casos'])
    
    dfTaxa = dfTaxa[lista_cidades]
    
    for i in range(suavizar):
        dfTaxa = dfTaxa.rolling(media_movel,1).mean()
    
    return dfTaxa.copy()


def iso_cidades(lista_cidades,media_movel=7,suavizar=3):

    sql = sql_isolamento_cidades(lista_cidades)
    dfIso = pd.read_sql(sql, conn)
    
    data_unique = set(dfIso.data)
    dfIsoCid = pd.DataFrame({'data':pd.to_datetime(list(data_unique), format='%Y-%m-%d')})
    dfIsoCid = dfIsoCid.sort_values(by='data')
    dfIsoCid.reset_index(drop=True, inplace=True)
    
    dfIso['data'] = pd.to_datetime(dfIso['data'], format='%Y-%m-%d')
    
    for cid in dfIso.cidade.unique():
        aux = dfIso[dfIso.cidade == cid].copy()
        
        aux = aux.rename(columns={'media':cid})
        
        dfIsoCid = pd.merge(dfIsoCid,
                            aux[['data',cid]],
                            on='data',
                            how='left')
    
    dfIsoCid = dfIsoCid.set_index('data')
    
    for i in range(suavizar):
            dfIsoCid = dfIsoCid.rolling(media_movel).mean()
    
    return dfIsoCid.copy()

def recuo(dataframe, n=15):
    aux = dataframe.copy()
    for col in aux.columns:
        aux[col] = list(aux[col])[n:] + [np.nan for i in range(n)]
    
    return aux

def classifica_idade(n):
    '''
    Classifica a faixa etária da idade n

    Parametros
    ----------
    n : int
        inteiro atribuído a idade.

    Retorna
    -------
    faixa : str
        Faixa etária atribuída.

    '''
    
    if n < 25:
        return '25-'
    if n < 40:
        return '25 a 39'
    if n < 55:
        return '40 a 54'
    if n < 70:
        return '55 a 69'
    if n < 85:
        return '70 a 84'
    if n >= 85:
        return '85+'
    
    





def classifica_idade2(n):
    '''
    Classifica a faixa etária da idade n

    Parametros
    ----------
    n : int
        inteiro atribuído a idade.

    Retorna
    -------
    faixa : str
        Faixa etária atribuída.

    '''
    # if n < 18:
    #     return 'menor 18'
    # if n < 20:
    #     return '18 a 19'
    # if n < 25:
    #     return '20 a 24'
    # if n < 30:
    #     return '25 a 29'
    # if n < 35:
    #     return '30 a 34'
    # if n < 40:
    #     return '35 a 39'
    # if n < 45:
    #     return '40 a 44'
    # if n < 50:
    #     return '45 a 49'
    # if n < 55:
    #     return '50 a 54'
    # if n < 60:
    #     return '55 a 59'
    # if n < 65:
    #     return '60 a 64'
    # if n < 70:
    #     return '65 a 69'
    # if n < 75:
    #     return '70 a 74'
    # if n < 80:
    #     return '75 a 79'
    # if n < 85:
    #     return '80 a 84'
    # if n < 90:
    #     return '85 a 89'
    # if n >= 90:
    #     return '90+'

    if n < 20:
        return 'menor 20'
    if n < 30:
        return '20 a 29'
    if n < 40:
        return '30 a 39'
    if n < 50:
        return '40 a 49'
    if n < 60:
        return '50 a 59'
    if n < 70:
        return '60 a 69'
    if n < 80:
        return '70 a 79'
    if n < 90:
        return '80 a 89'
    if n >= 90:
        return '90+'
    
    # if n < 25:
    #     return 'menor 25'
    # if n < 40:
    #     return '25 a 39'
    # if n < 55:
    #     return '40 a 54'
    # if n < 70:
    #     return '55 a 69'
    # if n < 85:
    #     return '70 a 84'
    # if n >= 85:
    #     return '85+'



def plot_obito_idade(dataframe, nome='obitos_idade_vacina.png', dpi=300):
    fig, ax = plt.subplots(1,1, figsize=(18,10))
    for cat in reversed(cats):
        ax.plot(dataframe.index, dataframe[cat],
                label=cat,
                color=color_dict[cat])
    
    shadow_col = 'white'
    
    # 85+ 
    plt.axvline(pd.to_datetime('2021-01-29'), color=color_dict['85+'])
    plt.text(pd.to_datetime('2021-01-29'), 300, 'vacina\n85+',
             color=color_dict['85+'], ha='right', va='center')
    # 70 a 84
    plt.axvline(pd.to_datetime('2021-02-8'), color=color_dict['70 a 84'])
    plt.text(pd.to_datetime('2021-02-8'), 250, 'vacina\n[70 a 84]',
             color=color_dict['70 a 84'], ha='left', va='center')
    
    # 55 a 69
    plt.axvline(pd.to_datetime('2021-04-14'), color=color_dict['55 a 69'])
    plt.text(pd.to_datetime('2021-04-14'), 300, 'vacina\n[55 a 69]',
             color=color_dict['55 a 69'], ha='left', va='center')
    
    # 40 a 54
    plt.axvline(pd.to_datetime('2021-06-15'), color=color_dict['40 a 54'])
    plt.text(pd.to_datetime('2021-06-15'), 300, 'vacina\n[40 a 54]',
             color=color_dict['40 a 54'], ha='right', va='center')
    
    # 70 a 84 morre menos que 55 a 69
    plt.plot(pd.to_datetime('2021-02-18'),96.402,'ro', color=color_dict['70 a 84'])
    # Sombra
    plt.text(pd.to_datetime('2021-02-18'), 83, '[70 a 84] < [55 a 69]',
             color=shadow_col, ha='center', va='center')
    plt.text(pd.to_datetime('2021-02-18'), 84, '[70 a 84] < [55 a 69]',
             color=color_dict['70 a 84'], ha='center', va='center')
    
    # 70 a 84 morre menos que 40 a 54
    plt.plot(pd.to_datetime('2021-04-29'),87.60,'ro', color=color_dict['70 a 84'])
    # Sombra
    plt.text(pd.to_datetime('2021-04-29'), 74, '[70 a 84] < [40 a 54]',
             color=shadow_col, ha='center', va='center')
    plt.text(pd.to_datetime('2021-04-29'), 75, '[70 a 84] < [40 a 54]',
             color=color_dict['70 a 84'], ha='center', va='center')
    
    # 85+ morre menos que 40 a 54
    plt.plot(pd.to_datetime('2021-02-10'),32.82,'ro', color=color_dict['85+'])
    # Sombra
    plt.text(pd.to_datetime('2021-02-10'), 19, '85+ < [40 a 54]',
              color=shadow_col, ha='center', va='center')
    plt.text(pd.to_datetime('2021-02-10'), 20, '85+ < [40 a 54]',
             color=color_dict['85+'], ha='center', va='center')
    
    # 85+ morre menos que 25 a 39
    plt.plot(pd.to_datetime('2021-03-29'),52,'ro', color=color_dict['85+'])
    # Sombra
    plt.text(pd.to_datetime('2021-03-29'), 39, '85+ < [25 a 39]',
              color=shadow_col, ha='center', va='center')
    plt.text(pd.to_datetime('2021-03-29'), 40, '85+ < [25 a 39]',
             color=color_dict['85+'], ha='center', va='center')
    
    ax.legend()
    ax.set_ylabel("Média de óbitos")
    ax.set_xlabel("tempo em dias de 01-01-2021 a 15-06-2021")
    plt.title("Média Móvel (7 dias) de óbitos por intervalo de idade")
    fig.savefig(nome, dpi=dpi)


def plot_tx_obito_idade(dataframe, nome='tx_obitos_idade_vacina.png', dpi=300):
    fig, ax = plt.subplots(1,1, figsize=(18,10))
    for cat in reversed(cats):
        ax.plot(dataframe.index, dataframe[cat],
                label=cat,
                color=color_dict[cat])
    
    shadow_col = 'white'
    
    # 85+ 
    plt.axvline(pd.to_datetime('2021-01-29'), color=color_dict['85+'])
    plt.text(pd.to_datetime('2021-01-29'), 40, 'vacina\n85+',
             color=color_dict['85+'], ha='center', va='center')
    # 70 a 84
    plt.axvline(pd.to_datetime('2021-02-8'), color=color_dict['70 a 84'])
    plt.text(pd.to_datetime('2021-02-8'), 30, 'vacina\n[70 a 84]',
             color=color_dict['70 a 84'], ha='center', va='center')
    
    # 55 a 69
    plt.axvline(pd.to_datetime('2021-04-14'), color=color_dict['55 a 69'])
    plt.text(pd.to_datetime('2021-04-14'), 40, 'vacina\n[55 a 69]',
             color=color_dict['55 a 69'], ha='center', va='center')
    
    # 40 a 54
    plt.axvline(pd.to_datetime('2021-06-15'), color=color_dict['40 a 54'])
    plt.text(pd.to_datetime('2021-06-15'), 40, 'vacina\n[40 a 54]',
             color=color_dict['40 a 54'], ha='right', va='center')
    
    # # 70 a 84 morre menos que 55 a 69
    # plt.plot(pd.to_datetime('2021-02-26'),112.235,'ro', color=color_dict['70 a 84'])
    # # Sombra
    # plt.text(pd.to_datetime('2021-02-26'), 99, '[70 a 84] < [55 a 69]',
    #          color=shadow_col, ha='center', va='center')
    # plt.text(pd.to_datetime('2021-02-26'), 100, '[70 a 84] < [55 a 69]',
    #          color=color_dict['70 a 84'], ha='center', va='center')
    
    # # 70 a 84 morre menos que 40 a 54
    # plt.plot(pd.to_datetime('2021-05-05'),94.98,'ro', color=color_dict['70 a 84'])
    # # Sombra
    # plt.text(pd.to_datetime('2021-05-05'), 86, '[70 a 84] < [40 a 54]',
    #          color=shadow_col, ha='center', va='center')
    # plt.text(pd.to_datetime('2021-05-05'), 87, '[70 a 84] < [40 a 54]',
    #          color=color_dict['70 a 84'], ha='center', va='center')
    
    # # 85+ morre menos que 40 a 54
    # plt.plot(pd.to_datetime('2021-02-15'),33,'ro', color=color_dict['85+'])
    # # Sombra
    # plt.text(pd.to_datetime('2021-02-15'), 23, '85+ < [40 a 54]',
    #           color=shadow_col, ha='center', va='center')
    # plt.text(pd.to_datetime('2021-02-15'), 24, '85+ < [40 a 54]',
    #          color=color_dict['85+'], ha='center', va='center')
    
    # # 85+ morre menos que 25 a 39
    # plt.plot(pd.to_datetime('2021-04-11'),43,'ro', color=color_dict['85+'])
    # # Sombra
    # plt.text(pd.to_datetime('2021-04-11'), 32, '85+ < [25 a 39]',
    #           color=shadow_col, ha='center', va='center')
    # plt.text(pd.to_datetime('2021-04-11'), 33, '85+ < [25 a 39]',
    #          color=color_dict['85+'], ha='center', va='center')
    
    ax.legend()
    ax.set_ylabel("Média de óbitos")
    ax.set_xlabel("tempo em dias de 01-01-2021 a 15-06-2021")
    plt.title("Taxa de mortalidade de óbitos por intervalo de idade")
    fig.savefig(nome, dpi=dpi)


#############--------------div--------------#############

###################################
#
# SQLs
#
###################################
sql_a_idade_obito = '''
select
	paciente,
	idade,
	data_inicio_sintomas
from 
	caso join
	paciente on
		caso.paciente = paciente.codigo
where
	diagnostico = 'CONFIRMADO' AND
	obito = true
order by 
	data_inicio_sintomas, idade;
'''

sql_b_idade_obito_taxa = '''
select
	paciente,
	idade,
	data_inicio_sintomas,
	obito
from 
	caso join
	paciente on
		caso.paciente = paciente.codigo
where
	diagnostico = 'CONFIRMADO'
order by 
	data_inicio_sintomas, idade;
'''

sql_idd_comorb = '''
select 
	p.codigo as paciente,
	idade,
	descricao as comorbidade
from 
	paciente p join 
	comorbidade_paciente cp 
	on p.codigo = cp.paciente join
	comorbidade cm
	on cp.comorbidade = cm.codigo
where
    idade > 0;
'''