#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 11:38:24 2021

@author: grupo_p
"""

import pandas as pd
import numpy as np
import scipy.stats
from funcoes import *

import matplotlib.pyplot as plt

plt.style.use('ggplot')
# from datetime import datetime


data_fim = pd.to_datetime('2021-06-15', format='%Y-%m-%d')

data_inicio = pd.to_datetime('2021-01-01', format='%Y-%m-%d')

suavizar = 3
mm_tam = 7

# sumario
# 1 - Número absoluto de óbitos
# 2 - Taxa de Letalidade
# 3 - Araquara
# 4 - Serrana
# 5 - Scatter plot
# 6 - Barras comorbidade

#plotar = [1,2,3,4,5,6]
plotar = [1,3,4,5,6]

cumulativo = True



###################################
#
# Número absoluto de óbitos
#
###################################

if 1 in plotar or 2 in plotar:
    
    # Baixar pacientes que vieram a obto e suas idades 
    dfObtIdd = pd.read_sql(sql_a_idade_obito, conn)
    
    # Classificar com uma faixa
    dfObtIdd['Faixa'] = dfObtIdd.idade.apply(classifica_idade)
    
    # Agrupar pela faixa
    dfObtIdd = dfObtIdd.groupby(['Faixa','data_inicio_sintomas']).count()
    dfObtIdd.reset_index(inplace=True)
    
    
    # Gerar DataFrame linhas => Data, Colunas mortes por faixa estária
    data_unique = dfObtIdd.data_inicio_sintomas.unique()
    dfObtIdd_plot = pd.DataFrame({'data':data_unique})
    dfObtIdd_plot = dfObtIdd_plot.sort_values(by='data')
    
    for f in dfObtIdd.Faixa.unique():
        
        # SubFrame por faixa
        aux = dfObtIdd[dfObtIdd.Faixa == f].copy()
        aux = aux.rename(columns={'data_inicio_sintomas':'data',
                                  'paciente':f})
        
        dfObtIdd_plot = pd.merge(dfObtIdd_plot,
                                 aux[['data',f]],
                                 on='data',
                                 how='left')
    
    # Usar data como indice
    dfObtIdd_plot.set_index('data', inplace=True)
    # Gerar média móvel mensal (7 dias)
    for i in range(suavizar):
        dfObtIdd_plot = dfObtIdd_plot.rolling(mm_tam,1).mean()
    
    
    # for col in dfObtIdd_plot.columns:
    #     dfObtIdd_plot[col] = media_movel(dfObtIdd_plot[col],30)
    
    mask = (dfObtIdd_plot.index >= data_inicio) & (dfObtIdd_plot.index <= data_fim)
    
    dfObtIdd_plot = dfObtIdd_plot[mask]
    
    plot_obito_idade(dfObtIdd_plot)
    print("Grafico 01",
          "Média móvel de óbitos por intervalo de idade",
          "\tOK!\n",
          "Descrição dos dados:",
          sep="\n")
    
    
    print(dfObtIdd_plot.describe().to_markdown(tablefmt="psql"))
    print("\n")

###################################
#
# Taxa de Mortalidade
#
###################################

if 2 in plotar:
#if True:
    
    # Baixar pacientes que vieram a obto e suas idades
    dfObtTaxa = pd.read_sql(sql_b_idade_obito_taxa, conn)
    # Classificar com uma faixa
    dfObtTaxa['Faixa'] = dfObtTaxa.idade.apply(classifica_idade)
    
    # Agrupar pela faixa
    dfObtTaxa = dfObtTaxa.groupby(['Faixa','data_inicio_sintomas']).count()
    dfObtTaxa.reset_index(inplace=True)
    
    # Gerar DataFrame linhas => Data, Colunas mortes por faixa estária
    data_unique = dfObtTaxa.data_inicio_sintomas.unique()
    dfObtTaxa_plot = pd.DataFrame({'data':data_unique})
    dfObtTaxa_plot = dfObtTaxa_plot.sort_values(by='data')
    
    for f in dfObtTaxa.Faixa.unique():
        
        # SubFrame por faixa
        aux = dfObtTaxa[dfObtTaxa.Faixa == f].copy()
        aux = aux.rename(columns={'data_inicio_sintomas':'data',
                                  'paciente':f+' casos'})
        if cumulativo:
            #aux[f+' casos'] = aux[f+' casos'].rolling(7, min_periods=1).sum()
            aux[f+' casos'] = aux[f+' casos'].cumsum()
        
        dfObtTaxa_plot = pd.merge(dfObtTaxa_plot,
                                  aux[['data',f+' casos']],
                                  on='data',
                                  how='left')
        
    for f in dfObtIdd.Faixa.unique():
        
        # SubFrame por faixa
        aux = dfObtIdd[dfObtIdd.Faixa == f].copy()
        aux = aux.rename(columns={'data_inicio_sintomas':'data',
                                  'paciente':f+' obitos'})
        if cumulativo:
            #aux[f+' obitos'] = aux[f+' obitos'].rolling(7, min_periods=1).sum()
            aux[f+' obitos'] = aux[f+' obitos'].cumsum()
        
        dfObtTaxa_plot = pd.merge(dfObtTaxa_plot,
                                  aux[['data',f+' obitos']],
                                  on='data',
                                  how='left')
        
    # Usar data como indice
    dfObtTaxa_plot.set_index('data', inplace=True)
    
    
    
    for col in cats:
        dfObtTaxa_plot[col] = 100*(dfObtTaxa_plot[col+' obitos']/dfObtTaxa_plot[col+' casos'])
    
    dfObtTaxa_plot = dfObtTaxa_plot[cats]
    
    for i in range(suavizar):
        dfObtTaxa_plot = dfObtTaxa_plot.rolling(mm_tam,1).mean()
    
    
    mask = (dfObtTaxa_plot.index >= data_inicio) & (dfObtTaxa_plot.index <= data_fim)
    dfObtTaxa_plot = dfObtTaxa_plot[mask]
    
    #dfObtTaxa_plot.plot()
    plot_tx_obito_idade(dfObtTaxa_plot)
    print("Grafico 02",
          "Taxa de mortalidade por intervalo de idade",
          "\tOK!\n",
          "Descrição dos dados:",
          sep="\n")
    
    
    print(dfObtTaxa_plot.describe().to_markdown(tablefmt="psql"))
    print("\n")

###################################
#
# Araquara
#
###################################

if 3 in plotar:
#if True:

    data_fim_ar = pd.to_datetime('2021-06-15', format='%Y-%m-%d')
    
    data_inicio_ar = pd.to_datetime('2020-11-01', format='%Y-%m-%d')
    
    clr_cont = '#66DE93'
    #clr_pont = '#FF616D'
    clr_pont = '#54436B'
    
    # Pegando Araraquara e cidades semelhantes casos e isolamentos
    
    alvos = ['ITAPEVI', 'HORTOLÂNDIA', 'AMERICANA','ARARAQUARA','JACAREÍ']
    
    dfCasos = num_casos(alvos)
    mask = (dfCasos.index >= data_inicio_ar) & (dfCasos.index <= data_fim_ar)
    dfCasos = dfCasos[mask]
    
    dfIso = iso_cidades(alvos)
    mask = (dfIso.index >= data_inicio_ar) & (dfIso.index <= data_fim_ar)
    dfIso = dfIso[mask]
    
    dfObt = num_obtos(alvos)
    mask = (dfObt.index >= data_inicio_ar) & (dfObt.index <= data_fim_ar)
    dfObt = dfObt[mask]
    
    CasosR = recuo(dfCasos, 15)
    ObitoR = recuo(dfObt,15)
    
    fig, axes = plt.subplots(3,3, sharex=True, sharey='row', figsize=(20,10))
    
    font_size = 13
    
    # ARARAQUARA
    col = 0
    cidade = 'ARARAQUARA'
    # Isolamento
    axes[0][col].plot(dfIso.index,dfIso[cidade], color=clr_cont)
    axes[0][col].set_title('Isolamento em '+cidade.capitalize())
    axes[0][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    axes[0][col].text(pd.to_datetime('2020-12-25'), 44, 'Natal',
                      color='r', ha='right', va='center', size=font_size)
    
    axes[0][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[0][col].text(pd.to_datetime('2021-02-21'), 46, 'Lockdown',
                      color='black', ha='right', va='center', size=font_size)
    axes[0][col].set_ylabel("isolamento em %")
    
    # Casos
    axes[1][col].plot(dfCasos.index,dfCasos[cidade], color=clr_cont)
    axes[1][col].plot(CasosR.index,CasosR[cidade], linestyle=':',
                      label='Recuo 15 dias',color=clr_pont)
    axes[1][col].set_title('Média Casos em '+cidade.capitalize())
    axes[1][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    axes[1][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[1][col].set_ylabel("número de casos")
    axes[1][col].legend()
    
    # Obitos
    axes[2][col].plot(dfObt.index,dfObt[cidade], color=clr_cont)
    axes[2][col].plot(ObitoR.index,ObitoR[cidade], linestyle=':', 
                      label='Recuo 15 dias',color=clr_pont)
    axes[2][col].set_title('Média Óbitos em '+cidade.capitalize())
    axes[2][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    axes[2][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[2][col].set_ylabel("número de óbitos")
    axes[2][col].legend()
    
    # AMERICANA
    col = 1
    cidade = 'AMERICANA'
    # Isolamento
    axes[0][col].plot(dfIso.index,dfIso[cidade], color=clr_cont)
    axes[0][col].set_title('Isolamento em '+cidade.capitalize())
    axes[0][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    axes[0][col].text(pd.to_datetime('2020-12-25'), 44, 'Natal',
                      color='r', ha='right', va='center', size=font_size)
    
    # axes[0][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    # axes[0][col].text(pd.to_datetime('2021-02-21'), 46, 'Lockdown',
    #                   color='black', ha='right', va='center', size=font_size)
    
    # Casos
    axes[1][col].plot(dfCasos.index,dfCasos[cidade], color=clr_cont)
    axes[1][col].plot(CasosR.index,CasosR[cidade], linestyle=':',
                      label='Recuo 15 dias',color=clr_pont)
    axes[1][col].set_title('Média Casos em '+cidade.capitalize())
    axes[1][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    #axes[1][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[1][col].legend()
    
    # Obitos
    axes[2][col].plot(dfObt.index,dfObt[cidade], color=clr_cont)
    axes[2][col].plot(ObitoR.index,ObitoR[cidade], linestyle=':', 
                      label='Recuo 15 dias',color=clr_pont)
    axes[2][col].set_title('Média Óbitos em '+cidade.capitalize())
    axes[2][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    #axes[2][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[2][col].legend()
    
    
    
    # HORTOLÂNDIA
    col = 2
    cidade = 'HORTOLÂNDIA'
    # Isolamento
    axes[0][col].plot(dfIso.index,dfIso[cidade], color=clr_cont)
    axes[0][col].set_title('Isolamento em '+cidade.capitalize())
    axes[0][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    axes[0][col].text(pd.to_datetime('2020-12-25'), 44, 'Natal',
                      color='r', ha='right', va='center', size=font_size)
    
    # axes[0][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    # axes[0][col].text(pd.to_datetime('2021-02-21'), 46, 'Lockdown',
    #                   color='black', ha='right', va='center', size=font_size)
    
    # Casos
    axes[1][col].plot(dfCasos.index,dfCasos[cidade], color=clr_cont)
    axes[1][col].plot(CasosR.index,CasosR[cidade], linestyle=':', 
                      label='Recuo 15 dias',color=clr_pont)
    axes[1][col].set_title('Média Casos em '+cidade.capitalize())
    axes[1][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    #axes[1][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[1][col].legend()
    
    # Obitos
    axes[2][col].plot(dfObt.index,dfObt[cidade], color=clr_cont)
    axes[2][col].plot(ObitoR.index,ObitoR[cidade], linestyle=':', 
                      label='Recuo 15 dias',color=clr_pont)
    axes[2][col].set_title('Média Óbitos em '+cidade.capitalize())
    axes[2][col].axvline(pd.to_datetime('2020-12-25'), color='r')
    #axes[2][col].axvline(pd.to_datetime('2021-02-21'), color='black')
    axes[2][col].legend()
    
    for i in range(3):
        axes[2][i].tick_params(axis='x', rotation=90)
    
    plt.suptitle('Isolamento, casos e óbtos em Araraquara, Americana e Hortolândia')
    
    fig.savefig('araraquara.png', dpi=300)
    
    print("Grafico 03",
          "Similares a Araraquara (Lockdown)",
          "\tOK!\n",
          sep="\n")

    
    print("ISOLAMENTOS")
    print(dfIso.describe().to_markdown(tablefmt="psql"))
    print("\n\n")
    print("*********".center(70))
    print("\n\n")
    print("ÓBITOS")
    print(dfObt.describe().to_markdown(tablefmt="psql"))
    print("\n\n")
    print("*********".center(70))
    print("\n\n")
    print("CASOS")
    print(dfCasos.describe().to_markdown(tablefmt="psql"))
    print("*********".center(70))
    print("\n\n")

###################################
#
# Serrana
#
###################################

if 4 in plotar:
#if True:
    
    alvos = ['SERRANA', 'JARDINÓPOLIS', 'GARÇA']
    
    #data_inicio = pd.to_datetime('2020-04-16')
    
    dfCasoS = num_casos(alvos)
    mask = (dfCasoS.index >= data_inicio) & (dfCasoS.index <= data_fim)
    dfCasoS = dfCasoS[mask]
    
    dfTMS = taxa_mort(alvos)
    mask = (dfTMS.index >= data_inicio) & (dfTMS.index <= data_fim)
    dfTMS = dfTMS[mask]
    
    
    fig, (ax1, ax2) = plt.subplots(2,1, sharex=True, figsize=(15,10))
        
    font_size = 13
    
    cor_cidade = {'SERRANA':'#1b9e77',
                  'JARDINÓPOLIS':'#d95f02',
                  'GARÇA':'#7570b3'}
    
    sty_cidade = {'SERRANA':'-',
                  'JARDINÓPOLIS':'--',
                  'GARÇA':'-.'}
    
    for c in alvos:
        ax1.plot(dfCasoS.index,
                 dfCasoS[c],
                 color=cor_cidade[c],
                 linestyle=sty_cidade[c],
                 label=c.capitalize())
        
        ax2.plot(dfTMS.index,
                 dfTMS[c],
                 color=cor_cidade[c],
                 linestyle=sty_cidade[c],
                 label=c.capitalize())
    
    # Anotações acima
    ax1.set_title("Média Móvel de casos por dia em 2021")
    ax1.set_ylabel("casos confirmados")
    
    ax1.axvline(pd.to_datetime('2021-02-14'), color='#1b9e77')
    ax1.text(pd.to_datetime('2021-02-14'), 30, 'Inicio\nProjeto S',
             color='black', ha='right', va='center', size=font_size)
    
    ax1.axvline(pd.to_datetime('2021-04-10'), color='#1b9e77')
    ax1.text(pd.to_datetime('2021-04-10'), 30, 'Termino\nVacinação',
             color='black', ha='left', va='center', size=font_size)
    
    # Anotações abaixo
    ax2.set_title("Taxa de letalidade (cumulativo de casos e óbitos) em 2021")
    ax2.set_ylabel("letalidade %")
    
    ax2.axvline(pd.to_datetime('2021-02-14'), color='#1b9e77')
    ax2.text(pd.to_datetime('2021-02-14'), 3.8, 'Inicio\nProjeto S',
             color='black', ha='right', va='center', size=font_size)
    
    ax2.axvline(pd.to_datetime('2021-04-10'), color='#1b9e77')
    ax2.text(pd.to_datetime('2021-04-10'), 3.8, 'Termino\nVacinação',
             color='black', ha='left', va='center', size=font_size)
    
    
    ax1.legend()
    ax2.legend()
    plt.suptitle("Projeto S  - Vacinação em Serrana-SP e comparação com  cidades similares")
    fig.savefig('serrana.png', dpi=300)
    print("Grafico 04",
          "Similares a Serrana (Projeto S)",
          "\tOK!\n")
    print("CASOS")
    print(dfCasoS.describe().to_markdown(tablefmt="psql"))
    print("\n\n")
    print("*********".center(70))
    print("\n\n")
    print("letalidade")
    print(dfTMS.describe().to_markdown(tablefmt="psql"))
    
    inicio_s = pd.to_datetime('2021-02-14')

    
    teste_t = scipy.stats.ttest_ind(dfTMS[dfTMS.index >= inicio_s].dropna()["SERRANA"],
                                    dfTMS[dfTMS.index >= inicio_s].dropna()["GARÇA"])
    
    if teste_t.pvalue < 0.05:
        print("P valor:",teste_t.pvalue)
        print("Rejeito Igualdade entre as médias de letalidade de SERRANA e GARÇA")
        print("médias de letalidade após projeto S diferente")
    else:
        print("P valor:",teste_t.pvalue)
        print("Rejeito Igualdade entre as médias de letalidade de SERRANA e GARÇA")
        print("médias de letalidade após projeto S diferente")
        
    
###################################
#
# Scatter Idade, letalidade
#
###################################
if 5 in plotar:
#if True:
    
    dfObtIdd = pd.read_sql(sql_b_idade_obito_taxa, conn)
    CasosCont = dfObtIdd.groupby('idade').count()
    CasosCont.reset_index(inplace=True)
    CasosCont = CasosCont[CasosCont.idade != 0]
    
    ObtCont = dfObtIdd[dfObtIdd.obito == True].groupby('idade').count()
    ObtCont.reset_index(inplace=True)
    ObtCont = ObtCont[ObtCont.idade != 0]
    
    CasosCont = CasosCont.set_index('idade')
    ObtCont = ObtCont.set_index('idade')
    
    dfIdTx = 100*(ObtCont.paciente/CasosCont.paciente)
    dfIdTx = pd.DataFrame(dfIdTx)
    dfIdTx = dfIdTx.dropna()
    dfIdTx.reset_index(inplace=True)
    
    dfIdTx = dfIdTx.rename(columns={'paciente':'letalidade'})
    
    #Linha de tendencia
    t_line = np.polyfit(dfIdTx.letalidade, dfIdTx.idade,1)
    f = np.poly1d(t_line)
    r_x = np.arange(dfIdTx.letalidade.min(), dfIdTx.letalidade.max(), 0.1)
    
    fig, ax = plt.subplots(1,1,figsize=(12,7))
    ax.scatter(dfIdTx.letalidade, dfIdTx.idade, 
               color='#66DE93', alpha=0.75)
    
    
    ax.plot(r_x, f(r_x), label="aproximação linear da relação",
            color='#FF616D', alpha=0.85)
    
    ax.set_ylabel('Idade em anos')
    ax.set_xlabel('Taxa de letalidade em %')
    ax.set_title('Relação entre idade e taxa de letalidade')
    
    ax.legend(loc='lower right')
    
    fig.savefig('scatter.png', dpi = 300)
    
    print("Grafico 05, OK")
    print("Descrição")
    print(dfIdTx.describe().to_markdown(tablefmt="psql"))
    print("Correlação Pearson")
    cp = scipy.stats.pearsonr(dfIdTx.idade,dfIdTx.letalidade)
    print("correl: "+str(round(cp[0],3)))
    print("p-valor: "+str(cp[1]))
    print("Correlação Spearman")
    cp = scipy.stats.spearmanr(dfIdTx.idade,dfIdTx.letalidade)
    print("correl: "+str(round(cp[0],3)))
    print("p-valor: "+str(cp[1]))

###################################
#
# Barra Idade, Comorbidade
#
###################################

if 6 in plotar:
#if True:
    
    dfConsulta = pd.read_sql(sql_idd_comorb, conn)
    dfConsulta['faixa'] = dfConsulta.idade.apply(classifica_idade)
    dfIddCM = dfConsulta.groupby(['comorbidade','faixa']).count()
    dfIddCM = dfIddCM.sort_values(by=['faixa','paciente'], ascending=[True,False])
    dfIddCM.reset_index(inplace=True)
    
    fig, axes = plt.subplots(5,1,sharey=True,sharex=True,figsize=(15,12))
    for i in range(5):
        axes[i].bar(dfIddCM[dfIddCM.faixa == cats[i]]['comorbidade'],
                    dfIddCM[dfIddCM.faixa == cats[i]]['paciente'],
                    color=color_dict[cats[i]],
                    label=cats[i])
        axes[i].set_ylabel("número de pacientes")
        axes[i].legend()
    plt.tick_params(axis='x', rotation=45)
    
    fig.suptitle('Prevalência de comorbidade por faixa etária', fontsize=16)
    fig.savefig('bar.png', dpi = 300)
    
    print("Grafico 06, OK")

# Fechar conexão
conn.close()
