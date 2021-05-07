options(scipen=999)
#library(tidyverse)
ano = 2021
opcao = T
while (opcao){
tabelas = c('tb_enturmacao','tb_ultimaenturmacao','tb_turmadisciplina')
print('Tabelas:')
print(tabelas)
#tabelas=c('tb_turma')
for(tabela in tabelas){
source('../conexoes/dbi_astim_seduc.R')
df <- dbGetQuery(con,paste0(
"select * from academico.",tabela," te 
where nr_anoletivo = ",ano,"
and exists (select 1 from academico.tb_turma tt 
                      where tt.nr_anoletivo = ",ano," 
                      and ci_turma = cd_turma 
                      and tt.cd_prefeitura = 0)
"))
df$dt_carga = Sys.time()
source('../conexoes/dbi_astim_bi.R')
dbWriteTable(con, SQL(paste0('dl_sige.',tabela,'_',ano)),df,overwrite = T) #escrever dados
print(paste0(tabela,' inserida com sucesso!'))
rm(df)
gc(reset=T)
}

tabelas = c('tb_ofertaitens','tb_turma')
print('Tabelas:')
print(tabelas)
for(tabela in tabelas){
  source('../conexoes/dbi_astim_seduc.R')
  df <- dbGetQuery(con,paste0(
    "select * from academico.",tabela," df
    where nr_anoletivo = ",ano,"
    and df.cd_prefeitura = 0
    "))
  df$dt_carga = Sys.time()
  source('../conexoes/dbi_astim_bi.R')
  dbWriteTable(con, SQL(paste0('dl_sige.',tabela,'_',ano)),df,overwrite = T) #escrever dados
  print(paste0(tabela,' inserida com sucesso!'))
  rm(df)
  gc(reset=T)
}

print('Tabela: tb_movimento, tb_resultado')
tabela ='tb_movimento' 
source('../conexoes/dbi_astim_seduc.R')
  df <- dbGetQuery(con,paste0(
    "select * from academico.",tabela," df
    where nr_anoletivo = ",ano,"
    and exists (select 1 from rede_fisica.tb_unidade_trabalho tut 
                      where tut.ci_unidade_trabalho = df.cd_unidade_trabalho_destino 
                      and tut.cd_dependencia_administrativa < 3)
      "))
  df$dt_carga = Sys.time()
  source('../conexoes/dbi_astim_bi.R')
  dbWriteTable(con, SQL(paste0('dl_sige.',tabela,'_',ano)),df,overwrite = T) #escrever dados
  print(paste0(tabela,' inserida com sucesso!'))
  rm(df)
  gc(reset=T)

  tabela ='tb_resultado' 
  source('../conexoes/dbi_astim_seduc.R')
  df <- dbGetQuery(con,paste0(
    "select * from academico.",tabela," df
   where nr_anoletivo = ",ano-1,"
    and exists (select 1 from academico.tb_turma tt 
      where tt.nr_anoletivo = ",ano-1," 
      and ci_turma = cd_turma 
      and tt.cd_prefeitura = 0)
    "))
  df$dt_carga = Sys.time()
  source('../conexoes/dbi_astim_bi.R')
  dbWriteTable(con, SQL(paste0('dl_sige.',tabela,'_',ano-1)),df,overwrite = T) #escrever dados
  print(paste0(tabela,' inserida com sucesso!'))
  rm(df)
  gc(reset=T)

  
tabelas = c('tb_ambiente','tb_categoria','tb_local_funcionamento','tb_local_unid_trab','tb_tipo_ambiente','tb_unidade_trabalho','tb_localizacao_zona',
                          'tb_curso','tb_disciplinas','tb_etapa','tb_grupodisciplina','tb_tiporesultado',
            'tb_bairros','tb_coordenadas_unid_trab','tb_localidades','tb_subcategoria','tb_aluno_cancelamento','tb_aluno_deficiencia',
                          'tb_aluno_ies','tb_tipo_ingresso','tb_tipo_ies','tb_aluno_enem',
                           'tb_curso_formacao_superior','tb_ies','tb_tpmovimento','tb_situacao','tb_municipio_censo')
esquemas = c('rede_fisica','rede_fisica','rede_fisica','rede_fisica','rede_fisica','rede_fisica','rede_fisica',
                        'academico','academico','academico','academico','academico',
                         'util','util','util','util','academico','academico'
                          ,'enem','enem','enem','enem',
                           'educacenso_exp','educacenso_exp','academico','academico','util')
print('Tabelas:')
print(tabelas)
for(i in c(1:length(tabelas))){
  source('../conexoes/dbi_astim_seduc.R')
  df <- dbGetQuery(con,paste0("select * from ",esquemas[i],"." ,tabelas[i]))
  df$dt_carga = Sys.time()
  source('../conexoes/dbi_astim_bi.R')
  dbWriteTable(con, SQL(paste0('dl_sige.',tabelas[i])),df,overwrite = T) #escrever dados
  print(paste0(tabelas[i],' inserida com sucesso!'))
  rm(df)
  gc(reset=T)
}
opcao = F
}

#Criaçao de chaves primarias:





#Dataframes longos
source('../conexoes/dbi_astim_seduc.R')
source('sql/sql_tb_aluno.R')
df <- dbGetQuery(con,query)
df$dt_carga = Sys.time()
dbWriteTable(con, SQL('dl_sige.tb_aluno'),df,overwrite = T) #escrever dados
print('tb_aluno inserida com sucesso!')
#Do enem, importante é: enem.tb_aluno_ies, enem.tb_tipo_ingresso, enem.tb_tipo_ies, enem.tb_aluno_enem
#E as tabelas do schema educacenso_exp: educacenso_exp.tb_curso_formacao_superior, educacenso_exp.tb_ies
