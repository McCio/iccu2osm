#!/usr/bin/env python3
import polars as pl
import pandas as pd

contatti = pl.from_pandas(pd.read_xml('data/contatti.xml')).drop('denominazione', 'codice-sbn')
fondi_speciali = pl.from_pandas(pd.read_xml('data/fondi-speciali.xml')).drop('denominazione')
patrimonio = pl.from_pandas(pd.read_xml('data/patrimonio.xml')).drop('denominazione')
biblioteche = pl.read_json('data/biblioteche.json')
# biblioteche = biblioteche.select(pl.col('biblioteche').list.eval(pl.struct.unnest)).unnest('biblioteche')
biblioteche = biblioteche.drop('metadati').explode('biblioteche').unnest('biblioteche').unnest('codici-identificativi').unnest('denominazioni').rename({'isil': 'codice-isil', 'ufficiale': 'denominazione', 'precedenti': 'denominazioni-precedenti', 'alternative': 'denominazioni-alternative'}).drop('indirizzo')
territorio = pl.read_csv('data/territorio.csv', separator=';').drop('denominazione', 'codice-sbn')
tipologie = pl.read_csv('data/tipologie.csv', separator=';').drop('denominazione biblioteca').rename({'codice isil': 'codice-isil'})

def show_df(df):
  print(df.columns)
  print(df)

show_df(contatti)
show_df(fondi_speciali)
show_df(patrimonio)
show_df(biblioteche)
show_df(territorio)
show_df(tipologie)

print(pl.sql("select f.* from fondi_speciali f where trim(f.\"fondo-speciale\") != ''").collect())

complete = biblioteche.join(contatti, on='codice-isil', how='left', validate='1:1', coalesce=True)
complete = complete.join(fondi_speciali, on='codice-isil', how='left', validate='1:1', coalesce=True)
complete = complete.join(patrimonio, on='codice-isil', how='left', validate='1:1', coalesce=True)
complete = complete.join(territorio, on='codice-isil', how='left', validate='1:1', coalesce=True)
#complete = complete.join(tipologie, on='codice-isil', how='left', validate='1:1', coalesce=True)
reserved_access = pl.col('accesso').struct.field('riservato')
wheelchair_access = pl.col('accesso').struct.field('portatori-handicap')
complete = complete.with_columns(
  pl.when(reserved_access == True).then(pl.lit('permit')).when(reserved_access == False).then(pl.lit('yes')).alias('access'),
  pl.when(wheelchair_access == True).then(pl.lit('yes')).when(wheelchair_access == False).then(pl.lit('no')).alias('wheelchair'),
).drop('accesso')
show_df(complete)

print(pl.sql("select indirizzo, contatti, contatto, telefono, fax, email, url from complete").collect())
complete.write_excel('data/complete.xlsx', 'biblioteche')
