from argparse import ArgumentParser
import sys 
import gc
import os
import time
import zlib
import hashlib
import pandas as pd

start_time = time.time()

# Argumentos execucao
parser = ArgumentParser()
parser.add_argument("-o", "--original", dest="original", help="Nome do arquivo original", metavar="ORIGINAL")
parser.add_argument("-n", "--novo", dest="novo", help="Nome do arquivo novo", metavar="NOVO")
parser.add_argument("-d", "--delimiter", dest="delimiter", help="Delimitador do arquivo", metavar="DELIMITER")
parser.add_argument("-e", "--encoding", dest="encoding", help="Charset do arquivo", metavar="ENCODING")
args = parser.parse_args()

# Funcoes
def checkFileHash(file):
  with open(file, "rb") as f:
    file_hash = hashlib.blake2b()
    while chunk := f.read(8192):
      file_hash.update(chunk)
  return file_hash.hexdigest()

def hashLine(row):
  row = " ".join(str(x) for x in row)
  return zlib.adler32(row.encode())

def listComprehension(df):
  return pd.Series([ hashLine(row) for row in df.to_numpy() ])

# Incio
# Validacao nomes dos arquivos para comparacao
if args.original == None or args.novo == None: 
  print("B1. Necessario informar os nomes dos arquivos a serem comparados.")
  print("Execução interrompida.")
  print("--- %s seconds ---" % (time.time() - start_time))
  sys.exit()

# Valida se o conteudo dos arquivos sao iguais
if checkFileHash(args.original) == checkFileHash(args.novo):
  print("B2. O conteudo dos arquivos sao identicos.")
  print("Execução interrompida.")
  print("--- %s seconds ---" % (time.time() - start_time))
  sys.exit()

# Valida primeira linha entre os arquivos
with open(args.original) as f:
  first_line_original = f.readline()
with open(args.novo) as f:
  first_line_novo = f.readline()

if first_line_original != first_line_novo:
  print("B3. O cabecalho dos arquivos sao diferentes.")
  print("Execução interrompida.")
  print("--- %s seconds ---" % (time.time() - start_time))
  sys.exit()

# Padroes parametros opcionais
if args.delimiter == None:
  args.delimiter = ";"

if args.encoding == None:
  args.encoding = "utf-8"

print('--- %s seconds --- Iniciando processo...' % (time.time() - start_time))

# Estrutura para gerar o arquivo com as linhas diferentes do arquivo original
df_original = pd.read_csv(args.original, dtype=str, delimiter=args.delimiter, encoding=args.encoding)
df_original['HASH_$DEL'] = listComprehension(df_original)

df_columns = [c for c in df_original.columns if c != 'HASH_$DEL']
df_original.drop(df_columns, axis=1, inplace=True)

df_original.to_csv('temp_original.csv', index=False)
del(df_original)
gc.collect()

df_novo = pd.read_csv(args.novo, dtype=str, delimiter=args.delimiter, encoding=args.encoding)
df_novo['HASH_$DEL'] = listComprehension(df_novo)

df_original = pd.read_csv('temp_original.csv', delimiter=args.delimiter, encoding=args.encoding)

df_original.set_index('HASH_$DEL', inplace=True)
df_original.sort_index(inplace=True)
df_novo.set_index('HASH_$DEL', inplace=True)

df_diff = df_novo[~df_novo.index.isin(df_original.index)]

del(df_novo)
del(df_original)
gc.collect()

df_diff = df_diff[[c for c in df_diff.columns if not c.endswith('_$DEL')]]
df_diff.to_csv('Output.csv', index=False)

if os.path.exists("temp_original.csv"):
  os.remove("temp_original.csv")

print('--- %s seconds --- Processo concluido!!!' % (time.time() - start_time))
sys.exit()