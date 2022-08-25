from argparse import ArgumentParser
from csv import reader
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
parser.add_argument("-c", "--chunksize", dest="chunksize", help="Chuncksize de abertura dos csv", metavar="CHUNKSIZE")
args = parser.parse_args()

# Padroes parametros opcionais
if args.delimiter == None:
  args.delimiter = ";"

if args.encoding == None:
  args.encoding = "utf-8"

if args.chunksize == None:
  args.chunksize = 1000000
else:
  args.chunksize = int(args.chunksize)

# Funcoes
def checkFileHash(file):
  with open(file, "rb") as f:
    file_hash = hashlib.blake2b()
    while chunk := f.read(32768):
      file_hash.update(chunk)
  return file_hash.hexdigest()

def hashLine(row):
  row = " ".join(str(x) for x in row)
  return zlib.adler32(row.encode())

def listComprehension(df):
  return pd.Series([ hashLine(row) for row in df.to_numpy() ])

print('--- %s seconds --- Iniciando processo...' % (time.time() - start_time))

# Incio
# Validacao nomes dos arquivos para comparacao
if args.original == None or args.novo == None: 
  print("E1. Necessario informar os nomes dos arquivos a serem comparados.")
  print("Execução interrompida.")
  print("--- %s seconds ---" % (time.time() - start_time))
  sys.exit()

# Valida se o conteudo dos arquivos sao iguais
print('--- %s seconds --- Validando se o conteudo dos arquivo sao iguais...' % (time.time() - start_time))
if checkFileHash(args.original) == checkFileHash(args.novo):
  print("E2. O conteudo dos arquivos sao identicos.")
  print("Execução interrompida.")
  print("--- %s seconds ---" % (time.time() - start_time))
  sys.exit()

# Valida primeira linha entre os arquivos
print('--- %s seconds --- Validando se o cabecalho dos arquivos sao iguais...' % (time.time() - start_time))
with open(args.original) as f:
  first_line_original = f.readline()
with open(args.novo) as f:
  first_line_novo = f.readline()

if first_line_original != first_line_novo:
  print("B3. O cabecalho dos arquivos sao diferentes.")
  print("Execução interrompida.")
  print("--- %s seconds ---" % (time.time() - start_time))
  sys.exit()

# Estrutura para gerar o arquivo com as linhas diferentes do arquivo original
print('--- %s seconds --- Abrindo arquivo original e gerando hash...' % (time.time() - start_time))
ls_temp_hash = []
df_hash = pd.DataFrame(ls_temp_hash, dtype=int)
with pd.read_csv(args.original, dtype=str, delimiter=args.delimiter, encoding=args.encoding, chunksize=args.chunksize) as reader:
  reader
  for chunk in reader:
    df_original = chunk
    ls_temp_hash = listComprehension(df_original)
    df_hash = pd.concat([df_hash, ls_temp_hash], axis=0, ignore_index=True)
del(df_original)
gc.collect()

print('--- %s seconds --- Ordenando hash...' % (time.time() - start_time))
df_hash.set_index(0, inplace=True)
#df_hash.sort_index(inplace=True)

print('--- %s seconds --- Gerando cabecalho do arquivo de output...' % (time.time() - start_time))
if os.path.exists("Output.csv"):
  os.remove("Output.csv")
with open("Output.csv", "w") as text_file:
  text_file.write(first_line_novo)

print('--- %s seconds --- Abrindo arquivo novo e gerando output das diferencas...' % (time.time() - start_time))
ls_temp_hash = []
with pd.read_csv(args.novo, dtype=str, delimiter=args.delimiter, encoding=args.encoding, chunksize=args.chunksize) as reader:
  reader
  for chunk in reader:
    df_novo = chunk
    df_novo.reset_index(inplace=True, drop=True)
    df_novo['HASH_$DEL'] = listComprehension(df_novo)
    df_novo.set_index('HASH_$DEL', inplace=True)
    #df_novo.sort_index(inplace=True)
    df_diff = df_novo[~df_novo.index.isin(df_hash.index)]
    df_diff = df_diff[[c for c in df_diff.columns if not c.endswith('_$DEL')]]
    df_diff.to_csv('Output.csv', index=False, mode='a', header=False)

print('--- %s seconds --- Processo concluido!!!' % (time.time() - start_time))
sys.exit()