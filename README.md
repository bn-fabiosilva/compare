# compare
 
Gera arquivo 'Output.csv' com as diferenças do arquivo novo para o arquivo original.

<hr> 

UserAchievements ~780 MB

>> python compare.py -o 'rating_complete_full.csv' -n 'rating_complete_full_alterado.csv' -d ','

Log exemplo:

- --- 0.000919342041015 seconds --- Iniciando processo...
- --- 0.000945806503295 seconds --- Validando se o conteudo dos arquivo sao iguais...
- --- 4.344660997390747 seconds --- Validando se o cabecalho dos arquivos sao iguais...
- --- 4.345240116119385 seconds --- Abrindo arquivo original e gerando hash...
- --- 145.7147624492645 seconds --- Ordenando hash...
- --- 159.2000730037689 seconds --- Gerando cabecalho do arquivo de output...
- --- 159.2015650272369 seconds --- Abrindo arquivo novo gerando output das diferencas...
- --- 328.4548277854919 seconds --- Processo concluido!!!

<hr>

UserAchievements ~8,38 GB

>> python compare.py -o '2019-Nov.csv' -n '2019-Nov_alterado.csv' -d ','

Log exemplo:

- --- 0.0012559890747 seconds --- Iniciando processo...
- --- 0.0012950897216 seconds --- Validando se o conteudo dos arquivo sao iguais...
- --- 0.0013062953948 seconds --- Validando se o cabecalho dos arquivos sao iguais...
- --- 0.0018653869628 seconds --- Abrindo arquivo original e gerando hash...
- --- 281.55957055091 seconds --- Ordenando hash...
- --- 301.68407440185 seconds --- Gerando cabecalho do arquivo de output...
- --- 301.68952798843 seconds --- Abrindo arquivo novo gerando output das diferencas...
- --- 763.20144510269 seconds --- Processo concluido!!!
