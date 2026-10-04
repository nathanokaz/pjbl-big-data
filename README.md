```
```

# Projeto Big Data com Hadoop MapReduce

Projeto acadêmico para analisar operações comerciais com Java e Apache Hadoop MapReduce. Os jobs são executados localmente: não é necessário configurar um cluster Hadoop.

## Tecnologias

- Java 17
- Maven
- Apache Hadoop 3.3.6 (Common e MapReduce)

## Dados de entrada

Todos os programas procuram o arquivo abaixo, usando o diretório do projeto como diretório de trabalho:

```text
data/operacoes_comerciais_inteira.csv
```

O arquivo de dados não está incluído neste checkout. Para executar os jobs, crie a pasta `data/` na raiz do projeto e coloque nela o CSV com esse nome. O código espera campos separados por ponto e vírgula (`;`) e pelo menos dez colunas. Entre as colunas usadas estão país (índice 0), ano (1), código da mercadoria (2), fluxo (4), valor de comércio (5), quantidade/amount (8) e categoria (9). Os índices são contados a partir de zero.

Os jobs ignoram o cabeçalho e, nas análises de transações, registros agregados identificados pelo código `TOTAL`.

## Questões implementadas

| Questões | Programa                        | Análise                                                                                                                     |
| --------- | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 1 a 4     | `questao_1_2_3_4.Main`        | Conta registros do Brasil ou agrupa registros por ano, categoria ou fluxo (importação/exportação).                       |
| 5         | `questao_5.MainMediaBrasil`   | Calcula a média anual do valor de comércio das transações do Brasil.                                                     |
| 6         | `questao_6.MainTransacao2016` | Encontra as transações brasileiras de maior e menor valor em 2016 e mantém a linha original.                              |
| 7         | `questao_7.MainMediaExport`   | Calcula a média anual do valor das exportações brasileiras.                                                               |
| 8         | `questao_8.MainMaximo`        | Encontra o maior valor de comércio do Brasil por ano e ordena os resultados do maior para o menor. Usa dois jobs MapReduce. |
| 9         | `questao_9.MainTransacao`     | Encontra os menores e maiores valores de amount/quantidade por país e ano, mantendo a linha original.                       |

Nas questões 1 a 4, `brasil` conta somente registros do Brasil; `ano`, `categoria` e `flow` agrupam registros de todos os países pela coluna correspondente.

## Preparação e execução

Abra um terminal na pasta que contém este `README.md`. Verifique se Java 17 e Maven estão disponíveis e compile o projeto:

```powershell
mvn compile
```

Para executar uma classe pelo Maven, use o plugin Exec. Na primeira execução, o Maven pode precisar baixar o plugin e as dependências:

```powershell
mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java "-Dexec.mainClass=questao_1_2_3_4.Main" "-Dexec.args=flow"
```

Troque o nome da classe pelo programa desejado:

| Questão | Classe principal                | Saída padrão                                                                                                     |
| -------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| 1 a 4    | `questao_1_2_3_4.Main`        | `output/` para `brasil`; `output_ano/`, `output_categoria/` ou `output_flow/` para as outras operações |
| 5        | `questao_5.MainMediaBrasil`   | `output_media_brasil/`                                                                                           |
| 6        | `questao_6.MainTransacao2016` | `output_transacao_2016/`                                                                                         |
| 7        | `questao_7.MainMediaExport`   | `output_media_export/`                                                                                           |
| 8        | `questao_8.MainMaximo`        | `output_maximo_intermediario/` e `output_maximo_ordenado/`                                                     |
| 9        | `questao_9.MainTransacao`     | `output_maior_menor_amount/`                                                                                     |

Para mudar a operação das questões 1 a 4, passe `brasil`, `ano`, `categoria` ou `flow` em `-Dexec.args`. Exemplo:

```powershell
mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java "-Dexec.mainClass=questao_1_2_3_4.Main" "-Dexec.args=categoria"
```

Sem argumento, essa classe usa `brasil`. Também é possível passar um segundo argumento para indicar outro diretório de saída, por exemplo `-Dexec.args="flow resultado_flow"`.

## Arquivos de saída

Cada pasta de saída é escrita no diretório do projeto. O Hadoop grava nela arquivos `part-r-*` e `_SUCCESS`. Após a execução bem-sucedida, os programas também juntam os arquivos `part-*` em um arquivo `.txt` ao lado da pasta. Por exemplo:

```text
output_flow/part-r-00000
output_flow/_SUCCESS
output_flow.txt
```

O diretório é o resultado nativo do Hadoop; o `.txt` é uma cópia consolidada para facilitar a leitura. Ao executar novamente, os programas removem a saída anterior daquele job antes de gravar a nova. Na questão 8, `output_maximo_intermediario/` contém a saída da primeira etapa e `output_maximo_ordenado/` contém o resultado final; o arquivo consolidado é criado para a saída final.

## Organização do código

```text
src/main/java/
	questao_1_2_3_4/  Job configurável para as questões 1 a 4
	questao_5/        Média das transações brasileiras por ano
	questao_6/        Maior e menor transação brasileira em 2016
	questao_7/        Média anual das exportações brasileiras
	questao_8/        Máximo anual e ordenação dos resultados
	questao_9/        Menor e maior amount por país e ano
	util/             Utilitário para consolidar as saídas em arquivos TXT
```

Os `Mapper`s leem e filtram as linhas, emitindo chaves e valores intermediários. Os `Combiner`s, usados em alguns jobs, agregam dados localmente antes do `Reducer`. Os `Reducer`s agrupam os valores por chave e calculam os resultados finais. As classes `Writable` representam os valores que o Hadoop transfere entre essas etapas.