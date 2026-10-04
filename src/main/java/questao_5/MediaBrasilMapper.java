package questao_5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Filtra as transações brasileiras e emite ano, valor e quantidade.
public class MediaBrasilMapper extends Mapper<Object, Text, Text, TransacaoWritable> {

    // Índices das colunas usadas no arquivo CSV.
    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_CODIGO = 2;
    private static final int COLUNA_VALOR = 5;

    // Valida cada registro, filtra o Brasil e converte o valor para número.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();
        String[] colunas = linha.split(";", -1);

        // Ignora o cabeçalho do CSV.
        if (colunas[0].trim().equalsIgnoreCase("Country") || colunas[0].trim().equalsIgnoreCase("country_or_area")) {
            return;
        }

        // Rejeita registros que não contenham as colunas esperadas.
        if (colunas.length < 10) {
            System.err.println("Linha inválida: " + linha);
            return;
        }

        // Mantém somente registros cujo país seja Brasil.
        if (!colunas[COLUNA_PAIS].trim().equalsIgnoreCase("Brazil")) {
            return;
        }

        // Exclui linhas agregadas para calcular a média apenas sobre transações.
        if (colunas[COLUNA_CODIGO].trim().equalsIgnoreCase("TOTAL")) {
            return;
        }

        String ano = colunas[COLUNA_ANO].trim();
        String precoTexto = colunas[COLUNA_VALOR].trim();
        if (ano.isEmpty() || precoTexto.isEmpty()) {
            return;
        }

        // Converte o valor e emite a transação; registra valores inválidos.
        try {
            double preco = Double.parseDouble(precoTexto);
            context.write(new Text(ano), new TransacaoWritable(preco, 1));
        } catch (NumberFormatException e) {
            System.err.println("Preço inválido: [" + precoTexto + "]");
        }
    }
}