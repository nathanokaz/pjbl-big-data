package questao_8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Filtra os registros brasileiros e emite o preço associado a cada ano.
public class MaximoMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    // Índices das colunas usadas no arquivo CSV.
    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_PRECO = 5;

    // Valida cada registro, aplica os filtros e converte o preço.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();
        String[] colunas = linha.split(";", -1);

        // Ignora a linha de cabeçalho do CSV.
        if (colunas[0].trim().equalsIgnoreCase("Country") || colunas[0].trim().equalsIgnoreCase("country_or_area")) {
            return;
        }

        // Descarta registros que não tenham as colunas esperadas.
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String precoTexto = colunas[COLUNA_PRECO].trim();
        if (ano.isEmpty() || precoTexto.isEmpty()) {
            return;
        }

        // Mantém somente registros referentes ao Brasil.
        if (!pais.equalsIgnoreCase("Brazil")) {
            return;
        }

        // Ignora registros agregados identificados pelo código TOTAL.
        if (colunas[2].trim().equalsIgnoreCase("TOTAL")) {
            return;
        }

        // Converte e emite o preço; registra o texto quando a conversão falha.
        try {
            double preco = Double.parseDouble(precoTexto);
            context.write(new Text(ano), new DoubleWritable(preco));
        } catch (NumberFormatException e) {
            System.err.println("Preço inválido: " + precoTexto);
        }
    }
}