package questao_7;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Filtra as exportações brasileiras e emite ano, valor e quantidade.
public class MediaExportMapper extends Mapper<Object, Text, Text, TransacaoWritable> {

    // Índices das colunas usadas no arquivo CSV.
    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_CODIGO = 2;
    private static final int COLUNA_FLOW = 4;
    private static final int COLUNA_PRECO = 5;

    // Valida cada linha, aplica os filtros e converte o preço para número.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();
        String[] colunas = linha.split(";", -1);

        // Ignora a linha de cabeçalho do CSV.
        if (colunas[0].trim().equalsIgnoreCase("Country") || colunas[0].trim().equalsIgnoreCase("country_or_area")) {
            return;
        }

        // Descarta linhas que não tenham todas as colunas esperadas.
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String flow = colunas[COLUNA_FLOW].trim();
        String precoTexto = colunas[COLUNA_PRECO].trim();
        if (ano.isEmpty() || precoTexto.isEmpty()) {
            return;
        }

        // Mantém somente registros referentes ao Brasil.
        if (!pais.equalsIgnoreCase("Brazil")) {
            return;
        }

        // Mantém somente registros classificados como exportação.
        if (!flow.equalsIgnoreCase("Export")) {
            return;
        }

        // Exclui linhas agregadas para calcular a média apenas sobre transações.
        if (colunas[COLUNA_CODIGO].trim().equalsIgnoreCase("TOTAL")) {
            return;
        }

        // Converte e emite o preço; registra o texto quando a conversão falha.
        try {
            double preco = Double.parseDouble(precoTexto);
            context.write(new Text(ano), new TransacaoWritable(preco, 1));
        } catch (NumberFormatException e) {
            System.err.println("Preço inválido: " + precoTexto);
        }
    }
}