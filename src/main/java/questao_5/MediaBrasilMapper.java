package questao_5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MediaBrasilMapper
        extends Mapper<Object, Text, Text, TransacaoWritable> {

    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_VALOR = 5;

    @Override
    protected void map(
            Object key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        String[] colunas = linha.split(";", -1);

        // Ignora cabeçalho
        if (colunas[0].trim().equalsIgnoreCase("Country")) {
            return;
        }

        // Garante que existem as 10 colunas
        if (colunas.length < 10) {
            System.err.println(
                    "Linha inválida: " + linha
            );
            return;
        }

        // Somente Brasil
        if (!colunas[COLUNA_PAIS]
                .trim()
                .equalsIgnoreCase("Brazil")) {
            return;
        }

        String ano = colunas[COLUNA_ANO].trim();
        String precoTexto = colunas[COLUNA_VALOR].trim();

        try {

            double preco = Double.parseDouble(precoTexto);

            context.write(
                    new Text(ano),
                    new TransacaoWritable(preco, 1)
            );

        } catch (NumberFormatException e) {

            System.err.println(
                    "Preço inválido: [" + precoTexto + "]"
            );
        }
    }
}