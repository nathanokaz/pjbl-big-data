package questao_7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MediaExportMapper
        extends Mapper<Object, Text, Text, TransacaoWritable> {

    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_FLOW = 4;
    private static final int COLUNA_PRECO = 5;

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

        // Garante as 10 colunas
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String flow = colunas[COLUNA_FLOW].trim();
        String precoTexto = colunas[COLUNA_PRECO].trim();

        // Somente Brasil
        if (!pais.equalsIgnoreCase("Brazil")) {
            return;
        }

        // Somente Export
        if (!flow.equalsIgnoreCase("Export")) {
            return;
        }

        try {

            double preco = Double.parseDouble(precoTexto);

            context.write(
                    new Text(ano),
                    new TransacaoWritable(preco, 1)
            );

        } catch (NumberFormatException e) {

            System.err.println(
                    "Preço inválido: " + precoTexto
            );
        }
    }
}