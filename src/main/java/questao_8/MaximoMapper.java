package questao_8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaximoMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
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

        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String precoTexto = colunas[COLUNA_PRECO].trim();

        // Somente Brasil
        if (!pais.equalsIgnoreCase("Brazil")) {
            return;
        }

        // Ignora registros agregados
        if (colunas[2].trim().equalsIgnoreCase("TOTAL")) {
            return;
        }

        try {

            double preco = Double.parseDouble(precoTexto);

            context.write(
                    new Text(ano),
                    new DoubleWritable(preco)
            );

        } catch (NumberFormatException e) {

            System.err.println(
                    "Preço inválido: " + precoTexto
            );
        }
    }
}