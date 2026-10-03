package questao_9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacaoMapper
        extends Mapper<Object, Text, Text, TransacaoWritable> {

    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_AMOUNT = 8;

    @Override
    protected void map(
            Object key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        String[] colunas = linha.split(";", -1);

        // Ignora cabeçalho
        if (colunas[0].trim().equalsIgnoreCase("country_or_area")) {
            return;
        }

        // Garante as 10 colunas
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String amountTexto = colunas[COLUNA_AMOUNT].trim();

        if (pais.isEmpty() || ano.isEmpty() || amountTexto.isEmpty()) {
            return;
        }

        try {

            double amount = Double.parseDouble(amountTexto);

            context.write(
                    new Text(pais + ";" + ano),
                    new TransacaoWritable(amount, linha)
            );

        } catch (NumberFormatException e) {

            System.err.println(
                    "Amount inválido: [" + amountTexto + "]"
                            + " | Linha: " + linha
            );
        }
    }
}