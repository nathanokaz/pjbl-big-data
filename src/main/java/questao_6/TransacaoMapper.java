package questao_6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacaoMapper
        extends Mapper<Object, Text, Text, TransacaoWritable> {

    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_CODIGO = 2;
    private static final int COLUNA_PRECO = 5;

    private static final Text CHAVE = new Text("2016");

    @Override
    protected void map(
            Object key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        String[] colunas = linha.split(";", -1);

        // Ignora o cabeçalho
        if (colunas[0].trim().equalsIgnoreCase("Country")) {
            return;
        }

        // Garante que existem as 10 colunas
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String codigo = colunas[COLUNA_CODIGO].trim();
        String precoTexto = colunas[COLUNA_PRECO].trim();

        // Somente Brasil
        if (!pais.equalsIgnoreCase("Brazil")) {
            return;
        }

        // Somente 2016
        if (!ano.equals("2016")) {
            return;
        }

        // Ignora registros agregados
        if (codigo.equalsIgnoreCase("TOTAL")) {
            return;
        }

        try {

            double preco = Double.parseDouble(precoTexto);

            context.write(
                    CHAVE,
                    new TransacaoWritable(preco, linha)
            );

        } catch (NumberFormatException e) {

            System.err.println(
                    "Preço inválido: " + precoTexto
            );
        }
    }
}