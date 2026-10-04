package questao_1_2_3_4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Converte cada linha do CSV em uma chave de agrupamento e no valor 1.
public class GenericMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable UM = new IntWritable(1);
    private static final int COLUNA_CODIGO = 2;

    // Seleciona a coluna de acordo com a operação configurada para o job.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String operacao = conf.get("operacao");
        String[] colunas = value.toString().split(";", -1);
        String resultado = null;

        // Ignora cabeçalho e linhas agregadas, que não representam transações.
        if (colunas[0].trim().equalsIgnoreCase("country") || colunas[0].trim().equalsIgnoreCase("country_or_area")) {
            return;
        }
        if (colunas.length < 10 || colunas[COLUNA_CODIGO].trim().equalsIgnoreCase("TOTAL")) {
            return;
        }

        // Extrai do registro o campo solicitado pela operação.
        switch (operacao) {
            case "brasil":
                // Country = coluna 0
                // Mantém somente registros referentes ao Brasil.
                if (colunas[0].trim().equalsIgnoreCase("Brazil")) {
                    resultado = "Brazil";
                }
                break;
            case "ano":
                // Year = coluna 1
                resultado = colunas[1].trim();
                break;
            case "categoria":
                // Category = coluna 9
                resultado = colunas[9].trim();
                break;
            case "flow":
                // Flow = coluna 4
                resultado = colunas[4].trim();
                break;
        }

        // Emite somente os registros que produziram uma chave.
        if (resultado != null && !resultado.isEmpty()) {
            context.write(new Text(resultado), UM);
        }
    }
}