package questao_1_2_3_4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenericMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable UM = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        String operacao = conf.get("operacao");

        String linha = value.toString();

        String[] colunas = linha.split(";");

        String resultado = null;

        switch (operacao) {

            case "brasil":

                // Country = coluna 0
                if (colunas[0].equalsIgnoreCase("Brazil")) {
                    resultado = "Brazil";
                }

                break;

            case "ano":

                // Year = coluna 1
                resultado = colunas[1];

                break;

            case "categoria":

                // Category = coluna 9
                resultado = colunas[9];

                break;

            case "flow":

                // Flow = coluna 4
                resultado = colunas[4];

                break;
        }

        if (resultado != null) {
            context.write(new Text(resultado), UM);
        }
    }
}