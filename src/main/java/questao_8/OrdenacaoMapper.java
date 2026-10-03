package questao_8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrdenacaoMapper
        extends Mapper<Object, Text, DoubleWritable, Text> {

    @Override
    protected void map(
            Object key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String linha = value.toString().trim();

        if (linha.isEmpty()) {
            return;
        }

        String[] partes = linha.split("\\s+");

        if (partes.length < 2) {
            return;
        }

        String ano = partes[0];
        double valor = Double.parseDouble(partes[1]);

        // Negativo para ordenar de forma decrescente
        context.write(
                new DoubleWritable(-valor),
                new Text(ano)
        );
    }
}