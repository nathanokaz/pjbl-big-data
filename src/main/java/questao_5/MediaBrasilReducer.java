package questao_5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MediaBrasilReducer
        extends Reducer<Text, TransacaoWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(
            Text ano,
            Iterable<TransacaoWritable> valores,
            Context context)
            throws IOException, InterruptedException {

        double soma = 0;
        int quantidade = 0;

        for (TransacaoWritable valor : valores) {

            soma += valor.getValor();
            quantidade += valor.getQuantidade();
        }

        if (quantidade > 0) {

            double media = soma / quantidade;

            context.write(
                    ano,
                    new DoubleWritable(media)
            );
        }
    }
}