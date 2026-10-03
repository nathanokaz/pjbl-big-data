package questao_7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MediaExportCombiner
        extends Reducer<Text, TransacaoWritable, Text, TransacaoWritable> {

    @Override
    protected void reduce(
            Text ano,
            Iterable<TransacaoWritable> valores,
            Context context)
            throws IOException, InterruptedException {

        double soma = 0;
        int quantidade = 0;

        for (TransacaoWritable valor : valores) {

            soma += valor.getSoma();
            quantidade += valor.getQuantidade();
        }

        context.write(
                ano,
                new TransacaoWritable(
                        soma,
                        quantidade
                )
        );
    }
}