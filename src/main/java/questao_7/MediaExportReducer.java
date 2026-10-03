package questao_7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

public class MediaExportReducer
        extends Reducer<Text, TransacaoWritable, Text, Text> {

    @Override
    protected void reduce(
            Text ano,
            Iterable<TransacaoWritable> valores,
            Context context)
            throws IOException, InterruptedException {

        double somaTotal = 0;
        int quantidadeTotal = 0;

        for (TransacaoWritable valor : valores) {

            somaTotal += valor.getSoma();
            quantidadeTotal += valor.getQuantidade();
        }

        if (quantidadeTotal > 0) {

            double media = somaTotal / quantidadeTotal;

            context.write(
                    ano,
                    new Text(
                            String.format(
                                    Locale.US,
                                    "%.2f",
                                    media
                            )
                    )
            );
        }
    }
}