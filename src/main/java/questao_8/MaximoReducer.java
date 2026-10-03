package questao_8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaximoReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(
            Text ano,
            Iterable<DoubleWritable> valores,
            Context context)
            throws IOException, InterruptedException {

        double maior = Double.MIN_VALUE;

        for (DoubleWritable valor : valores) {

            if (valor.get() > maior) {
                maior = valor.get();
            }
        }

        context.write(
                ano,
                new DoubleWritable(maior)
        );
    }
}