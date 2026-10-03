package questao_8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

public class OrdenacaoReducer
        extends Reducer<DoubleWritable, Text, Text, Text> {

    @Override
    protected void reduce(
            DoubleWritable chave,
            Iterable<Text> anos,
            Context context)
            throws IOException, InterruptedException {

        double valor = -chave.get();

        for (Text ano : anos) {

            context.write(
                    new Text(ano.toString()),
                    new Text(
                            String.format(
                                    Locale.US,
                                    "%.2f",
                                    valor
                            )
                    )
            );
        }
    }
}