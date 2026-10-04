package questao_1_2_3_4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Soma os valores associados a cada chave emitida pelo mapper.
public class GenericReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Calcula e grava o total de ocorrências de cada chave.
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;

        // Acumula os valores recebidos para a chave atual.
        for (IntWritable value : values) {
            total += value.get();
        }

        // Emite a chave com sua soma total.
        context.write(key, new IntWritable(total));
    }
}