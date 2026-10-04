package questao_8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Encontra o maior preço recebido para cada ano.
public class MaximoReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // Percorre os preços do ano e emite o maior valor encontrado.
    @Override
    protected void reduce(Text ano, Iterable<DoubleWritable> valores, Context context) throws IOException, InterruptedException {
        double maior = Double.NEGATIVE_INFINITY;

        // Atualiza o máximo conforme percorre os valores recebidos.
        for (DoubleWritable valor : valores) {
            // Substitui o máximo atual quando encontra um preço maior.
            if (valor.get() > maior) {
                maior = valor.get();
            }
        }

        // Emite o maior preço associado ao ano.
        context.write(ano, new DoubleWritable(maior));
    }
}