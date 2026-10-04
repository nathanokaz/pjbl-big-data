package questao_8;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Restaura os valores positivos e emite cada ano com seu máximo formatado.
public class OrdenacaoReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    // Converte a chave de ordenação em valor e grava os anos associados.
    @Override
    protected void reduce(DoubleWritable chave, Iterable<Text> anos, Context context) throws IOException, InterruptedException {
        double valor = -chave.get();

        // Emite cada ano associado à chave numérica com duas casas decimais.
        for (Text ano : anos) {
            context.write(new Text(ano.toString()), new Text(String.format(Locale.US, "%.2f", valor)));
        }
    }
}