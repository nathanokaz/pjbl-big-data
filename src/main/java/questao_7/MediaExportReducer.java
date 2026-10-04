package questao_7;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Calcula e formata a média anual dos valores exportados.
public class MediaExportReducer extends Reducer<Text, TransacaoWritable, Text, Text> {

    // Soma os subtotais e quantidades e emite a média para o ano.
    @Override
    protected void reduce(Text ano, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        double somaTotal = 0;
        int quantidadeTotal = 0;

        // Acumula os subtotais recebidos do mapper ou do combiner.
        for (TransacaoWritable valor : valores) {
            somaTotal += valor.getSoma();
            quantidadeTotal += valor.getQuantidade();
        }

        // Evita divisão por zero e emite a média formatada para o ano.
        if (quantidadeTotal > 0) {
            double media = somaTotal / quantidadeTotal;
            context.write(ano, new Text(String.format(Locale.US, "%.2f", media)));
        }
    }
}