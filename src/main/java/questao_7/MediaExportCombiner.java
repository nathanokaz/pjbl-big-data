package questao_7;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Combina localmente as somas e quantidades das exportações agrupadas por ano.
public class MediaExportCombiner extends Reducer<Text, TransacaoWritable, Text, TransacaoWritable> {

    // Soma os subtotais recebidos e emite um único subtotal para o ano.
    @Override
    protected void reduce(Text ano, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        double soma = 0;
        int quantidade = 0;

        // Acumula o valor e a quantidade dos registros recebidos.
        for (TransacaoWritable valor : valores) {
            soma += valor.getSoma();
            quantidade += valor.getQuantidade();
        }

        // Emite o subtotal combinado para a chave do ano.
        context.write(ano, new TransacaoWritable(soma, quantidade));
    }
}