package questao_5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Calcula a média dos valores de transação agrupados por ano.
public class MediaBrasilReducer extends Reducer<Text, TransacaoWritable, Text, DoubleWritable> {

    // Soma os valores e quantidades e emite a média para o ano.
    @Override
    protected void reduce(Text ano, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        double soma = 0;
        int quantidade = 0;

        // Acumula os valores e as quantidades das transações recebidas.
        for (TransacaoWritable valor : valores) {
            soma += valor.getValor();
            quantidade += valor.getQuantidade();
        }

        // Evita divisão por zero e grava a média quando há transações.
        if (quantidade > 0) {
            double media = soma / quantidade;
            context.write(ano, new DoubleWritable(media));
        }
    }
}