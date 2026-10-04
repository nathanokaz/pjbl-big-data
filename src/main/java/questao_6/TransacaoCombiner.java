package questao_6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reduz localmente cada grupo às transações de maior e menor preço.
public class TransacaoCombiner extends Reducer<Text, TransacaoWritable, Text, TransacaoWritable> {

    // Encontra os extremos entre as transações recebidas para a chave.
    @Override
    protected void reduce(Text chave, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        TransacaoWritable maior = null;
        TransacaoWritable menor = null;

        // Mantém uma cópia da transação de maior e de menor preço.
        for (TransacaoWritable transacao : valores) {
            // Atualiza o maior preço encontrado até este ponto.
            if (maior == null || transacao.getPreco() > maior.getPreco()) {
                maior = new TransacaoWritable(transacao.getPreco(), transacao.getLinha());
            }

            // Atualiza o menor preço encontrado até este ponto.
            if (menor == null || transacao.getPreco() < menor.getPreco()) {
                menor = new TransacaoWritable(transacao.getPreco(), transacao.getLinha());
            }
        }

        // Emite o maior preço local quando pelo menos uma transação foi recebida.
        if (maior != null) {
            context.write(chave, maior);
        }

        // Emite o menor preço local quando pelo menos uma transação foi recebida.
        if (menor != null) {
            context.write(chave, menor);
        }
    }
}