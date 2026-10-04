package questao_9;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

// Combina localmente as transações de menor e maior amount por chave.
public class TransacaoCombiner extends Reducer<PaisAnoWritable, TransacaoWritable, PaisAnoWritable, TransacaoWritable> {

    // Encontra os extremos entre as transações recebidas para a chave.
    @Override
    protected void reduce(PaisAnoWritable chave, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        TransacaoWritable menor = null;
        TransacaoWritable maior = null;

        // Atualiza as transações mínima e máxima durante a iteração.
        for (TransacaoWritable transacao : valores) {
            // Guarda a menor transação encontrada até o momento.
            if (menor == null || transacao.compareTo(menor) < 0) {
                menor = new TransacaoWritable(transacao.getAmount(), transacao.getLinha());
            }

            // Guarda a maior transação encontrada até o momento.
            if (maior == null || transacao.compareTo(maior) > 0) {
                maior = new TransacaoWritable(transacao.getAmount(), transacao.getLinha());
            }
        }

        // Emite o mínimo local quando há transações para a chave.
        if (menor != null) {
            context.write(chave, menor);
        }

        // Emite o máximo local quando há transações para a chave.
        if (maior != null) {
            context.write(chave, maior);
        }
    }
}