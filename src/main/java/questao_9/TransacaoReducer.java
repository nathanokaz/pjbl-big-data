package questao_9;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

// Emite as transações de menor e maior amount para cada país e ano.
public class TransacaoReducer extends Reducer<PaisAnoWritable, TransacaoWritable, PaisAnoWritable, TransacaoWritable> {

    // Percorre as transações da chave e seleciona os dois extremos.
    @Override
    protected void reduce(PaisAnoWritable chave, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        TransacaoWritable menor = null;
        TransacaoWritable maior = null;

        // Atualiza os extremos conforme percorre os valores recebidos.
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

        // Emite a menor transação formatada quando houver um valor mínimo.
        if (menor != null) {
            context.write(chave, new TransacaoWritable("Menor", menor.getAmount(), menor.getLinha()));
        }

        // Emite a maior transação formatada quando houver um valor máximo.
        if (maior != null) {
            context.write(chave, new TransacaoWritable("Maior", maior.getAmount(), maior.getLinha()));
        }
    }
}