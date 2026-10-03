package questao_9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacaoCombiner
        extends Reducer<Text, TransacaoWritable,
        Text, TransacaoWritable> {

    @Override
    protected void reduce(
            Text chave,
            Iterable<TransacaoWritable> valores,
            Context context)
            throws IOException, InterruptedException {

        TransacaoWritable menor = null;
        TransacaoWritable maior = null;

        for (TransacaoWritable transacao : valores) {

            if (menor == null ||
                    transacao.compareTo(menor) < 0) {

                menor = new TransacaoWritable(
                        transacao.getAmount(),
                        transacao.getLinha()
                );
            }

            if (maior == null ||
                    transacao.compareTo(maior) > 0) {

                maior = new TransacaoWritable(
                        transacao.getAmount(),
                        transacao.getLinha()
                );
            }
        }

        if (menor != null) {

            context.write(
                    chave,
                    menor
            );
        }

        if (maior != null) {

            context.write(
                    chave,
                    maior
            );
        }
    }
}