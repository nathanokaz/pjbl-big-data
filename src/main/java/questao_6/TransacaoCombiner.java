package questao_6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacaoCombiner
        extends Reducer<Text, TransacaoWritable, Text, TransacaoWritable> {

    @Override
    protected void reduce(
            Text chave,
            Iterable<TransacaoWritable> valores,
            Context context)
            throws IOException, InterruptedException {

        TransacaoWritable maior = null;
        TransacaoWritable menor = null;

        for (TransacaoWritable transacao : valores) {

            if (maior == null ||
                    transacao.getPreco() > maior.getPreco()) {

                maior = new TransacaoWritable(
                        transacao.getPreco(),
                        transacao.getLinha()
                );
            }

            if (menor == null ||
                    transacao.getPreco() < menor.getPreco()) {

                menor = new TransacaoWritable(
                        transacao.getPreco(),
                        transacao.getLinha()
                );
            }
        }

        // Envia apenas a maior transação deste Mapper
        if (maior != null) {
            context.write(
                    chave,
                    maior
            );
        }

        // Envia apenas a menor transação deste Mapper
        if (menor != null) {
            context.write(
                    chave,
                    menor
            );
        }
    }
}