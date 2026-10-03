package questao_6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacaoReducer
        extends Reducer<Text, TransacaoWritable, Text, Text> {

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

        if (maior != null) {

            context.write(
                    new Text("Transacao mais cara"),
                    new Text(
                            String.format(
                                    "%.2f | %s",
                                    maior.getPreco(),
                                    maior.getLinha()
                            )
                    )
            );
        }

        if (menor != null) {

            context.write(
                    new Text("Transacao mais barata"),
                    new Text(
                            String.format(
                                    "%.2f | %s",
                                    menor.getPreco(),
                                    menor.getLinha()
                            )
                    )
            );
        }
    }
}