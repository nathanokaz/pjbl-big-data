package questao_6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Encontra e emite as transações de maior e menor preço entre os registros recebidos.
public class TransacaoReducer extends Reducer<Text, TransacaoWritable, Text, TransacaoWritable> {

    // Percorre os valores da chave e guarda as transações extremas.
    @Override
    protected void reduce(Text chave, Iterable<TransacaoWritable> valores, Context context) throws IOException, InterruptedException {
        TransacaoWritable maior = null;
        TransacaoWritable menor = null;

        // Atualiza os extremos à medida que percorre as transações.
        for (TransacaoWritable transacao : valores) {
            // Guarda a transação com maior preço encontrada até agora.
            if (maior == null || transacao.getPreco() > maior.getPreco()) {
                maior = new TransacaoWritable(transacao.getPreco(), transacao.getLinha());
            }

            // Guarda a transação com menor preço encontrada até agora.
            if (menor == null || transacao.getPreco() < menor.getPreco()) {
                menor = new TransacaoWritable(transacao.getPreco(), transacao.getLinha());
            }
        }

        // Emite a transação mais cara quando há valores para a chave.
        if (maior != null) {
            context.write(new Text("Transacao mais cara"), maior);
        }

        // Emite a transação mais barata quando há valores para a chave.
        if (menor != null) {
            context.write(new Text("Transacao mais barata"), menor);
        }
    }
}