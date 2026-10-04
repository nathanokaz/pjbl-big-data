package questao_8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Lê os resultados intermediários e prepara chaves para ordenação decrescente.
public class OrdenacaoMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    // Separa a linha intermediária em ano e valor máximo.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString().trim();

        // Ignora linhas vazias.
        if (linha.isEmpty()) {
            return;
        }

        String[] partes = linha.split("\\s+");

        // Ignora linhas que não contenham ano e valor.
        if (partes.length < 2) {
            return;
        }

        String ano = partes[0];
        try {
            double valor = Double.parseDouble(partes[1]);

            // Usa o valor negativo como chave para obter a ordem decrescente.
            context.write(new DoubleWritable(-valor), new Text(ano));
        } catch (NumberFormatException e) {
            return;
        }
    }
}