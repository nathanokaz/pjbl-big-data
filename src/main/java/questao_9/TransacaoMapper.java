package questao_9;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Valida as linhas e emite as transações agrupadas por país e ano.
public class TransacaoMapper extends Mapper<Object, Text, PaisAnoWritable, TransacaoWritable> {

    // Índices das colunas usadas no arquivo CSV.
    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_CODIGO = 2;
    private static final int COLUNA_AMOUNT = 8;

    // Filtra registros inválidos e converte o amount para número.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();
        String[] colunas = linha.split(";", -1);

        // Ignora a linha de cabeçalho do CSV.
        if (colunas[0].trim().equalsIgnoreCase("Country") || colunas[0].trim().equalsIgnoreCase("country_or_area")) {
            return;
        }

        // Descarta linhas que não tenham todas as colunas esperadas.
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String codigo = colunas[COLUNA_CODIGO].trim();
        String amountTexto = colunas[COLUNA_AMOUNT].trim();

        // Exclui registros agregados, que não correspondem a uma transação individual.
        if (codigo.equalsIgnoreCase("TOTAL")) {
            return;
        }

        // Ignora registros sem país, ano ou amount.
        if (pais.isEmpty() || ano.isEmpty() || amountTexto.isEmpty()) {
            return;
        }

        // Converte e emite o amount; registra o valor e a linha se inválido.
        try {
            double amount = Double.parseDouble(amountTexto);
            context.write(new PaisAnoWritable(pais, ano), new TransacaoWritable(amount, linha));
        } catch (NumberFormatException e) {
            System.err.println("Amount inválido: [" + amountTexto + "]" + " | Linha: " + linha);
        }
    }
}