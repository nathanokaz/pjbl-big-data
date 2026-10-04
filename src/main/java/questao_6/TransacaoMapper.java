package questao_6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Filtra as transações brasileiras de 2016 e emite seus preços e linhas originais.
public class TransacaoMapper extends Mapper<Object, Text, Text, TransacaoWritable> {

    // Índices das colunas relevantes no arquivo CSV.
    private static final int COLUNA_PAIS = 0;
    private static final int COLUNA_ANO = 1;
    private static final int COLUNA_CODIGO = 2;
    private static final int COLUNA_PRECO = 5;

    // Chave comum usada para agrupar todas as transações selecionadas.
    private static final Text CHAVE = new Text("2016");

    // Valida cada linha, aplica os filtros e converte o preço para número.
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();
        String[] colunas = linha.split(";", -1);

        // Ignora a linha de cabeçalho do CSV.
        if (colunas[0].trim().equalsIgnoreCase("Country")) {
            return;
        }

        // Descarta linhas que não tenham todas as colunas esperadas.
        if (colunas.length < 10) {
            return;
        }

        String pais = colunas[COLUNA_PAIS].trim();
        String ano = colunas[COLUNA_ANO].trim();
        String codigo = colunas[COLUNA_CODIGO].trim();
        String precoTexto = colunas[COLUNA_PRECO].trim();

        // Mantém apenas as transações brasileiras.
        if (!pais.equalsIgnoreCase("Brazil")) {
            return;
        }

        // Mantém apenas as transações do ano de 2016.
        if (!ano.equals("2016")) {
            return;
        }

        // Ignora linhas agregadas identificadas pelo código TOTAL.
        if (codigo.equalsIgnoreCase("TOTAL")) {
            return;
        }

        // Converte e emite o preço; registra o texto quando a conversão falha.
        try {
            double preco = Double.parseDouble(precoTexto);
            context.write(CHAVE, new TransacaoWritable(preco, linha));
        } catch (NumberFormatException e) {
            System.err.println("Preço inválido: " + precoTexto);
        }
    }
}