package questao_9;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Writable;

// Armazena a quantidade monetária e a linha original para o processamento MapReduce.
public class TransacaoWritable implements Writable, Comparable<TransacaoWritable> {

    private double amount;
    private String linha;
    private String tipo;

    // Construtor vazio exigido pelo Hadoop durante a desserialização.
    public TransacaoWritable() {
    }

    // Inicializa o amount e a linha original da transação.
    public TransacaoWritable(double amount, String linha) {
        this("Transacao", amount, linha);
    }

    // Inicializa os campos da transação ou do resultado mínimo/máximo.
    public TransacaoWritable(String tipo, double amount, String linha) {
        this.tipo = tipo;
        this.amount = amount;
        this.linha = linha;
    }

    // Retorna o amount armazenado.
    public double getAmount() {
        return amount;
    }

    // Retorna a linha original armazenada.
    public String getLinha() {
        return linha;
    }

    // Serializa os campos na ordem usada pelo método readFields.
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tipo);
        out.writeDouble(amount);
        out.writeUTF(linha);
    }

    // Restaura os campos na mesma ordem em que foram serializados.
    @Override
    public void readFields(DataInput in) throws IOException {
        tipo = in.readUTF();
        amount = in.readDouble();
        linha = in.readUTF();
    }

    // Compara transações pelo amount para permitir selecionar mínimo e máximo.
    @Override
    public int compareTo(TransacaoWritable outra) {
        return Double.compare(this.amount, outra.amount);
    }

    // Formata os campos estruturados para a saída de texto final.
    @Override
    public String toString() {
        return String.format(Locale.US, "%s\t%.2f\t%s", tipo, amount, linha);
    }
}