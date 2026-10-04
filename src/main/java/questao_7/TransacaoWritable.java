package questao_7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// Armazena e serializa a soma dos valores e a quantidade de transações.
public class TransacaoWritable implements Writable {

    private double soma;
    private int quantidade;

    // Construtor vazio exigido pelo Hadoop durante a desserialização.
    public TransacaoWritable() {
    }

    // Inicializa a soma e a quantidade de transações.
    public TransacaoWritable(double soma, int quantidade) {
        this.soma = soma;
        this.quantidade = quantidade;
    }

    // Retorna a soma armazenada.
    public double getSoma() {
        return soma;
    }

    // Retorna a quantidade armazenada.
    public int getQuantidade() {
        return quantidade;
    }

    // Serializa os campos na ordem usada pelo método readFields.
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeInt(quantidade);
    }

    // Restaura os campos na mesma ordem em que foram serializados.
    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        quantidade = in.readInt();
    }
}