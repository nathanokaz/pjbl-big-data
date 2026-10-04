package questao_5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// Armazena um valor de transação e a quantidade associada para o MapReduce.
public class TransacaoWritable implements Writable {

    private double valor;
    private int quantidade;

    // Construtor vazio exigido pelo Hadoop durante a desserialização.
    public TransacaoWritable() {
    }

    // Inicializa o valor e a quantidade da transação.
    public TransacaoWritable(double valor, int quantidade) {
        this.valor = valor;
        this.quantidade = quantidade;
    }

    // Retorna o valor armazenado.
    public double getValor() {
        return valor;
    }

    // Retorna a quantidade armazenada.
    public int getQuantidade() {
        return quantidade;
    }

    // Serializa os campos na ordem esperada pelo método readFields.
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(valor);
        out.writeInt(quantidade);
    }

    // Restaura os campos na mesma ordem em que foram serializados.
    @Override
    public void readFields(DataInput in) throws IOException {
        valor = in.readDouble();
        quantidade = in.readInt();
    }
}