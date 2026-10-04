package questao_6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Writable;

// Armazena e serializa o preço e a linha original de uma transação.
public class TransacaoWritable implements Writable {

    private double preco;
    private String linha;

    // Construtor vazio exigido pelo Hadoop durante a desserialização.
    public TransacaoWritable() {
    }

    // Inicializa o preço e a linha da transação.
    public TransacaoWritable(double preco, String linha) {
        this.preco = preco;
        this.linha = linha;
    }

    // Retorna o preço armazenado.
    public double getPreco() {
        return preco;
    }

    // Retorna a linha original armazenada.
    public String getLinha() {
        return linha;
    }

    // Serializa os campos na ordem usada pelo método readFields.
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(preco);
        out.writeUTF(linha);
    }

    // Restaura os campos na mesma ordem em que foram serializados.
    @Override
    public void readFields(DataInput in) throws IOException {
        preco = in.readDouble();
        linha = in.readUTF();
    }

    // Formata os campos estruturados para a saída de texto do Hadoop.
    @Override
    public String toString() {
        return String.format(Locale.US, "%.2f\t%s", preco, linha);
    }
}