package questao_6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransacaoWritable implements Writable {

    private double preco;
    private String linha;

    public TransacaoWritable() {
    }

    public TransacaoWritable(double preco, String linha) {
        this.preco = preco;
        this.linha = linha;
    }

    public double getPreco() {
        return preco;
    }

    public String getLinha() {
        return linha;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(preco);
        out.writeUTF(linha);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        preco = in.readDouble();
        linha = in.readUTF();
    }
}