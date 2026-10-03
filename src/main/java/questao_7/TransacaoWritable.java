package questao_7;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransacaoWritable implements Writable {

    private double soma;
    private int quantidade;

    public TransacaoWritable() {
    }

    public TransacaoWritable(double soma, int quantidade) {
        this.soma = soma;
        this.quantidade = quantidade;
    }

    public double getSoma() {
        return soma;
    }

    public int getQuantidade() {
        return quantidade;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeInt(quantidade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        quantidade = in.readInt();
    }
}