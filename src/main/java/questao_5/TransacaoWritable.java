package questao_5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransacaoWritable implements Writable {

    private double valor;
    private int quantidade;

    public TransacaoWritable() {
    }

    public TransacaoWritable(double valor, int quantidade) {
        this.valor = valor;
        this.quantidade = quantidade;
    }

    public double getValor() {
        return valor;
    }

    public int getQuantidade() {
        return quantidade;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(valor);
        out.writeInt(quantidade);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        valor = in.readDouble();
        quantidade = in.readInt();
    }
}