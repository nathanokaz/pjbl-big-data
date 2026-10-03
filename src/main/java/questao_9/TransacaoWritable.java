package questao_9;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransacaoWritable
        implements Writable, Comparable<TransacaoWritable> {

    private double amount;
    private String linha;

    public TransacaoWritable() {
    }

    public TransacaoWritable(double amount, String linha) {
        this.amount = amount;
        this.linha = linha;
    }

    public double getAmount() {
        return amount;
    }

    public String getLinha() {
        return linha;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(amount);
        out.writeUTF(linha);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        amount = in.readDouble();
        linha = in.readUTF();
    }

    @Override
    public int compareTo(TransacaoWritable outra) {
        return Double.compare(this.amount, outra.amount);
    }
}