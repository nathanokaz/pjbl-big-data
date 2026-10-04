package questao_9;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

// Representa país e ano como campos separados para agrupar as transações.
public class PaisAnoWritable implements WritableComparable<PaisAnoWritable> {

    private String pais = "";
    private String ano = "";

    // Construtor vazio exigido pelo Hadoop durante a desserialização.
    public PaisAnoWritable() {
    }

    // Inicializa os campos que formam a chave composta.
    public PaisAnoWritable(String pais, String ano) {
        this.pais = pais;
        this.ano = ano;
    }

    // Serializa os campos na mesma ordem usada por readFields.
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pais);
        out.writeUTF(ano);
    }

    // Restaura os campos da chave.
    @Override
    public void readFields(DataInput in) throws IOException {
        pais = in.readUTF();
        ano = in.readUTF();
    }

    // Ordena primeiro por país e depois por ano.
    @Override
    public int compareTo(PaisAnoWritable outra) {
        int comparacaoPais = pais.compareTo(outra.pais);
        return comparacaoPais != 0 ? comparacaoPais : ano.compareTo(outra.ano);
    }

    // Usa os mesmos campos da comparação para a partição Hadoop.
    @Override
    public int hashCode() {
        return Objects.hash(pais, ano);
    }

    // Considera iguais as chaves com país e ano iguais.
    @Override
    public boolean equals(Object objeto) {
        return objeto instanceof PaisAnoWritable outra && pais.equals(outra.pais) && ano.equals(outra.ano);
    }

    // Fornece campos tabulados para a saída de texto do Hadoop.
    @Override
    public String toString() {
        return String.format("%s\t%s", pais, ano);
    }
}