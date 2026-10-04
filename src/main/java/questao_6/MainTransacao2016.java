package questao_6;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.MapReduceTextOutput;

// Configura e executa a busca pelas transações mais caras e mais baratas do Brasil em 2016.
public class MainTransacao2016 {

    // Prepara o job, substitui a saída anterior e informa o resultado da execução.
    public static void main(String[] args) throws Exception {
        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output_transacao_2016";

        // Configura a execução local do Hadoop.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        // Registra o mapper, combiner, reducer e os tipos usados pelo job.
        Job job = Job.getInstance(conf, "Transacao mais cara e mais barata do Brasil em 2016");
        job.setJarByClass(MainTransacao2016.class);
        job.setMapperClass(TransacaoMapper.class);
        job.setCombinerClass(TransacaoCombiner.class);
        job.setReducerClass(TransacaoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransacaoWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TransacaoWritable.class);

        // Define o arquivo CSV que será processado.
        FileInputFormat.addInputPath(job, new Path(input));

        // Remove o diretório de saída anterior, se ele existir.
        File outputDirectory = new File(output);
        MapReduceTextOutput.deleteTextFile(outputDirectory);
        if (outputDirectory.exists()) {
            MapReduceTextOutput.deleteDirectory(outputDirectory);
        }

        // Define onde o Hadoop gravará os resultados.
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.out.println("Iniciando MapReduce...");
        boolean sucesso = job.waitForCompletion(true);

        // Mostra a conclusão do job e, em caso de falha, o motivo disponível.
        if (sucesso) {
            System.out.println();
            System.out.println("MapReduce executado com sucesso!");
            System.out.println("Arquivo TXT: " + MapReduceTextOutput.writeTextFile(outputDirectory).getPath());
        } else {
            System.out.println();
            System.out.println("MapReduce falhou!");

            // Exibe os detalhes quando o Hadoop fornece um status para o job.
            if (job.getStatus() != null) {
                System.out.println("Motivo: " + job.getStatus().getFailureInfo());
            }
        }
    }

}