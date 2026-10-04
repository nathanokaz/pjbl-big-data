package questao_9;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.MapReduceTextOutput;

// Configura e executa a busca pelos maiores e menores amounts por país e ano.
public class MainTransacao {

    // Prepara o job, substitui a saída anterior e apresenta o resultado.
    public static void main(String[] args) throws Exception {
        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output_maior_menor_amount";

        // Configura a execução local do Hadoop.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        // Registra as classes do job e os tipos de entrada e saída.
        Job job = Job.getInstance(conf, "Maior e Menor Amount por Ano e Pais");
        job.setJarByClass(MainTransacao.class);
        job.setMapperClass(TransacaoMapper.class);
        job.setCombinerClass(TransacaoCombiner.class);
        job.setReducerClass(TransacaoReducer.class);
        job.setMapOutputKeyClass(PaisAnoWritable.class);
        job.setMapOutputValueClass(TransacaoWritable.class);
        job.setOutputKeyClass(PaisAnoWritable.class);
        job.setOutputValueClass(TransacaoWritable.class);

        // Define o arquivo CSV de entrada.
        FileInputFormat.addInputPath(job, new Path(input));

        // Remove o diretório de saída anterior, se existir.
        File outputDirectory = new File(output);
        MapReduceTextOutput.deleteTextFile(outputDirectory);
        if (outputDirectory.exists()) {
            MapReduceTextOutput.deleteDirectory(outputDirectory);
        }

        // Define o destino dos resultados e executa o job.
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.out.println("Iniciando MapReduce...");
        boolean sucesso = job.waitForCompletion(true);

        // Mostra o resultado da execução e, em caso de falha, o motivo disponível.
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