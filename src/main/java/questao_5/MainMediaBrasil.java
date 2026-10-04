package questao_5;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.MapReduceTextOutput;

// Configura e executa o cálculo da média das transações do Brasil por ano.
public class MainMediaBrasil {

    // Prepara o job, limpa a saída anterior e apresenta o resultado da execução.
    public static void main(String[] args) throws Exception {
        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output_media_brasil";

        // Configura a execução local do Hadoop.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        // Registra as classes do job e os tipos de entrada e saída.
        Job job = Job.getInstance(conf, "Media das Transacoes do Brasil por Ano");
        job.setJarByClass(MainMediaBrasil.class);
        job.setMapperClass(MediaBrasilMapper.class);
        job.setReducerClass(MediaBrasilReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransacaoWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Define o arquivo CSV de entrada.
        FileInputFormat.addInputPath(job, new Path(input));

        // Remove a saída anterior, se existir, antes de iniciar o job.
        File outputDirectory = new File(output);
        MapReduceTextOutput.deleteTextFile(outputDirectory);
        if (outputDirectory.exists()) {
            MapReduceTextOutput.deleteDirectory(outputDirectory);
        }

        // Define o diretório onde os resultados serão gravados.
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.out.println("Iniciando MapReduce...");
        boolean sucesso = job.waitForCompletion(true);

        // Exibe mensagens diferentes conforme o resultado do job.
        if (sucesso) {
            System.out.println();
            System.out.println("MapReduce executado com sucesso!");
            System.out.println("Arquivo TXT: " + MapReduceTextOutput.writeTextFile(outputDirectory).getPath());
        } else {
            System.out.println();
            System.out.println("MapReduce falhou!");

            // Informa os detalhes da falha quando o Hadoop disponibiliza o status.
            if (job.getStatus() != null) {
                System.out.println("Motivo: " + job.getStatus().getFailureInfo());
            }
        }
    }

}