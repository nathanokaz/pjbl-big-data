package questao_8;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.MapReduceTextOutput;

// Executa dois jobs: encontra o máximo anual no Brasil e ordena os resultados.
public class MainMaximo {

    // Configura e executa as duas etapas do processamento MapReduce.
    public static void main(String[] args) throws Exception {
        String input = "data/operacoes_comerciais_inteira.csv";
        String outputIntermediario = "output_maximo_intermediario";
        String outputFinal = "output_maximo_ordenado";

        // Configura a execução local dos jobs Hadoop.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        // Job 1: agrupa os valores por ano e encontra o maior em cada grupo.
        Job job1 = Job.getInstance(conf, "Maior valor por ano no Brasil");
        job1.setJarByClass(MainMaximo.class);
        job1.setMapperClass(MaximoMapper.class);
        job1.setReducerClass(MaximoReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        // Define a entrada do primeiro job.
        FileInputFormat.addInputPath(job1, new Path(input));

        // Remove a saída intermediária anterior, se existir.
        File intermediario = new File(outputIntermediario);
        if (intermediario.exists()) {
            MapReduceTextOutput.deleteDirectory(intermediario);
        }

        // Grava a saída do primeiro job no caminho intermediário.
        FileOutputFormat.setOutputPath(job1, new Path(outputIntermediario));
        System.out.println("Iniciando Job 1...");
        boolean sucessoJob1 = job1.waitForCompletion(true);

        // Interrompe a execução se a primeira etapa falhar.
        if (!sucessoJob1) {
            System.out.println("Job 1 falhou!");
            return;
        }

        System.out.println("Job 1 concluído!");

        // Job 2: ordena os máximos anuais em ordem decrescente.
        Job job2 = Job.getInstance(conf, "Ordenacao dos maiores valores");
        job2.setJarByClass(MainMaximo.class);
        job2.setMapperClass(OrdenacaoMapper.class);
        job2.setReducerClass(OrdenacaoReducer.class);
        // Um reducer mantém a ordenação global dos máximos anuais.
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Usa a saída intermediária do primeiro job como entrada do segundo.
        FileInputFormat.addInputPath(job2, new Path(outputIntermediario));

        // Remove a saída final anterior, se existir.
        File finalOutput = new File(outputFinal);
        MapReduceTextOutput.deleteTextFile(finalOutput);
        if (finalOutput.exists()) {
            MapReduceTextOutput.deleteDirectory(finalOutput);
        }

        // Define o destino final e executa o segundo job.
        FileOutputFormat.setOutputPath(job2, new Path(outputFinal));
        System.out.println("Iniciando Job 2...");
        boolean sucessoJob2 = job2.waitForCompletion(true);

        // Informa o resultado da ordenação.
        if (sucessoJob2) {
            System.out.println();
            System.out.println("MapReduce executado com sucesso!");
            System.out.println("Arquivo TXT: " + MapReduceTextOutput.writeTextFile(finalOutput).getPath());
        } else {
            System.out.println();
            System.out.println("Job 2 falhou!");
        }
    }

}