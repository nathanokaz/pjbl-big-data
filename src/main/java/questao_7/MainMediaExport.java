package questao_7;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.MapReduceTextOutput;

// Configura e executa o cálculo da média anual das exportações brasileiras.
public class MainMediaExport {

    // Prepara o job, substitui a saída anterior e informa o resultado da execução.
    public static void main(String[] args) throws Exception {
        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output_media_export";

        // Configura a execução local do Hadoop.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        // Registra o mapper, combiner, reducer e os tipos de dados do job.
        Job job = Job.getInstance(conf, "Media das Exportacoes do Brasil por Ano");
        job.setJarByClass(MainMediaExport.class);
        job.setMapperClass(MediaExportMapper.class);
        job.setCombinerClass(MediaExportCombiner.class);
        job.setReducerClass(MediaExportReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransacaoWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define o arquivo CSV de entrada.
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