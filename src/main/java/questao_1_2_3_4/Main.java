package questao_1_2_3_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class Main {

    public static void main(String[] args) throws Exception {

        String operacao = "brasil";
        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("operacao", operacao);

        Job job = Job.getInstance(conf, "Analise de Transacoes");

        job.setJarByClass(Main.class);
        job.setMapperClass(GenericMapper.class);
        job.setReducerClass(GenericReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));

        File outputDirectory = new File(output);
        if (outputDirectory.exists()) {
            deleteDirectory(outputDirectory);
        }

        FileOutputFormat.setOutputPath(job, new Path(output));

        System.out.println("Iniciando MapReduce...");

        boolean sucesso = job.waitForCompletion(true);

        System.out.println(
                sucesso
                        ? "MapReduce executado com sucesso!"
                        : "Erro na execução do MapReduce."
        );

        System.exit(sucesso ? 0 : 1);
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }

        directory.delete();
    }
}