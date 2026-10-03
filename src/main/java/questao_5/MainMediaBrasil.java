package questao_5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class MainMediaBrasil {

    public static void main(String[] args) throws Exception {

        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output_media_brasil";

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(
                conf,
                "Media das Transacoes do Brasil por Ano"
        );

        job.setJarByClass(MainMediaBrasil.class);

        job.setMapperClass(MediaBrasilMapper.class);
        job.setReducerClass(MediaBrasilReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransacaoWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job,
                new Path(input)
        );

        File outputDirectory = new File(output);

        if (outputDirectory.exists()) {
            deleteDirectory(outputDirectory);
        }

        FileOutputFormat.setOutputPath(
                job,
                new Path(output)
        );

        System.out.println("Iniciando MapReduce...");

        boolean sucesso = job.waitForCompletion(true);

        if (sucesso) {

            System.out.println();
            System.out.println("MapReduce executado com sucesso!");
            System.out.println("Resultado salvo em: " + output);

        } else {

            System.out.println();
            System.out.println("MapReduce falhou!");

            if (job.getStatus() != null) {
                System.out.println(
                        "Motivo: " +
                                job.getStatus().getFailureInfo()
                );
            }
        }
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