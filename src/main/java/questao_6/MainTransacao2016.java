package questao_6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class MainTransacao2016 {

    public static void main(String[] args) throws Exception {

        String input = "data/operacoes_comerciais_inteira.csv";
        String output = "output_transacao_2016";

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(
                conf,
                "Transacao mais cara e mais barata do Brasil em 2016"
        );

        job.setJarByClass(MainTransacao2016.class);

        // Mapper
        job.setMapperClass(TransacaoMapper.class);

        // Combiner obrigatório
        job.setCombinerClass(TransacaoCombiner.class);

        // Reducer
        job.setReducerClass(TransacaoReducer.class);

        // Saída do Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransacaoWritable.class);

        // Saída final
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Entrada
        FileInputFormat.addInputPath(
                job,
                new Path(input)
        );

        // Remove output anterior
        File outputDirectory = new File(output);

        if (outputDirectory.exists()) {
            deleteDirectory(outputDirectory);
        }

        // Saída
        FileOutputFormat.setOutputPath(
                job,
                new Path(output)
        );

        System.out.println("Iniciando MapReduce...");

        boolean sucesso = job.waitForCompletion(true);

        if (sucesso) {

            System.out.println();
            System.out.println("MapReduce executado com sucesso!");
            System.out.println(
                    "Resultado salvo em: " + output
            );

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