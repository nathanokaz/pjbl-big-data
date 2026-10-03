package questao_8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class MainMaximo {

    public static void main(String[] args) throws Exception {

        String input = "data/operacoes_comerciais_inteira.csv";

        String outputIntermediario =
                "output_maximo_intermediario";

        String outputFinal =
                "output_maximo_ordenado";

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        // =====================================================
        // JOB 1
        // Encontrar o maior valor de cada ano
        // =====================================================

        Job job1 = Job.getInstance(
                conf,
                "Maior valor por ano no Brasil"
        );

        job1.setJarByClass(MainMaximo.class);

        job1.setMapperClass(MaximoMapper.class);
        job1.setReducerClass(MaximoReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job1,
                new Path(input)
        );

        File intermediario =
                new File(outputIntermediario);

        if (intermediario.exists()) {
            deleteDirectory(intermediario);
        }

        FileOutputFormat.setOutputPath(
                job1,
                new Path(outputIntermediario)
        );

        System.out.println(
                "Iniciando Job 1..."
        );

        boolean sucessoJob1 =
                job1.waitForCompletion(true);

        if (!sucessoJob1) {

            System.out.println(
                    "Job 1 falhou!"
            );

            return;
        }

        System.out.println(
                "Job 1 concluído!"
        );

        // =====================================================
        // JOB 2
        // Ordenar do maior valor para o menor
        // =====================================================

        Job job2 = Job.getInstance(
                conf,
                "Ordenacao dos maiores valores"
        );

        job2.setJarByClass(MainMaximo.class);

        job2.setMapperClass(OrdenacaoMapper.class);
        job2.setReducerClass(OrdenacaoReducer.class);

        job2.setMapOutputKeyClass(
                DoubleWritable.class
        );

        job2.setMapOutputValueClass(
                Text.class
        );

        job2.setOutputKeyClass(
                Text.class
        );

        job2.setOutputValueClass(
                Text.class
        );

        // Entrada = saída do Job 1
        FileInputFormat.addInputPath(
                job2,
                new Path(outputIntermediario)
        );

        File finalOutput =
                new File(outputFinal);

        if (finalOutput.exists()) {
            deleteDirectory(finalOutput);
        }

        FileOutputFormat.setOutputPath(
                job2,
                new Path(outputFinal)
        );

        System.out.println(
                "Iniciando Job 2..."
        );

        boolean sucessoJob2 =
                job2.waitForCompletion(true);

        if (sucessoJob2) {

            System.out.println();
            System.out.println(
                    "MapReduce executado com sucesso!"
            );

            System.out.println(
                    "Resultado final: " +
                            outputFinal
            );

        } else {

            System.out.println();
            System.out.println(
                    "Job 2 falhou!"
            );
        }
    }

    private static void deleteDirectory(
            File directory) {

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