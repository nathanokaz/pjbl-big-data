package questao_1_2_3_4;

import java.io.File;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.MapReduceTextOutput;

// Configura e executa o job MapReduce das questões 1 a 4.
public class Main {

    // Define a operação, prepara o job e informa seu resultado ao sistema.
    public static void main(String[] args) throws Exception {
        // Aceita brasil, ano, categoria ou flow; mantém brasil como padrão.
        String operacao = args.length > 0 ? args[0].trim().toLowerCase(Locale.ROOT) : "flow";
        if (!operacao.equals("brasil") && !operacao.equals("ano") && !operacao.equals("categoria") && !operacao.equals("flow")) {
            throw new IllegalArgumentException("Operação inválida. Use: brasil, ano, categoria ou flow.");
        }

        String input = "data/operacoes_comerciais_inteira.csv";
        String outputPadrao = operacao.equals("brasil") ? "output" : "output_" + operacao;
        String output = args.length > 1 ? args[1] : outputPadrao;

        // Define as configurações locais e disponibiliza a operação ao mapper.
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("operacao", operacao);

        // Cria o job e registra suas classes e tipos de entrada e saída.
        Job job = Job.getInstance(conf, "Analise de Transacoes - " + operacao);
        job.setJarByClass(Main.class);
        job.setMapperClass(GenericMapper.class);
        job.setReducerClass(GenericReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Define o arquivo CSV que será processado.
        FileInputFormat.addInputPath(job, new Path(input));

        // Remove a saída anterior para permitir a execução do Hadoop.
        File outputDirectory = new File(output);
        MapReduceTextOutput.deleteTextFile(outputDirectory);
        if (outputDirectory.exists()) {
            MapReduceTextOutput.deleteDirectory(outputDirectory);
        }

        // Define onde o Hadoop gravará os resultados.
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.out.println("Iniciando MapReduce...");
        boolean sucesso = job.waitForCompletion(true);

        // Exibe o resultado e encerra com código compatível com o sucesso do job.
        System.out.println(sucesso ? "MapReduce executado com sucesso!" : "Erro na execução do MapReduce.");
        if (sucesso) {
            System.out.println("Arquivo TXT: " + MapReduceTextOutput.writeTextFile(outputDirectory).getPath());
        }
        System.exit(sucesso ? 0 : 1);
    }

}