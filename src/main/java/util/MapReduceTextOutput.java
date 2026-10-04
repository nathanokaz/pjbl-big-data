package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

// Reúne as partes textuais de uma saída Hadoop em um arquivo .txt separado.
public final class MapReduceTextOutput {

    private MapReduceTextOutput() {
    }

    // Remove o arquivo anterior para evitar que uma execução falha deixe resultado desatualizado.
    public static void deleteTextFile(File outputDirectory) throws IOException {
        Files.deleteIfExists(new File(outputDirectory.getPath() + ".txt").toPath());
    }

    // Remove uma saída Hadoop existente e todo o seu conteúdo.
    public static void deleteDirectory(File directory) {
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

    // Copia as partes do reducer em ordem para um arquivo UTF-8 ao lado da saída.
    public static File writeTextFile(File outputDirectory) throws IOException {
        List<Path> partFiles = new ArrayList<>();
        try (Stream<Path> entries = Files.list(outputDirectory.toPath())) {
            entries.filter(path -> path.getFileName().toString().startsWith("part-"))
                    .sorted()
                    .forEach(partFiles::add);
        }

        File textFile = new File(outputDirectory.getPath() + ".txt");
        try (BufferedWriter writer = Files.newBufferedWriter(textFile.toPath(), StandardCharsets.UTF_8)) {
            for (Path partFile : partFiles) {
                try (BufferedReader reader = Files.newBufferedReader(partFile, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
            }
        }

        return textFile;
    }
}