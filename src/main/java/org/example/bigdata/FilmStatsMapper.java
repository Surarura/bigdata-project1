package org.example.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Definiujemy typy danych:
// 1. LongWritable, Text -> typy klucza i wartości wejściowej (offset bajtowy i linia tekstu)
// 2. Text, ViewStats   -> typy klucza i wartości wyjściowej (klucz "film||platforma" i nasz obiekt statystyk)
public class FilmStatsMapper extends Mapper<LongWritable, Text, Text, ViewStats> {

    // Tworzymy obiekty klucza i wartości raz, aby ich nie tworzyć w pętli dla każdej linii - to optymalizacja.
    private final Text outputKey = new Text();
    private final ViewStats outputValue = new ViewStats();

    public void map(LongWritable offset, Text lineText, Context context) {
        try {
            if (offset.get() != 0) { //pominięcie nagłówka
                String line = lineText.toString();

                String filmId = "";
                String platform = "";
                long durationSeconds = 0;

                int i = 0;
                for (String word : line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                    if (i == 1) { // Kolumna 1 to 'film_id'
                        filmId = word;
                    }
                    if (i == 4) { // Kolumna 4 to 'duration_seconds'
                        durationSeconds = Long.parseLong(word);
                    }
                    if (i == 6) { // Kolumna 6 to 'platform'
                        platform = word;
                    }
                    i++;
                }
                // Po przejściu przez wszystkie kolumny, tworzymy klucz i wartość.
                outputKey.set(filmId + "||" + platform);
                //klucz jako text, mozna jako klasa osobna jak nizej ale musi byc writablecomparable
                //comparable bo musi po kluczach móc sortowac
                //imo jako klasa byłoby lepiej kiedy bym musiał jakies operacje na kluczu, jak nizej musze dodawac nie, a tak to w.e imo?
                outputValue.set(new LongWritable(durationSeconds), new IntWritable(1));

                // Zapisujemy parę klucz-wartość do kontekstu.
                context.write(outputKey, outputValue);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
