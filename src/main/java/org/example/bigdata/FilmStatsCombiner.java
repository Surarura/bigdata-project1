package org.example.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.swing.text.View;
import java.io.IOException;

public class FilmStatsCombiner extends Reducer<Text, ViewStats, Text, ViewStats> {

    @Override
    public void reduce(Text key, Iterable<ViewStats> values, Context context) throws IOException, InterruptedException {

        ViewStats viewStats = new ViewStats();

        for (ViewStats val : values) {
            viewStats.addViewStats(val);
        }
        context.write(key, viewStats);
    }
}
