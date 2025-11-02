package org.example.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilmStatsReducer extends Reducer<Text, ViewStats, Text, Text> {

    private final Text resultValue = new Text();

    @Override
    public void reduce(Text key, Iterable<ViewStats> values, Context context) throws IOException, InterruptedException {
        long totalWatchTime = 0;
        int total_views = 0;

        for (ViewStats val : values) {
            totalWatchTime += (long) val.getTotalDuration().get();
            total_views += val.getViewCount().get();
        }

        float avg_watch_time = (float) ((double) totalWatchTime / total_views / 60.0);

        String[] keyParts = key.toString().split("\\|\\|");
        String filmId = keyParts[0];
        String platform = keyParts[1];

        String resultString = String.format(java.util.Locale.US, "%s,%s,%d,%.2f", filmId, platform, total_views, avg_watch_time);
        //UTIL LOCALE US ŻEBY NIE BYŁO 72,8 TYLKO 72.8 -> PRZECINEK PSUŁ POTEM W HIVE COS
        resultValue.set(resultString);
        context.write(key, resultValue);
    }
}
