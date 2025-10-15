package org.example.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FilmStats extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new FilmStats(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        //Sprawdzenie, czy podano ścieżki wejściową i wyjściową
        if (args.length != 2) {
            System.err.print("Podano za malo argumentow, oczekiwano: <input> <output>\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = Job.getInstance(getConf(), "FilmStats");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FilmStatsMapper.class);
        job.setCombinerClass(FilmStatsCombiner.class);
        job.setReducerClass(FilmStatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ViewStats.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
