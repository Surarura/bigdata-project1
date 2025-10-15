package org.example.bigdata;

import org.apache.hadoop.io.*;
import java.io.*;

//cel klasy - zgrupowanie czasu i licznika odsłon do 1 obiektu zamiast przesyłać jedno po drugim
public class ViewStats implements Writable {
    private DoubleWritable totalDuration;
    private IntWritable viewCount;

    public ViewStats(){set(new DoubleWritable(0), new IntWritable(0));}

    public ViewStats(long duration, int count) {set(new DoubleWritable(duration), new IntWritable(count));}

    public DoubleWritable getTotalDuration() {
        return totalDuration;
    }

    public IntWritable getViewCount() {
        return viewCount;
    }

    public void set(DoubleWritable duration, IntWritable count) {
        this.totalDuration = duration;
        this.viewCount = count;
    }

    public void addViewStats(ViewStats viewStats) {
        double newDuration = this.totalDuration.get() + viewStats.getTotalDuration().get();
        int newCount = this.viewCount.get() + viewStats.getViewCount().get();
        this.set(new DoubleWritable(newDuration), new IntWritable(newCount));
    }

    // Metoda mówiąca Hadoopowi, jak zapisać (serializować) dane tej klasy.
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Zapisz oba pola do strumienia wyjściowego.
        totalDuration.write(dataOutput);
        viewCount.write(dataOutput);
    }

    // Metoda mówiąca Hadoopowi, jak odczytać (deserializować) dane do obiektu tej klasy.
    // WAŻNE: Kolejność odczytu musi być identyczna jak kolejność zapisu!
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // Odczytaj oba pola ze strumienia wejściowego.
        totalDuration.readFields(dataInput);
        viewCount.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ViewStats viewStats = (ViewStats) o;

        return totalDuration.equals(viewStats.totalDuration) && viewCount.equals(viewStats.viewCount);
    }

    @Override
    public int hashCode() {
        int result = totalDuration.hashCode();
        result = 31 * result + viewCount.hashCode();
        return result;
    }

}
