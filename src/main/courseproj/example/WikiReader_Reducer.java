package courseproj.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;


//Reducer: sums up all the counts.
public class WikiReader_Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    private final static IntWritable SUM = new IntWritable();

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> collector,
            Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        SUM.set(sum);
        collector.collect(key, SUM);
    }
}

