package courseproj.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


//Reducer: sums up all the counts.
public class WikiReader_Reducer implements Reducer<Text, IntWritable, Text, IntWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> collector,
            Reporter reporter) throws IOException {
        
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }

        System.out.println("\n\nTotal Count: " + sum);

        SUM.set(sum);
        collector.collect(key, SUM);

    }
    
    
    @Override
    public void configure(JobConf arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }
}