package courseproj.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Mapper: emits (token, 1) for every article occurrence.
public class WikiReader_Mapper implements Mapper<Text, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static Text KEY = new Text();
    private final static IntWritable VALUE = new IntWritable(1);

    @Override
    public void map(Text key, Text arvalueg1, OutputCollector<Text, IntWritable> collector, Reporter reporter)
            throws IOException {
        // TODO Auto-generated method stub
        KEY.set("article count");
        collector.collect(KEY, VALUE);
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