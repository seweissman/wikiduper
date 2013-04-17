package courseproj.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Mapper: emits (token, 1) for every article occurrence.
public class WikiReader_Mapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static Text KEY = new Text();
    private final static IntWritable VALUE = new IntWritable(1);

    @Override
    public void map(Text key, Text value, OutputCollector<Text, IntWritable> collector, Reporter reporter)
            throws IOException {
        KEY.set("article count");
        collector.collect(KEY, VALUE);
    }
}

