package courseproj.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper: emits (token, 1) for every article occurrence.
public class WikiReader_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static Text KEY = new Text();
    private final static IntWritable VALUE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = ((Text) value).toString();
        
        context.write(KEY, VALUE);
    }
}