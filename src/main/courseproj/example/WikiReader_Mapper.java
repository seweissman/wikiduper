package courseproj.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper: emits (token, 1) for every article occurrence.
public class WikiReader_Mapper extends Mapper<Text, Text, Text, IntWritable> {

  // Reuse objects to save overhead of object creation.
  private final static Text KEY = new Text();
  private final static IntWritable VALUE = new IntWritable(1);

  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    String keyy = key.toString();
    String valuee = value.toString();
    
    System.out.println("\n\nKEY: " + keyy + "\n\n");
    System.out.println("\n\nVALUE: " + valuee + "\n\n");
    KEY.set("article count");
    context.write(KEY, VALUE);
  }
}