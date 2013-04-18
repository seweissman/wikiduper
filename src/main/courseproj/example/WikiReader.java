package courseproj.example;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import courseproj.example.WikipediaInput.Cloud9.WikipediaPage;
import courseproj.example.WikipediaInput.Cloud9.WikipediaPageInputFormat;
import cern.colt.Arrays;

public class WikiReader extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WikiReader.class);

    //Mapper: emits (token, 1) for every article occurrence.
    private static class MyMapper extends Mapper<LongWritable, WikipediaPage, Text, IntWritable> {
        
        private static final Text KEY = new Text();
        private static final IntWritable VALUE = new IntWritable(1);
        
        public void map(LongWritable key, WikipediaPage p, Context context)
                throws IOException, InterruptedException {
            
            System.out.println("\n\nKEY: " + key + "\n\n");
            System.out.println(p.toString() + "\n\n\n");
            KEY.set("TOTAL");
            context.write(KEY, VALUE);
            
            if (p.isRedirect()) {
                KEY.set("REDIRECT");
            } else if (p.isDisambiguation()) {
                KEY.set("DISAMBIGUATION");
            } else if (p.isEmpty()) {
                KEY.set("EMPTY");
            } else if (p.isArticle()) {
                KEY.set("ARTICLE");
                if (p.isStub()) {
                    KEY.set("ARTICLE");
                }
            } else {
                KEY.set("NON_ARTICLE");
            }
            
            System.out.println("\n\nDIFFERENT KEY: " + KEY.toString() + "\n\n");
            
            context.write(KEY, VALUE);
            
        }
        
    }

    //Reducer: sums up all the counts.
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Reuse objects.
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum up values.
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            
            System.out.println("\n\n" + key.toString() + " : " + sum);
            
            SUM.set(sum);
            context.write(key, SUM);
        }
        
    }

    /**
     * Creates an instance of this tool.
     */
    public WikiReader() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {

        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + WikiReader.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);
        
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(WikiReader.class.getSimpleName());
        job.setJarByClass(WikiReader.class);

        job.setNumReduceTasks(reduceTasks);
        
        
        job.setInputFormatClass(WikipediaPageInputFormat.class);
        
        
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WikiReader(), args);
    }
}