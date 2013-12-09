package wikiduper.utils;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;

public class FilterResults extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(FilterResults.class);

    private static class FilterMapper extends MapReduceBase implements
    Mapper<IntWritable, ArrayListWritable<DocSentence>, IntWritable, ArrayListWritable<DocSentence>> {
        
        static HashSet<Long> docIdSet;
        static String lang;
        
        public void map(IntWritable key, ArrayListWritable<DocSentence> sentlist, OutputCollector<IntWritable, ArrayListWritable<DocSentence>> output,
                    Reporter reporter) throws IOException {
            
            
            for(DocSentence ds : sentlist){
                if(ds.getLanguage().equals(lang)){
                    long docid = ds.getId();
                    if(docIdSet.contains(docid)){
                        output.collect(key, sentlist);
                        return;
                    }
                }
            }

        }
        
        public void configure(JobConf job) {
            lang = job.get("lang","de");
            readSampleDocIds(job);
        }

        public void readSampleDocIds(JobConf job) {
            String docSampleFile = job.get("sampleDocs");
            FileSystem fs;

            try{
                fs = FileSystem.get(job);
                FSDataInputStream in = fs.open(new Path(docSampleFile));
                InputStreamReader bin = new InputStreamReader(in);
                BufferedReader reader = new BufferedReader(bin);
                String line;
                while((line = reader.readLine()) != null){
                    long docid = Long.parseLong(line);
                    docIdSet.add(docid);
                }
                reader.close();
             }catch (EOFException e) {
                // For some reason it doesn't know when the input stream is done??
             }catch (IOException e) {
                // TODO Auto-generated catch block
                 e.printStackTrace();
             }
            System.out.println("Number of ids read: " + docIdSet.size());


        }
        
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    
    private static final String LANGUAGE = "lang";
    
    // Sample docs
    private static final String sampleDocsOption = "sampledocs";
    
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input").create(INPUT));
        
        options.addOption(OptionBuilder.withArgName("string")
                .hasArg().withDescription("language").create(LANGUAGE));
        
        // Sample Docs
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("sampled documents").create(sampleDocsOption));
        
        
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT)
                || !cmdline.hasOption(LANGUAGE)
                ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        //int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 4;

        String lang = cmdline.getOptionValue(LANGUAGE);
        String sampleDocsFile = cmdline.getOptionValue(sampleDocsOption);
        
        
        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - nput file: " + inputPath);
        LOG.info(" - output file: " + outputPath);

        JobConf conf = new JobConf(getConf(), FilterResults.class);
        conf.setJobName(String.format("FilterResults[%s: %s]", OUTPUT, outputPath));


        conf.set("lang", lang);
        conf.set("sampleDocs", sampleDocsFile);
        
        conf.setNumMapTasks(20);
        conf.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(FilterMapper.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "8000");
        conf.set("mapred.reduce.child.java.opts", "-Xmx8000m");
        //conf.set("mapred.child.java.opts", "-Xmx2048m");
        
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(ArrayListWritable.class);
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        JobClient.runJob(conf);

        return 0;
    }

    public FilterResults() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FilterResults(), args);
    }
}
