package wikiduper.utils;



import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;


import wikiduper.application.MergeClusters;

public class CombineSequenceFiles extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MergeClusters.class);

    private static final String INPUT1 = "input1";
    private static final String INPUT2 = "input2";
    private static final String OUTPUT = "output";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input file 1").create(INPUT1));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("input file 2").create(INPUT2));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT1) || !cmdline.hasOption(INPUT2) || !cmdline.hasOption(OUTPUT)){  
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String input1Path = cmdline.getOptionValue(INPUT1);
        String input2Path = cmdline.getOptionValue(INPUT2);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        LOG.info("Tool name: " + this.getClass().getName());
            
        JobConf conf = new JobConf(getConf(), MergeClusters.class);

        IntWritable clusterid;
        IntWritable category;
        try {
            //HashMap<Integer,Integer> clustercategorymap = readGtData(gtin, conf);
                
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Writer outputWriter  = SequenceFile.createWriter(conf, Writer.file(new Path(outputPath)),
                    Writer.keyClass(IntWritable.class), Writer.valueClass(IntWritable.class));
                
            try{
                FSDataInputStream in1 = fs.open(new Path(input1Path));
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in1));
                clusterid = new IntWritable();
                category = new IntWritable();
                reader.next(clusterid, category);
                int lastid = clusterid.get();
                while(reader.next(clusterid, category)){
                    IntWritable clusterout = new IntWritable();
                    clusterout.set(lastid);
                    outputWriter.append(clusterout, category);
                    lastid = clusterid.get();
                }
                reader.close();
            }catch (EOFException e) {
                    //  For some reason it doesn't know when the input stream is done??
            }

            try{
                FSDataInputStream in2 = fs.open(new Path(input2Path));
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in2));
                clusterid = new IntWritable();
                category = new IntWritable();
                
                while(reader.next(clusterid, category)){
                    outputWriter.append(clusterid, category);
                }
                reader.close();
            }catch (EOFException e) {
                    // For some reason it doesn't know when the input stream is done??
            }

                outputWriter.close();                      
            
        }catch(IOException e){
            e.printStackTrace();
        }
        return 0;
    }
    

    public CombineSequenceFiles() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CombineSequenceFiles(), args);
    }
}
