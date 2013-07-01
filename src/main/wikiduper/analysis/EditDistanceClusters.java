package wikiduper.analysis;

/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import wikiduper.dist.EditDistance;

public class EditDistanceClusters extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(EditDistanceClusters.class);

    /**
     * ClusterMapper
     * 
     * Reads in a map from docid -> sentence number -> cluster number as side data. 
     * 
     * Maps over wikipedia input looking for pages with the right docid and pull out the corresponding sentences.
     * 
     * @author weissman
     *
     */
    private static class ClusterMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, LongWritable, Text> {
    //Mapper<LongWritable, WikipediaPage, IntWritable, Text> {
        
        // The document-sentence identifier
        static LongWritable CLUSTER = new LongWritable();
        static Text SENTENCE = new Text();
        
        //Adapted from http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence
        Pattern linepat = Pattern.compile("([-0-9]+)\t([^\t]+)\t(.*)");
        
        public void map(LongWritable key, Text line, OutputCollector<LongWritable, Text> output,
                Reporter reporter) throws IOException {

            Matcher m = linepat.matcher(line.toString());
            String sig = "";
            String sentence = "";
            String article = "";
            if(m.matches()){
                sig = m.group(1);
                article = m.group(2);
                sentence = m.group(3);
                System.out.println("sig = " + sig + ", article = " + article +  ", sentence = " + sentence);
                CLUSTER = new LongWritable();
                CLUSTER.set(Long.valueOf(sig));
                SENTENCE = new Text();
                SENTENCE.set(sentence);
                output.collect(CLUSTER, SENTENCE);
            }else{
                  System.out.println("Bad line: " + line);
                  System.exit(-1);
            }

        }


    }
    

    private static class ClusterReducer extends MapReduceBase implements 
    Reducer<LongWritable, Text, LongWritable, LongWritable> {
        
        @Override
        public void reduce(LongWritable key, Iterator<Text> values,
                OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
                        throws IOException {
            
            HashSet<String> valSet = new HashSet<String>();
            while (values.hasNext()) {
                valSet.add(values.next().toString());
            }
            LongWritable scoreOut;
            LongWritable clusterOut;
            if(valSet.size() > 1){
                String[] valList = valSet.toArray(new String[valSet.size()]);
                for(int i=0;i<valList.length;i++){
                    String m1 = valList[i];
                    for(int j=i+1;j<valList.length;j++){
                        String m2 = valList[j];
                        long d = EditDistance.dist(valList[i], valList[j]);
                        long dl = Math.max(m1.length(), m2.length()) - Math.min(m1.length(), m2.length());
                        long score = Math.round(100*(d - dl + 1)*1.0/Math.max(m1.length(), m2.length()));
                        scoreOut = new LongWritable();
                        clusterOut = new LongWritable();
                        scoreOut.set(score);
                        clusterOut.set(key.get());
                        output.collect(clusterOut, scoreOut);                                                        
                    }
                }
            }

        }

    }

    
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
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

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)){
                //|| !cmdline.hasOption(INDEXFILE) || !cmdline.hasOption(MAPFILE)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - input file: " + inputPath);
        LOG.info(" - output file: " + outputPath);
        
        JobConf conf = new JobConf(getConf(), EditDistanceClusters.class);

        conf.setJobName(String.format("EditDistanceClusters[%s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath));

        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));


        conf.setMapperClass(ClusterMapper.class);
        conf.setReducerClass(ClusterReducer.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "4096");
        conf.set("mapred.reduce.child.java.opts", "-Xmx4096m");
        
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        JobClient.runJob(conf);

        return 0;
    }


    public EditDistanceClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new EditDistanceClusters(), args);
    }
}
