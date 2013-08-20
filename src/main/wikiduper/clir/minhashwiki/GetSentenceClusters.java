package wikiduper.clir.minhashwiki;

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


import java.io.EOFException;
import java.io.IOException;
import java.util.TreeMap;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
import org.wikiclean.WikiClean;
import org.wikiclean.WikiClean.WikiLanguage;
import org.wikiclean.WikiCleanBuilder;

import wikiduper.wikipedia.WikipediaPage;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;


public class GetSentenceClusters extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(GetSentenceClusters.class);

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
    Mapper<PairOfLongInt, PairOfStrings, LongWritable, Text> {
    //Mapper<LongWritable, WikipediaPage, IntWritable, Text> {
        
        // Map from docid -> sentence number -> cluster number
        static final TreeMap<Long, TreeMap<Long, Long>> fdocmap = new TreeMap<Long, TreeMap<Long, Long>>();
        static final TreeMap<Long, TreeMap<Long, Long>> edocmap = new TreeMap<Long, TreeMap<Long, Long>>();
        
        // The document-sentence identifier
        static final LongWritable CLUSTER = new LongWritable();
        static final Text TITLESENTENCE = new Text();
        
        static String elang;
        static String flang;
        //Adapted from http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence

        public void map(PairOfLongInt docIdSentenceId, PairOfStrings langsentence, OutputCollector<LongWritable, Text> output,
                Reporter reporter) throws IOException {
          //public void map(LongWritable key, WikipediaPage p, OutputCollector<IntWritable, Text> output,
            //        Reporter reporter) throws IOException {

            long docid = docIdSentenceId.getLeftElement(); 
            long sentenceid = docIdSentenceId.getRightElement();
            String lang = langsentence.getLeftElement();
            TreeMap<Long, TreeMap<Long, Long>> docmap;
            if(lang.equals(elang)){
                docmap = edocmap;
            }else{
                docmap = fdocmap;
            }
            
            //System.out.println("ID " + docid + " " + docmap.containsKey(docid));
            if(!docmap.containsKey(docid)) return;
            TreeMap<Long,Long> sentMap = docmap.get(docid);

            if(sentMap.containsKey(sentenceid)){
                long clust = sentMap.get(sentenceid);
                TITLESENTENCE.set(docid + "\t" + langsentence.getLeftElement() + "\t" + langsentence.getRightElement());
                CLUSTER.set(clust);
                //System.out.println("cluster " + CLUSTER + " titlesentence " + TITLESENTENCE);
                output.collect(CLUSTER,TITLESENTENCE);
            }
        }

        public void configure(JobConf job) {
            String docMapFile = job.get("docmapfile");
            
            elang = job.get("wiki.language.e","en");
            flang = job.get("wiki.language.f","de");
            

            try{
                FileSystem fs = FileSystem.get(job);
                FSDataInputStream in = fs.open(new Path(docMapFile));
                SequenceFile.Reader reader;
                reader = new SequenceFile.Reader(job, SequenceFile.Reader.stream(in));
                PairOfLongString docidlang = new PairOfLongString();
                ArrayListWritable<PairOfLongs> sentlist = new ArrayListWritable<PairOfLongs>();
                while(reader.next(docidlang, sentlist)){
                    long docid = docidlang.getLeftElement();
                    String inlang = docidlang.getRightElement();
                    if(inlang.equals(elang)){
                        edocmap.put(docid, new TreeMap<Long, Long>());
                        for(PairOfLongs p : sentlist){
                            if(edocmap.get(docid).containsKey(p.getLeftElement())){
                                System.out.println("Sentence in more than one cluster: " + p);
                            }
                            edocmap.get(docid).put(p.getLeftElement(), p.getRightElement());
                        }
                    }
                    if(inlang.equals(flang)){
                        fdocmap.put(docid, new TreeMap<Long, Long>());
                        for(PairOfLongs p : sentlist){
                            if(fdocmap.get(docid).containsKey(p.getLeftElement())){
                                System.out.println("Sentence in more than one cluster: " + p);
                            }
                            fdocmap.get(docid).put(p.getLeftElement(), p.getRightElement());
                        }
                    }

                }
                reader.close();
            }catch (EOFException e) {
                // For some reason it doesn't know when the input stream is done??
            }catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            /*
            System.out.println("Docmap for code " + code + " for language + " + language);
           for(long d : docmap.keySet()){
               System.out.println("doc = " + d);
                for(long sentence : docmap.get(d).keySet()){
                    System.out.println("\t" + sentence + " " + docmap.get(d).get(sentence));
                }
            }
    */

        }
    }
    
    /*
    private static class ClusterReducer extends MapReduceBase implements Reducer<IntWritable, PairOfStringInt, IntWritable, Text> {
        static final Text articleSentence = new Text();
        WikipediaForwardIndex INDEX;
        @Override
        public void reduce(IntWritable key, Iterator<PairOfStringInt> values,
                OutputCollector<IntWritable, Text> output, Reporter reporter)
                        throws IOException {
            

            while (values.hasNext()) {
                PairOfStringInt val = values.next();
                int docid = val.getRightElement();
                String sentence = val.getLeftElement();
                WikipediaPage page = INDEX.getDocument(docid);
                //System.out.println(page.getContent());
                articleSentence.set(page.getTitle() + "\t" + sentence);
                output.collect(key, articleSentence);
            }



        }

        @Override
        public void configure(JobConf conf){
            INDEX = new WikipediaForwardIndex(conf);
            String indexFile = conf.get("indexfile");
            String mapFile = conf.get("mapfile");
            try {
                INDEX.loadIndex(new Path(indexFile), new Path(mapFile), FileSystem.get(conf));
                
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    
    }
*/
    
    //private static final String PAIRFILE = "pairfile";
    private static final String CLUSTERMAP = "clustermap";
    //private static final String INDEXFILE = "indexfile";
    //private static final String MAPFILE = "mapfile";
    private static final String eINPUT = "ewiki";
    private static final String fINPUT = "fwiki";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    private static final String eLANG = "elang";
    private static final String fLANG = "flang";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(eINPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(fINPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(eLANG));
        options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(fLANG));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("cluster map file").create(CLUSTERMAP));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(eINPUT) || !cmdline.hasOption(fINPUT) 
                || !cmdline.hasOption(eLANG) || !cmdline.hasOption(fLANG)
                || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(CLUSTERMAP)){
                //|| !cmdline.hasOption(INDEXFILE) || !cmdline.hasOption(MAPFILE)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }


        String eInputPath = cmdline.getOptionValue(eINPUT);
        String fInputPath = cmdline.getOptionValue(fINPUT);
        String eLang = cmdline.getOptionValue(eLANG);
        String fLang = cmdline.getOptionValue(fLANG);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        String clusterPath = cmdline.getOptionValue(CLUSTERMAP);
        
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - bz2 file: " + eInputPath);
        LOG.info(" - bz2 file: " + fInputPath);
        LOG.info(" - output file: " + outputPath);
        LOG.info(" - e language: " + eLang);
        LOG.info(" - f language: " + fLang);
        
        JobConf conf = new JobConf(getConf(), GetSentenceClusters.class);

        conf.set("docmapfile", clusterPath);
        conf.setJobName(String.format("GetSentenceClusters[%s: %s, %s: %s, %s: %s]", eINPUT, eInputPath, OUTPUT, outputPath, eLANG, eLang));

        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(reduceTasks);

        conf.setMapperClass(ClusterMapper.class);
        //conf.setReducerClass(ClusterReducer.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "4096");
        conf.set("mapred.reduce.child.java.opts", "-Xmx4096m");
        
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        // Job 1
        Path outputDir = new Path(outputPath);
        FileInputFormat.setInputPaths(conf, new Path(eInputPath), new Path(fInputPath));
        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.set("wiki.language.e", eLang);
        conf.set("wiki.language.f", fLang);
        
        FileSystem.get(conf).delete(outputDir, true);

        JobClient.runJob(conf);


        return 0;
    }


    public GetSentenceClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GetSentenceClusters(), args);
    }
}
