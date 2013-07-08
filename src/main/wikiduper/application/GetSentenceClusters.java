package wikiduper.application;

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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.wikiclean.WikiClean;
import org.wikiclean.WikiClean.WikiLanguage;
import org.wikiclean.WikiCleanBuilder;

import wikiduper.wikipedia.WikipediaPage;
import wikiduper.wikipedia.WikipediaPageInputFormat;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;

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
    Mapper<IntWritable, WikipediaPage, IntWritable, Text> {
    //Mapper<LongWritable, WikipediaPage, IntWritable, Text> {
        
        // Map from docid -> sentence number -> cluster number
        static final TreeMap<Integer, TreeMap<Integer, Integer>> docmap = new TreeMap<Integer, TreeMap<Integer, Integer>>();
        
        // The document-sentence identifier
        static final IntWritable CLUSTER = new IntWritable();
        static final Text TITLESENTENCE = new Text();
        
        //Adapted from http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence
        static final Pattern sentenceregex = Pattern.compile(
                "# Match a sentence ending in punctuation or EOS.\n" +
                        "[\\s]*    # Leading white space\n" + 
                        "([A-Z\"]    # First char capital letter or quotation\n" +
                        "[^.!?\\n]*      # Greedily consume up to punctuation.\n" +
                        "(?:          # Group for unrolling the loop.\n" +
                        "  [.!?]      # (special) inner punctuation ok if\n" +
                        "  (?!['\"]?\\s|$)  # not followed by ws or EOS.\n" +
                        "  [^.!?]*    # Greedily consume up to punctuation.\n" +
                        ")*           # Zero or more (special normal*)\n" +
                        "[.!?]?       # Optional ending punctuation.\n" +
                        "['\"]?)       # Optional closing quote.\n" +
                        "(\\s|\\n)*$?       # Trailing white space or new line\n",
                        Pattern.MULTILINE | Pattern.COMMENTS);

        public static WikiClean cleaner;

        public void map(IntWritable key, WikipediaPage p, OutputCollector<IntWritable, Text> output,
                Reporter reporter) throws IOException {
          //public void map(LongWritable key, WikipediaPage p, OutputCollector<IntWritable, Text> output,
            //        Reporter reporter) throws IOException {

            if(!p.isArticle() || p.isEmpty()) return;
            String raw = p.getRawXML();
            String content = cleaner.clean(raw);

            //cleaner.getTitle(content);
            //String content = p.getContent();
            if(content == null) return;
            String line = content
                    //.replace("\n", " ")
                    .replace("  ", " ")
                    .replace(",","")
                    .replace("(b.", "(b")
                    .replace("(d.", "(d");
            Matcher m = sentenceregex.matcher(line);
            
            // Assume a whole Wikipedia article has been passed to the mapper; track sentence number by counting
            int sentencect = 0;
            int id = Integer.parseInt(p.getDocid());
            if(!docmap.containsKey(id)) return;
            TreeMap<Integer,Integer> sentMap = docmap.get(id);

            try{
            // For each sentence in the input text:
            while(m.find()){
                String sentence = m.group(1);
                if(sentMap.containsKey(sentencect)){
                    int clust = sentMap.get(sentencect);
                    TITLESENTENCE.set(p.getTitle() + "\t" + sentence);
                    CLUSTER.set(clust);
                    output.collect(CLUSTER,TITLESENTENCE);
                }
                sentencect++;
            }
            }catch(Throwable e){
                System.err.println("WARNING: Possible stack overflow from regex at docid " + p.getDocid() + " and sentence # " + sentencect);
            }
        }

        public void configure(JobConf job) {
            String docMapFile = job.get("docmapfile");
            
            String language = job.get("wiki.language", "en");
            WikiLanguage wikilang = WikiLanguage.valueOf(language.toUpperCase());
            cleaner =  new WikiCleanBuilder()
                        .withLanguage(wikilang)
                        .withTitle(true)
                        .withFooter(false).build();
            try{
                FileSystem fs = FileSystem.get(job);
                FSDataInputStream in = fs.open(new Path(docMapFile));
                SequenceFile.Reader reader;
                reader = new SequenceFile.Reader(job, SequenceFile.Reader.stream(in));
                IntWritable docid = new IntWritable();
                ArrayListWritable<PairOfInts> sentlist = new ArrayListWritable<PairOfInts>();
                while(reader.next(docid, sentlist)){
                    docmap.put(docid.get(), new TreeMap<Integer, Integer>());
                    for(PairOfInts p : sentlist){
                        if(docmap.get(docid.get()).containsKey(p.getLeftElement())){
                            System.out.println("Sentence in more than one cluster: " + p);
                        }
                        docmap.get(docid.get()).put(p.getLeftElement(), p.getRightElement());
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
           for(int d : docmap.keySet()){
                for(Entry<Integer, Integer> e : docmap.get(d).entrySet()){
                    System.out.println(e.getKey() + " " + e.getValue());
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
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    private static final String LANGUAGE_OPTION = "wiki_language";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("bz2 input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(LANGUAGE_OPTION));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        //options.addOption(OptionBuilder.withArgName("path")
          //      .hasArg().withDescription("pair file").create(PAIRFILE));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("cluster map file").create(CLUSTERMAP));
        //options.addOption(OptionBuilder.withArgName("path")
          //      .hasArg().withDescription("index file").create(INDEXFILE));
        //options.addOption(OptionBuilder.withArgName("path")
          //      .hasArg().withDescription("map file").create(MAPFILE));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(CLUSTERMAP)){
                //|| !cmdline.hasOption(INDEXFILE) || !cmdline.hasOption(MAPFILE)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String language = "en";
        if (cmdline.hasOption(LANGUAGE_OPTION)) {
            language = cmdline.getOptionValue(LANGUAGE_OPTION);
            if(language.length()!=2){
                System.err.println("Error: \"" + language + "\" unknown language!");
                return -1;
            }
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        //String pairPath = cmdline.getOptionValue(PAIRFILE);
        String clusterPath = cmdline.getOptionValue(CLUSTERMAP);
        //String indexPath = cmdline.getOptionValue(INDEXFILE);
        //String mapPath = cmdline.getOptionValue(MAPFILE);
        
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - bz2 file: " + inputPath);
        LOG.info(" - output file: " + outputPath);
        LOG.info(" - language: " + language);
        
        JobConf conf = new JobConf(getConf(), GetSentenceClusters.class);

        //conf.set("indexfile", indexPath);
        //conf.set("mapfile", mapPath);
        
        /* Get Clusters from MinhashWikipediaPages pair output */
        
        //String docmapFile = "docmap.out";
        //String remoteDocmapFile = "docmap2.out";
        //getClusters(pairPath,conf,docmapFile);
        //System.exit(-1);
        //FileSystem fs = FileSystem.get(conf);
        //fs.copyFromLocalFile(new Path(docmapFile), new Path(remoteDocmapFile));
        
        conf.set("docmapfile", clusterPath);
        conf.setJobName(String.format("GetSentenceClusters[%s: %s, %s: %s, %s: %s]", INPUT, inputPath, OUTPUT, outputPath, LANGUAGE_OPTION, language));

        conf.setNumMapTasks(4);
        conf.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        if(language != null){
            conf.set("wiki.language", language);
        }
        
        conf.setMapperClass(ClusterMapper.class);
        //conf.setReducerClass(ClusterReducer.class);
        
        //conf.setInputFormat(WikipediaPageInputFormat.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        // Set heap space - using old API
        conf.set("mapred.job.map.memory.mb", "2048");
        conf.set("mapred.map.child.java.opts", "-Xmx2048m");
        conf.set("mapred.job.reduce.memory.mb", "4096");
        conf.set("mapred.reduce.child.java.opts", "-Xmx4096m");
        
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        //conf.setMapOutputKeyClass(IntWritable.class);
        //conf.setMapOutputValueClass(PairOfStringInt.class);
        
        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        JobClient.runJob(conf);

        return 0;
    }


    public GetSentenceClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GetSentenceClusters(), args);
    }
}
