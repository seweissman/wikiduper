package courseproj.application;

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
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;

public class MergeClusters extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(MergeClusters.class);


    private static final String PAIRFILE = "pairfile";
    //private static final String INDEXFILE = "indexfile";
    //private static final String MAPFILE = "mapfile";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    private static final String LANGUAGE_OPTION = "wiki_language";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
                .withDescription("two-letter language code").create(LANGUAGE_OPTION));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("pair file").create(PAIRFILE));
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

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(PAIRFILE)){
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

        String outputPath = cmdline.getOptionValue(OUTPUT);
        String pairPath = cmdline.getOptionValue(PAIRFILE);
        //String indexPath = cmdline.getOptionValue(INDEXFILE);
        //String mapPath = cmdline.getOptionValue(MAPFILE);
        
        //int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - output file: " + outputPath);
        LOG.info(" - language: " + language);
        
        JobConf conf = new JobConf(getConf(), MergeClusters.class);

        //conf.set("indexfile", indexPath);
        //conf.set("mapfile", mapPath);
        
        /* Get Clusters from MinhashWikipediaPages pair output */
        
        //String remoteDocmapFile = "docmap2.out";
        getClusters(pairPath,conf,outputPath);
        //System.exit(-1);
        //FileSystem fs = FileSystem.get(conf);
        //fs.copyFromLocalFile(new Path(docmapFile), new Path(remoteDocmapFile));
        
        //conf.set("docmapfile", docmapFile);

        return 0;
    }

    // Reads in pairs from MinhahsWikipediaPages output and performs connected component analysis
    // Creates a global cluster numbering and a map from doc numbers to sentences and their cluster numbers
    // Writes the docmap to docmapFile
    public static void getClusters(String filein, JobConf conf, String docmapFile){

        try {
            TreeMap<PairOfInts, HashSet<PairOfInts>> matchmap = new TreeMap<PairOfInts, HashSet<PairOfInts>>();
            // map from doc id to sentence numbers
            TreeMap<Integer, TreeSet<PairOfInts>> docmap = new TreeMap<Integer, TreeSet<PairOfInts>>();
            readPairs(filein,conf,matchmap);
            int componentct = 0;
            
            while(!matchmap.isEmpty()){
                PairOfInts entity = matchmap.firstKey();
                HashSet<PairOfInts> comp = getConnectedComponent(entity, matchmap);

                for(PairOfInts p : comp){
                    int docid = p.getLeftElement();
                    int sentencenum = p.getRightElement();
                    if(!docmap.containsKey(docid)){
                        docmap.put(docid, new TreeSet<PairOfInts>());
                    }
                    docmap.get(docid).add(new PairOfInts(sentencenum, componentct));
                }

                componentct++;

            }

            FileSystem fs = FileSystem.get(conf);
            Path clustersOut = new Path(docmapFile);
            FileSystem.get(conf).delete(clustersOut, true);
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
                    SequenceFile.Writer.file(clustersOut), 
                    SequenceFile.Writer.keyClass(IntWritable.class), 
                    SequenceFile.Writer.valueClass(ArrayListWritable.class));
            ArrayListWritable<PairOfInts> sentlist;
            IntWritable doc;
            for(int docid : docmap.navigableKeySet()){
                doc = new IntWritable();
                sentlist = new ArrayListWritable<PairOfInts>();
                sentlist.clear();
                doc.set(docid);
                for(PairOfInts sentcomp : docmap.get(docid)){
                    sentlist.add(sentcomp);
                }
                writer.append(doc,sentlist);
            }
            
            writer.close();
            fs.close();
            System.out.println("N components: " + componentct);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void readPairs(String filein, JobConf conf, TreeMap<PairOfInts, HashSet<PairOfInts>> matchmap){
     
    try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
            PairOfInts p1 = new PairOfInts();
            PairOfInts p2 = new PairOfInts();
            while(reader.next(p1, p2)){
                if(!matchmap.containsKey(p1)){
                    matchmap.put(p1, new HashSet<PairOfInts> ());
                }
                if(!matchmap.containsKey(p2)){
                    matchmap.put(p2, new HashSet<PairOfInts> ());
                }
                matchmap.get(p2).add(p1);
                matchmap.get(p1).add(p2);
                p1 = new PairOfInts();
                p2 = new PairOfInts();
            }
          reader.close();
          }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
          }
        }
    }catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }


    }
    
    static HashSet<PairOfInts> getConnectedComponent(PairOfInts entity, TreeMap<PairOfInts, HashSet<PairOfInts>> matchmap){
        HashSet<PairOfInts> component = new HashSet<PairOfInts>();
        component.add(entity);
        boolean hasmatchcomponent = true;
        while(!matchmap.isEmpty() && hasmatchcomponent){
            hasmatchcomponent = false;
            HashSet<PairOfInts> comp = (HashSet<PairOfInts>) component.clone();
            for(PairOfInts e : comp){
                if(matchmap.containsKey(e)){
                    hasmatchcomponent = true;
                    HashSet <PairOfInts> matches = matchmap.remove(e);
                    component.addAll(matches);
                }
            }
        }

        return component;
    }
    
    static HashSet<PairOfInts> getConnectedComponentRecursive(PairOfInts entity, TreeMap<PairOfInts, HashSet<PairOfInts>> matchmap){
        HashSet<PairOfInts> component = new HashSet<PairOfInts>();
        component.add(entity);
        if(matchmap.isEmpty() || !matchmap.containsKey(entity)){
            return component;
        }

        HashSet <PairOfInts> matches = matchmap.remove(entity);
        for(PairOfInts m : matches){
            HashSet<PairOfInts> c = getConnectedComponentRecursive(m, matchmap);
            component.addAll(c);
        }
        return component;
    }

    public MergeClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MergeClusters(), args);
    }
}
