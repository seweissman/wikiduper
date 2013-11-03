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


import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class GetDocComponents extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(GetDocComponents.class);


    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String THRESHOLD = "threshold";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("minhash output buckets").create(INPUT));
        options.addOption(OptionBuilder.withArgName("num")
                .hasArg().withDescription("similarity threshold").create(THRESHOLD));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT) || !cmdline.hasOption(THRESHOLD)){
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String outputPath = cmdline.getOptionValue(OUTPUT);
        String inputPath = cmdline.getOptionValue(INPUT);
        int threshold = Integer.parseInt(cmdline.getOptionValue(THRESHOLD));
        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - output file: " + outputPath);
        
        JobConf conf = new JobConf(getConf(), GetDocComponents.class);

        /* Get Clusters from MinhashWikipediaPages pair output */
        
        getClusters(inputPath,conf,outputPath,threshold);

        return 0;
    }
    
    static HashSet<String> getConnectedComponent(String entity, TreeMap<String, HashSet<String>> matchmap){
        HashSet<String> component = new HashSet<String>();
        component.add(entity);
        boolean hasmatchcomponent = true;
        while(!matchmap.isEmpty() && hasmatchcomponent){
            hasmatchcomponent = false;
            HashSet<String> comp = (HashSet<String>) component.clone();
            for(String e : comp){
                if(matchmap.containsKey(e)){
                    hasmatchcomponent = true;
                    HashSet <String> matches = matchmap.remove(e);
                    component.addAll(matches);
                }
            }
        }

        return component;
    }

    // Creates a global cluster numbering and a map from doc numbers to sentences and their cluster numbers
    // Writes the docmap to docmapFile
    //static final Pattern sentencepattern = Pattern.compile(".*\\[(.+), (.+), (.+)\\].*");
    public static void getClusters(String filein, JobConf conf, String docmapFile, int thresh){
        ArrayList<HashSet<String>> compList = new ArrayList<HashSet<String>>();
        try {
            TreeMap<String, HashSet<String>> doc2docmap = new TreeMap<String,HashSet<String>>();
            readPairs(filein,conf,doc2docmap,thresh);

            HashSet<String> entities = new HashSet<String>();
            entities.addAll(doc2docmap.keySet());
            for(String entity : entities){
                if(doc2docmap.containsKey(entity)){
                    HashSet<String> comp = getConnectedComponent(entity,doc2docmap);
                    compList.add(comp);
                }
                
            }


            FileSystem fs = FileSystem.get(conf);
            Path clustersOut = new Path(docmapFile);
            FileSystem.get(conf).delete(clustersOut, true);
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
                    SequenceFile.Writer.file(clustersOut), 
                    SequenceFile.Writer.keyClass(IntWritable.class), 
                    SequenceFile.Writer.valueClass(ArrayListWritable.class));
            ArrayListWritable<Text> doclist;
            IntWritable compnumber;
            for(int i=0;i<compList.size();i++){
                compnumber = new IntWritable(i);
                HashSet<String> comp = compList.get(i);
                doclist = new ArrayListWritable<Text>();
                for(String doc : comp){
                    Text docout = new Text();
                    docout.set(doc);
                    doclist.add(docout);
                }
                writer.append(compnumber,doclist);
            }
            
            writer.close();
            fs.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void readPairs(String filein, JobConf conf, TreeMap<String, HashSet<String>> doc2docmap, int thresh){
        try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        IntWritable count = new IntWritable();

        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));

            PairOfStrings docpair = new PairOfStrings();

            while(reader.next(count,docpair)){
                if(count.get() < thresh) continue;
                String doc1 = docpair.getLeftElement();
                String doc2 = docpair.getRightElement();
                if(!doc2docmap.containsKey(doc1)){
                    doc2docmap.put(doc1, new HashSet<String>());
                }
                if(!doc2docmap.containsKey(doc2)){
                    doc2docmap.put(doc2, new HashSet<String>());
                }
                doc2docmap.get(doc1).add(doc2);
                doc2docmap.get(doc2).add(doc1);
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
    
    
    
    public GetDocComponents() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GetDocComponents(), args);
    }
}
