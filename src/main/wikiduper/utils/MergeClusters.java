package wikiduper.utils;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
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

import wikiduper.utils.DocSentence;
import wikiduper.utils.Signature;
import wikiduper.utils.UnionFindSet;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfLongs;

public class MergeClusters extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(MergeClusters.class);


    private static final String INPUT = "input";
    private static final String OUTPUT = "output";

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("path")
                .hasArg().withDescription("minhash output buckets").create(INPUT));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(OUTPUT) || !cmdline.hasOption(INPUT)){
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String outputPath = cmdline.getOptionValue(OUTPUT);
        String inputPath = cmdline.getOptionValue(INPUT);
        
        LOG.info("Tool name: " + this.getClass().getName());
        LOG.info(" - input file: " + inputPath);
        LOG.info(" - output file: " + outputPath);
        
        JobConf conf = new JobConf(getConf(), MergeClusters.class);

        /* Get Clusters from MinhashWikipediaPages pair output */
        
        getClusters(inputPath,conf,outputPath);

        return 0;
    }

    // Creates a global cluster numbering and a map from doc numbers to sentences and their cluster numbers
    // Writes the docmap to docmapFile
    //static final Pattern sentencepattern = Pattern.compile(".*\\[(.+), (.+), (.+)\\].*");
    public static void getClusters(String filein, JobConf conf, String docmapFile){
        
        try {
            TreeMap<Integer, HashSet<DocSentence>> clustermap = new TreeMap<Integer, HashSet<DocSentence>>();
            //TreeMap<Integer, HashSet<ArrayListOfLongsWritable>> clustermap = new TreeMap<Integer, HashSet<ArrayListOfLongsWritable>>();
            // map from doc id to sentence numbers
            TreeMap<PairOfLongString, TreeSet<PairOfLongs>> docmap = new TreeMap<PairOfLongString, TreeSet<PairOfLongs>>();
            readBuckets(filein,conf,clustermap);
            HashSet<String> langSet = new HashSet<String>();
            // Renumber components
            int componentct = 0;
            FileSystem fs = FileSystem.get(conf);
            Path clustersOut = new Path(docmapFile);
            FileSystem.get(conf).delete(clustersOut, true);
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
                    SequenceFile.Writer.file(clustersOut), 
                    SequenceFile.Writer.keyClass(IntWritable.class), //cluster number 
                    SequenceFile.Writer.valueClass(ArrayListWritable.class)); // List of sentences
            ArrayListWritable<DocSentence> sentlist = new ArrayListWritable<DocSentence>();
            IntWritable cluster = new IntWritable();
            for(Integer cnum : clustermap.keySet()){
                HashSet<DocSentence> comp = clustermap.get(cnum);
                cluster.set(componentct);
                sentlist.clear();
                for(DocSentence p : comp){
                    sentlist.add(p);
                }
                writer.append(cluster,sentlist);
                componentct++;

            }

            writer.close();
            fs.close();
            System.out.println("N components: " + componentct);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void readBuckets(String filein, JobConf conf, TreeMap<Integer, HashSet<DocSentence>> cluster2sentencemap){
        try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        int ct = 0;
        Signature bucket = new Signature();
        UnionFindSet n;
        UnionFindSet currentSet = null;
        HashMap<DocSentence,UnionFindSet> nodeMap = new HashMap<DocSentence,UnionFindSet>(); 
        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));

            Signature lastbucket = null;
            DocSentence ds = new DocSentence();
            long linect = 0;
            long newnodect = 0;
            while(reader.next(bucket, ds)){
                System.out.println("bucket = " + bucket);
                System.out.println("lastbucket = " + lastbucket);
                linect++;
                if(ct % 1000 == 0) System.out.println("Count:"+ct);
                if(linect % 100000 == 0) System.out.println(linect+"\t"+ct+"\t"+newnodect);
                
                if(lastbucket != null && !(bucket.equals(lastbucket))){
                    currentSet = null;
                    ct++;
                }
                
                if(nodeMap.containsKey(ds)){
                    n = nodeMap.get(ds);
                }else{
                    newnodect++;
                    n = new UnionFindSet(ds);
                    nodeMap.put(ds, n);
                }
                if(currentSet == null){
                    currentSet = UnionFindSet.find(n);
                }else{
                    UnionFindSet.merge(currentSet, n);
                }
                nodeMap.put(ds, currentSet);
                
                lastbucket = bucket;
                bucket = new Signature();
                ds = new DocSentence();
                //sentenceList = new ArrayListWritable<ArrayListOfLongsWritable>();
            }
            reader.close();
          }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
          }
        }
        
        System.out.println("Done reading\n");
        //HashMap<DocSentence,HashSet<DocSentence>> clusterMap = new HashMap<DocSentence,HashSet<DocSentence>>();
        HashMap<DocSentence,Integer> clusterNumMap = new HashMap<DocSentence,Integer>();
        int clusterct = 0;
        for(DocSentence ds : nodeMap.keySet()){
            n = nodeMap.get(ds);
            UnionFindSet headn = UnionFindSet.find(n);
            //if(headn == n){
              //  continue;
            //}
            if(!clusterNumMap.containsKey(headn.data)){
                HashSet<DocSentence> newset = new HashSet<DocSentence>();
                newset.add(headn.data);
                clusterNumMap.put(headn.data,clusterct);
                cluster2sentencemap.put(clusterct,newset);
                clusterct++;
            }
            int cnum = clusterNumMap.get(headn.data);
            cluster2sentencemap.get(cnum).add(ds);
        }
/*
        System.out.println("Num clusters " + clusterNumMap.keySet().size());
        for(int c : cluster2sentencemap.keySet()){
            System.out.println(c + " " + cluster2sentencemap.get(c));
        }
*/
        
    }catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }

    }
    
    
    
    public MergeClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MergeClusters(), args);
    }
}
