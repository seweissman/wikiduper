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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStringInt;

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
        LOG.info(" - output file: " + outputPath);
        
        JobConf conf = new JobConf(getConf(), MergeClusters.class);

        /* Get Clusters from MinhashWikipediaPages pair output */
        
        getClusters(inputPath,conf,outputPath);

        return 0;
    }

    // Reads in pairs from MinhahsWikipediaPages output and performs connected component analysis
    // Creates a global cluster numbering and a map from doc numbers to sentences and their cluster numbers
    // Writes the docmap to docmapFile
    static final Pattern sentencepattern = Pattern.compile(".*\\[(.+), (.+), (.+)\\].*");
    public static void getClusters(String filein, JobConf conf, String docmapFile){
        
        try {
            TreeMap<Integer, HashSet<PairOfLongs>> clustermap = new TreeMap<Integer, HashSet<PairOfLongs>>();
            //TreeMap<Integer, HashSet<ArrayListOfLongsWritable>> clustermap = new TreeMap<Integer, HashSet<ArrayListOfLongsWritable>>();
            // map from doc id to sentence numbers
            TreeMap<PairOfLongs, TreeSet<PairOfLongs>> docmap = new TreeMap<PairOfLongs, TreeSet<PairOfLongs>>();
            readBuckets4(filein,conf,clustermap);
            
            // Renumber components
            int componentct = 0;
            for(Integer cnum : clustermap.keySet()){
                HashSet<PairOfLongs> comp = clustermap.get(cnum);
                //HashSet<ArrayListOfLongsWritable> comp = clustermap.get(cnum);
                for(PairOfLongs p : comp){
                //for(ArrayListOfLongsWritable p : comp){
                    
                    //Matcher m = sentencepattern.matcher(p);
                    //;System.out.println(">>>>"+p+"<<<<< " + m.matches() + " " + m.groupCount());
                    //if(m.matches()){
                       // System.out.println(">>>>"+p+"<<<<< " + m.groupCount());
                       // System.out.println(m.group(1));// + " " + m.group(2) + " " + m.group(3));
                    long docid = p.getLeftElement();
                    long sentencenum = p.getRightElement() >> 1;
                    long lang = (p.getRightElement()&1);
                    /*    
                    long docid = Long.parseLong(m.group(1));
                        long sentencenum = Long.parseLong(m.group(2));
                        long lang = Long.parseLong(m.group(3));
                        */
                    /*
                    long docid = p.get(0);
                    long sentencenum = p.get(1);
                    long lang = p.get(2);
                    */
                        PairOfLongs doclang = new PairOfLongs();
                        doclang.set(docid, lang);
                        if(!docmap.containsKey(doclang)){
                            docmap.put(doclang, new TreeSet<PairOfLongs>());
                        }
                        docmap.get(doclang).add(new PairOfLongs(sentencenum, componentct));
                    //}
                }
                componentct++;

            }

            FileSystem fs = FileSystem.get(conf);
            Path clustersOut = new Path(docmapFile);
            FileSystem.get(conf).delete(clustersOut, true);
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
                    SequenceFile.Writer.file(clustersOut), 
                    SequenceFile.Writer.keyClass(PairOfLongs.class), 
                    SequenceFile.Writer.valueClass(ArrayListWritable.class));
            ArrayListWritable<PairOfLongs> sentlist;
            PairOfLongs doc;
            for(PairOfLongs doclang : docmap.navigableKeySet()){
                doc = new PairOfLongs();
                sentlist = new ArrayListWritable<PairOfLongs>();
                doc.set(doclang.getLeftElement(),doclang.getRightElement());
                for(PairOfLongs sentcomp : docmap.get(doclang)){
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

    public static void readBuckets4(String filein, JobConf conf, TreeMap<Integer, HashSet<PairOfLongs>> cluster2sentencemap){
        HashMap<PairOfLongs, Integer> sentence2clustermap = new HashMap<PairOfLongs,Integer>();
        try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        int clusterct = 0;
        long ct = 0;
        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
            IntWritable bucket = new IntWritable();
            
            ArrayListWritable<PairOfLongs> sentenceList = new ArrayListWritable<PairOfLongs>();
            HashSet<Integer> clusterSet = new HashSet<Integer>();
            while(reader.next(bucket, sentenceList)){
                //System.out.println("cluster2sentencemap");
                //System.out.println("\t" + cluster2sentencemap.keySet());
                
                //System.out.println("sentence2clustermap");
                //System.out.println("\t" + sentence2clustermap.keySet());
                ct++;
                if(ct % 1000 == 0) System.out.println("Count:"+ct);
                if(ct % 1000 == 0) System.out.println("\t"+cluster2sentencemap.keySet().size());
                if(ct % 1000 == 0) System.out.println("\t"+sentence2clustermap.keySet().size());
                if(ct % 1000 == 0) System.out.println("\t"+sentenceList.size());
                clusterSet.clear();
                //System.out.println("Sentencelist " + sentenceList);
                for(PairOfLongs docsentence : sentenceList){
                    if(sentence2clustermap.containsKey(docsentence)){
                       clusterSet.add(sentence2clustermap.get(docsentence));
                    }
                }
                if(ct % 1000 == 0) System.out.println("\t"+clusterSet.size());
                //System.out.println("Cluster set" + clusterSet);
                cluster2sentencemap.put(clusterct, new HashSet<PairOfLongs>());
                if(!clusterSet.isEmpty()){
                    for(int cluster : clusterSet){
                        // for each cluster merge the sentences into a new cluster
                        for(PairOfLongs docsentence : cluster2sentencemap.get(cluster)){
                            cluster2sentencemap.get(clusterct).add(docsentence);
                            sentence2clustermap.put(docsentence, clusterct);
                        }
                        // Remove the old cluster from cluster2sentencemap
                        cluster2sentencemap.remove(cluster);
                    }
                }
                // Add all of the docsentences in the current list to the new cluster
                cluster2sentencemap.get(clusterct).addAll(sentenceList);
                for(PairOfLongs docsentence : sentenceList){
                    sentence2clustermap.put(docsentence, clusterct);
                }
                //bucket = new ArrayListOfLongsWritable();
                sentenceList = new ArrayListWritable<PairOfLongs>();
                clusterct++;
                
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
    
    public static void readBuckets2(String filein, JobConf conf, TreeMap<Integer, HashSet<String>> cluster2sentencemap){
        HashMap<String, Integer> sentence2clustermap = new HashMap<String,Integer>();
        try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        int clusterct = 0;
        long ct = 0;
        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
            IntWritable bucket = new IntWritable();
            
            ArrayListWritable<DocSentence> sentenceList = new ArrayListWritable<DocSentence>();
            HashSet<Integer> clusterSet = new HashSet<Integer>();
            while(reader.next(bucket, sentenceList)){
                //System.out.println("cluster2sentencemap");
                //System.out.println("\t" + cluster2sentencemap.keySet());
                
                //System.out.println("sentence2clustermap");
                //System.out.println("\t" + sentence2clustermap.keySet());
                
                
                ct++;
                if(ct % 1000 == 0) System.out.println("Count:"+ct);
                if(ct % 1000 == 0) System.out.println("\t"+cluster2sentencemap.keySet().size());
                if(ct % 1000 == 0) System.out.println("\t"+sentence2clustermap.keySet().size());
                if(ct % 1000 == 0) System.out.println("\t"+sentenceList.size());
                clusterSet.clear();
                //System.out.println("Sentencelist " + sentenceList);
                for(DocSentence docsentence : sentenceList){
                    if(sentence2clustermap.containsKey(docsentence.toString())){
                       clusterSet.add(sentence2clustermap.get(docsentence.toString()));
                    }
                }
                if(ct % 1000 == 0) System.out.println("\t"+clusterSet.size());
                //System.out.println("Cluster set" + clusterSet);
                cluster2sentencemap.put(clusterct, new HashSet<String>());
                if(!clusterSet.isEmpty()){
                    for(int cluster : clusterSet){
                        // for each cluster merge the sentences into a new cluster
                        for(String docsentence : cluster2sentencemap.get(cluster)){
                            cluster2sentencemap.get(clusterct).add(docsentence);
                            sentence2clustermap.put(docsentence, clusterct);
                        }
                        // Remove the old cluster from cluster2sentencemap
                        cluster2sentencemap.remove(cluster);

                    }
                }
                // Add all of the docsentences in the current list to the new cluster
                for(DocSentence docsentence : sentenceList){
                    cluster2sentencemap.get(clusterct).add(docsentence.toString());    
                }
                
                for(DocSentence docsentence : sentenceList){
                    sentence2clustermap.put(docsentence.toString(), clusterct);
                }
                //bucket = new ArrayListOfLongsWritable();
                //sentenceList = new ArrayListWritable<ArrayListOfLongsWritable>();
                clusterct++;
                sentenceList.clear();
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
    
    
    public static void readBuckets(String filein, JobConf conf, TreeMap<Integer, HashSet<ArrayListOfLongsWritable>> cluster2sentencemap){
        //HashMap<String, Integer> sentence2clustermap = new HashMap<String,Integer>();
        try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        int clusterct = 0;
        long ct = 0;
        for(FileStatus filestatus : infiles){

            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
            ArrayListOfLongsWritable bucket = new ArrayListOfLongsWritable();
            ArrayListWritable<ArrayListOfLongsWritable> sentenceList = new ArrayListWritable<ArrayListOfLongsWritable>();
            //HashSet<Integer> clusterSet = new HashSet<Integer>();
            while(reader.next(bucket, sentenceList)){
                

                cluster2sentencemap.put(clusterct, new HashSet<ArrayListOfLongsWritable>());
                cluster2sentencemap.get(clusterct).addAll(sentenceList);
                sentenceList = new ArrayListWritable<ArrayListOfLongsWritable>();
                clusterct++;
            }
             
            System.out.println("N clusters " + cluster2sentencemap.keySet().size());
            boolean changes = true;
            while(changes){
                changes = false;
                for(int i=0; i<clusterct;i++){
                    HashSet<ArrayListOfLongsWritable> cluster1 = cluster2sentencemap.get(i);
                    for(int j=i+1; j<clusterct;j++){
                        HashSet<ArrayListOfLongsWritable> cluster2 = cluster2sentencemap.get(j);
                        boolean mergecluster = false;
                        for(ArrayListOfLongsWritable sig : cluster2){
                            if(cluster1.contains(sig)){
                                mergecluster = true;
                                break;
                            }
                        }
                        if(mergecluster){
                            cluster1.addAll(cluster2);
                            cluster2.clear();
                            changes = true;
                        }
                    }
                }
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
    

    public static void readBuckets3(String filein, JobConf conf, TreeMap<Integer, HashSet<String>> cluster2sentencemap){
        //HashMap<String, Integer> sentence2clustermap = new HashMap<String,Integer>();
        try {
        FileSystem fs = FileSystem.get(conf);
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        int clusterct = 0;
        long ct = 0;
        for(FileStatus filestatus : infiles){

            System.out.println(filestatus.getPath().toString());
            try{
            FSDataInputStream in = fs.open(filestatus.getPath());
            SequenceFile.Reader reader;
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
            ArrayListOfLongsWritable bucket = new ArrayListOfLongsWritable();
            ArrayListWritable<ArrayListOfLongsWritable> sentenceList = new ArrayListWritable<ArrayListOfLongsWritable>();
            //HashSet<Integer> clusterSet = new HashSet<Integer>();
            HashSet<Integer> mergeClusters = new HashSet<Integer>();                
            while(reader.next(bucket, sentenceList)){
                ct++;
                if(ct%1000 == 0) System.out.println("Count:"+ct);
                HashSet<String> newSet = new HashSet<String>();
                for(ArrayListOfLongsWritable sentence : sentenceList){
                    newSet.add(sentence.toString());
                }
                
                mergeClusters.clear();
                for(int c : cluster2sentencemap.keySet()){
                    HashSet<String> cset = cluster2sentencemap.get(c);
                    for(String snew : newSet){
                        if(cset.contains(snew)){
                            mergeClusters.add(c);
                            break;
                        }
                    }
                    
                }
                
                for(int c : mergeClusters){
                    newSet.addAll(cluster2sentencemap.get(c));
                    cluster2sentencemap.remove(c);
                }

                cluster2sentencemap.put(clusterct, newSet);

                //sentenceList = new ArrayListWritable<ArrayListOfLongsWritable>();
                clusterct++;
            }
             
            
            reader.close();
          }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
          }
            
            System.out.println("N clusters " + cluster2sentencemap.keySet().size());
            boolean changes = true;
            System.out.println("cluster ct " + clusterct);
            while(changes){
                changes = false;
                for(int i=0; i<clusterct;i++){
                    HashSet<String> cluster1 = cluster2sentencemap.get(i);
                    //System.out.println("cluster1 " + cluster1);
                    for(int j=i+1; j<clusterct;j++){
                        HashSet<String> cluster2 = cluster2sentencemap.get(j);
                        //System.out.println("cluster2 " + cluster2);
                        boolean mergecluster = false;
                        for(String sig : cluster2){
                            if(cluster1.contains(sig)){
                                mergecluster = true;
                                break;
                            }
                        }
                        if(mergecluster){
                            System.out.println("Merging " + i + " " + j);
                            cluster1.addAll(cluster2);
                            cluster2.clear();
                            changes = true;
                        }
                    }
                }
            }
            
            for(int key : cluster2sentencemap.keySet()){
                if(cluster2sentencemap.get(key).size() == 0){
                    cluster2sentencemap.remove(key);
                }
            }

        }
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
