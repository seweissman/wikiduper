package wikiduper.analysis;



import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.pair.PairOfStrings;


import wikiduper.utils.MergeClusters;

/**
 * A class for heuristically classifying clusters as templates, identical
 * 
 * 
 * @author weissman
 *
 */
public class TemplateClusters extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MergeClusters.class);

    private static final String INPUT = "input";
    private static final String GT_IN = "gt_classification";
    private static final String TEMPLATE_OUT = "template_out";
    private static final String IDENTICAL_OUT = "identical_out";
    private static final String OTHER_OUT = "other_out";
    private static final String SCORES_OUT = "scores_out";

    private static final String SCORE_THRESH = "score_threshold";
    private static final String COUNT_THRESH = "count_threshold";
    private static final boolean DEBUG = false;
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {

            Options options = new Options();
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("minhash pipeline output").create(INPUT));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("cluster classification input").create(GT_IN));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("output file for template clusters").create(TEMPLATE_OUT));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("output file for identical clusters").create(IDENTICAL_OUT));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("output file for other clusters").create(OTHER_OUT));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("output file for scores").create(SCORES_OUT));
            options.addOption(OptionBuilder.withArgName("number")
                    .hasArg().withDescription("score threshold").create(SCORE_THRESH));
            options.addOption(OptionBuilder.withArgName("number")
                    .hasArg().withDescription("score threshold").create(COUNT_THRESH));

            CommandLine cmdline;
            CommandLineParser parser = new GnuParser();
            try {
                cmdline = parser.parse(options, args);
            } catch (ParseException exp) {
                System.err.println("Error parsing command line: " + exp.getMessage());
                return -1;
            }

            if (!cmdline.hasOption(INPUT) 
                    || !cmdline.hasOption(TEMPLATE_OUT) || !cmdline.hasOption(IDENTICAL_OUT)  || !cmdline.hasOption(OTHER_OUT) 
                    || !cmdline.hasOption(SCORES_OUT) 
                    || !cmdline.hasOption(SCORE_THRESH) || !cmdline.hasOption(COUNT_THRESH)){
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(120);
                formatter.printHelp(this.getClass().getName(), options);
                ToolRunner.printGenericCommandUsage(System.out);
                return -1;
            }

            String inputPath = cmdline.getOptionValue(INPUT);
            String templateOutputPath = cmdline.getOptionValue(TEMPLATE_OUT);
            String identicalOutputPath = cmdline.getOptionValue(IDENTICAL_OUT);
            String otherOutputPath = cmdline.getOptionValue(OTHER_OUT);
            String scoresOutputPath = cmdline.getOptionValue(SCORES_OUT);

            float score_threshold = Float.parseFloat(cmdline.getOptionValue(SCORE_THRESH));
            int count_threshold = Integer.parseInt(cmdline.getOptionValue(COUNT_THRESH));
            
            LOG.info("Tool name: " + this.getClass().getName());
            
            JobConf conf = new JobConf(getConf(), MergeClusters.class);

            // GT comparison is optional
            String gtInputPath = null;
            if(cmdline.hasOption(GT_IN)){ 
                gtInputPath = cmdline.getOptionValue(GT_IN);
                conf.set(GT_IN, gtInputPath);
            }
            conf.set("mapred.reduce.child.java.opts", "-Xmx6144m");
            conf.set(INPUT, inputPath);
            conf.set(TEMPLATE_OUT, templateOutputPath);
            conf.set(IDENTICAL_OUT, identicalOutputPath);
            conf.set(OTHER_OUT, otherOutputPath);
            conf.set(SCORES_OUT, scoresOutputPath);
            
            conf.setFloat(SCORE_THRESH, score_threshold);
            conf.setInt(COUNT_THRESH, count_threshold);

            getHistogram(conf);//inputPath,conf);

            return 0;
        }
    
    public void getHistogram(JobConf conf){
        String filein = conf.get(INPUT);
        String gtin = conf.get(GT_IN);
        String templateOut = conf.get(TEMPLATE_OUT);
        String identicalOut = conf.get(IDENTICAL_OUT);
        String otherOut = conf.get(OTHER_OUT);
        String scoresOut = conf.get(SCORES_OUT);

        System.out.println("scoresOut " + scoresOut);
        float score_threshold = conf.getFloat(SCORE_THRESH, .75f);
        float count_threshold = conf.getInt(COUNT_THRESH, 3);
        
        // Sets to keep track of overall unique title and sentences
        //HashSet<String> titleset = new HashSet<String>();
        //HashSet<String> sentenceset = new HashSet<String>();
        
        // Per cluster data structures
        ArrayList<String> cluster = new ArrayList<String>();
        HashSet<String> clustertitles = new HashSet<String>();
        TreeSet<String> clustersentences = new TreeSet<String>();
        TreeSet<String> clustertitlesentences = new TreeSet<String>();
        HashMap<String,Integer> clusterwordct = new HashMap<String,Integer>();
        
        int clusterct = 0;
        int linect = 0;
        long clustcurr = -1;
        int maxclustersize = 0;
        //Pattern linepat = Pattern.compile("^([^\t]+)\t(.*)$");
        //Pattern linepat = Pattern.compile("^([^\t]+)\t((?>\\P{M}\\p{M}*)+)$");
        
        int templateCt = 0;
        int templateSentencesCt = 0;
        int nontemplateSentencesCt = 0;
        int identicalCt = 0;
        int otherCt = 0;
        int otherCt2 = 0;
        
        
        LongWritable clusterid = null;
        ClassifyClusters.ClusterTypes clist[] = ClassifyClusters.ClusterTypes.values();
        try {
            
            HashMap<Integer,Integer> clustercategorymap = null;
            if(gtin != null){
                clustercategorymap = readGtData(gtin, conf);
            }
            clustercategorymap = new HashMap<Integer,Integer>();
            
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Writer templateWriter  = SequenceFile.createWriter(conf, Writer.file(new Path(templateOut)),
                    Writer.keyClass(LongWritable.class), Writer.valueClass(IntWritable.class));
            SequenceFile.Writer identicalWriter  = SequenceFile.createWriter(conf, Writer.file(new Path(identicalOut)),
                    Writer.keyClass(LongWritable.class), Writer.valueClass(IntWritable.class));
            SequenceFile.Writer otherWriter  = SequenceFile.createWriter(conf, Writer.file(new Path(otherOut)),
                    Writer.keyClass(LongWritable.class), Writer.valueClass(IntWritable.class));
            SequenceFile.Writer scoresWriter  = SequenceFile.createWriter(conf, Writer.file(new Path(scoresOut)),
                    Writer.keyClass(LongWritable.class), Writer.valueClass(ArrayListOfIntsWritable.class));
            
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
                FSDataInputStream in = fs.open(filestatus.getPath());
                SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
                clusterid = new LongWritable();
                PairOfStrings articlesentence = new PairOfStrings();
            
            while(reader.next(clusterid, articlesentence)){
                String linetext = articlesentence.toString()
                        .replace("\n", " ")
                        .replaceAll(" ?\\[?http\\S+", "");
                String title = "";
                String sentence = "";
                title = articlesentence.getLeftElement();
                sentence = articlesentence.getRightElement();

                if(clustcurr == -1){
                    clustcurr = clusterid.get();  
                }
                    
                if(!(clustcurr == clusterid.get())){
                    if(clusterct % 10000 == 0) System.err.println("clusterct = " + clusterct);
                    // Once we've found a new cluster Update each histogram
                    int size = cluster.size();
                    if(size > maxclustersize){
                        maxclustersize = size;
                    }

                    //getClusterWordCounts(clustersentences,clusterwordct);
                    ArrayList<HashSet<String>> sentencewordmap = new ArrayList<HashSet<String>>();
                    getClusterWordCounts(clustertitlesentences,clusterwordct, sentencewordmap);
                    int category = -1;
                    int isTemplate = -1;

                    if(clustercategorymap.containsKey(clustcurr)){
                        category = clustercategorymap.get(clustcurr);
                        if(category == ClassifyClusters.ClusterTypes.TEMPLATE.ordinal()){
                            isTemplate = 1;
                        }else{
                            isTemplate = 0;
                        }
                    }
                    LongWritable clusterIdOut = new LongWritable();
                    clusterIdOut.set(clustcurr);
                    IntWritable clusterSizeOut = new IntWritable();
                    double score = scoreClusterWords(clusterwordct, sentencewordmap, 
                            clustersentences.size(), clustertitlesentences.size(), clustcurr, 
                            scoresWriter,isTemplate);
                        
                    // identical case
                    if(clustersentences.size() == 1){
                        identicalCt++;
                        nontemplateSentencesCt += clustersentences.size();
                        clusterSizeOut.set(clustersentences.size());
                        identicalWriter.append(clusterIdOut, clusterSizeOut);
                    }else if(clustersentences.size() >= count_threshold && score >= score_threshold){
                        templateCt++;
                        templateSentencesCt += clustersentences.size();
                        clusterSizeOut.set(clustersentences.size());
                        templateWriter.append(clusterIdOut, clusterSizeOut);
                    }else{
                        otherCt++;
                        nontemplateSentencesCt += clustersentences.size();
                        clusterSizeOut.set(clustersentences.size());
                        otherWriter.append(clusterIdOut, clusterSizeOut);
                    }
                    if(DEBUG){
                       System.out.println("Cluster " + clustcurr + "(" + clusterct + ") size: " + cluster.size());
                       System.out.println("score: " + score);
                        /*
                         System.out.println("CLUSTER TYPE TEMPLTE = " + ClassifyClusters.ClusterTypes.TEMPLATE.ordinal());
                            if(category == ClassifyClusters.ClusterTypes.TEMPLATE.ordinal()){
                                System.out.println("Cluster type template: " + category + " " + clist[category]);
                            }else{
                                System.out.println("Cluster type : " + category + " " + clist[category]);
                            }
                            */

                        int i=0;
                        int samplect = 10;
                        for(String s : clustertitlesentences){
                            if(i > samplect) break;
                            System.out.println("sentence = " + s);
                            i++;
                        }
                        System.out.println("\n\n");
                    }

                            // Clear per cluster data structures
                    cluster.clear();
                    clustersentences.clear();
                    clustertitlesentences.clear();
                    clustertitles.clear();
                    clusterwordct.clear();
                    clusterct++;
                }
                    
                clustcurr = clusterid.get();
                cluster.add(articlesentence.toString());
                clustersentences.add(sentence);
                clustertitlesentences.add(title + " " + sentence);
                clustertitles.add(title);
                    
                //titleset.add(title);
                //sentenceset.add(sentence);
                
                linect++;
                clusterid = new LongWritable();
                articlesentence = new PairOfStrings();

            }
            reader.close();
            }catch (EOFException e) {
                // For some reason it doesn't know when the input stream is done??
               }

          // Update one time at the end of each file input loop to add remaining cluster            
            //getClusterWordCounts(clustersentences,clusterwordct);
            LongWritable clusterIdOut = new LongWritable();
            clusterIdOut.set(clustcurr);
            ArrayList<HashSet<String>> sentencewordmap = new ArrayList<HashSet<String>>();
            getClusterWordCounts(clustertitlesentences,clusterwordct, sentencewordmap);
            int category = -1;
            int isTemplate = -1;
            if(clustercategorymap.containsKey(clustcurr)){
                category = clustercategorymap.get(clustcurr);
                if(category == ClassifyClusters.ClusterTypes.TEMPLATE.ordinal()){
                    isTemplate = 1;
                }else{
                    isTemplate = 0;
                }
            }
            double score = scoreClusterWords(clusterwordct, sentencewordmap, clustersentences.size(), clustertitlesentences.size(), clustcurr,
                    scoresWriter, isTemplate);
            
            if(clustersentences.size() == 1){
                identicalCt++;
                nontemplateSentencesCt += clustersentences.size();
                identicalWriter.append(clusterIdOut, clustersentences.size());
            }else if(clustersentences.size() >= count_threshold && score >= score_threshold){
                templateCt++;
                templateSentencesCt += clustersentences.size();
                templateWriter.append(clusterIdOut, clustersentences.size());
            }else{
                otherCt++;
                nontemplateSentencesCt += clustersentences.size();
                otherWriter.append(clusterIdOut, clustersentences.size());
            }

            if(DEBUG){
                System.out.println("Cluster " + clusterct + " size: " + cluster.size());
                System.out.println("score: " + score);
                int i=0;
                int samplect = 10;
                for(String s : clustertitlesentences){
                    if(i > samplect) break;
                    System.out.println("sentence = " + s);
                    i++;
                }
                System.out.println("\n\n");
            }
            
            // Clear per cluster data structures
            cluster.clear();
            clustersentences.clear();
            clustertitlesentences.clear();
            clustertitles.clear();
            clusterwordct.clear();
            clusterct++;
        }
        
        templateWriter.close();
        identicalWriter.close();
        otherWriter.close();
        scoresWriter.close();

        System.out.println("N lines: " + linect);
        System.out.println("N clusters: " + clusterct);            
        //System.out.println("N unique titles: " + titleset.size());
        //System.out.println("N unique sentences: " + sentenceset.size());
        
        System.out.println("N identical clusters: " + identicalCt);
        System.out.println("N template clusters: " + templateCt);
        System.out.println("N template sentences: " + templateSentencesCt);
        System.out.println("N non-template sentences: " + nontemplateSentencesCt);
        System.out.println("N other clusters: " + otherCt);
        System.out.println("N other pair clusters: " + otherCt2);
        
        
        }catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private HashMap<Integer, Integer> readGtData(String gtin, JobConf conf) throws IOException {
        HashMap<Integer,Integer> clustercategorymap = new HashMap<Integer,Integer>();
        FileSystem fs = FileSystem.get(conf);
        IntWritable clusterid = new IntWritable();
        IntWritable category = new IntWritable();
        FSDataInputStream in = fs.open(new Path(gtin));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));

        // What code should look like
        try{
            while(reader.next(clusterid, category)){
                clustercategorymap.put(clusterid.get(), category.get());
            }

        }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
        }
        System.out.println("NUM CLASSIFICATIONS READ: " + clustercategorymap.keySet().size());
        /*
        int lastcluster;
        try{
            reader.next(clusterid, category);
            lastcluster = clusterid.get();
            while(reader.next(clusterid, category)){
                clustercategorymap.put(lastcluster, category.get());
                lastcluster = clusterid.get();
            }
        }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
        }
        */
        reader.close();
        return clustercategorymap;            
    }

    private static void getClusterWordCounts(Set<String> clustersentences,
            HashMap<String, Integer> clusterwordct, ArrayList<HashSet<String>> sentencewordmap) {
        
        Iterator<String> clusterit = clustersentences.iterator();
        while(clusterit.hasNext()){
            HashSet<String> sentencewordset = new HashSet<String>();
            String s = clusterit.next();
            s = s.replaceAll("\\p{Pd}", " ");
            String words[] = s.split("\\s");
            if(words.length < 5) continue;
            //skip first word
            for(int i=1;i<words.length;i++){
                String w = words[i];
                if(w.length() > 30) continue;
                w = w.replaceAll("\\.|,|\\?|!|%|\\$|/|\"|\\(|\\)|\\*","");
                w = w.replaceAll(" ?\\[?http\\S+", "");
                w = w.replace(":", "");
                sentencewordset.add(w);
            }
            
            for(String w : sentencewordset){
                if(!clusterwordct.containsKey(w)) clusterwordct.put(w, 0);
                clusterwordct.put(w, clusterwordct.get(w)+1);
            }
            sentencewordmap.add(sentencewordset);
        }
        
        
    }

    public static double scoreCluster(Set<String> clustersentences){
        double score = 0;
        HashMap<String,Integer> clusterwordct = new HashMap<String,Integer>();
        ArrayList<HashSet<String>> sentencewordmap = new ArrayList<HashSet<String>>();
        getClusterWordCounts(clustersentences, clusterwordct, sentencewordmap);
        score = scoreClusterWords(clusterwordct,sentencewordmap,clustersentences.size());
        return score;
    }
    
    // Some Java 6 -> 7 incompatibility issues here
    static Pattern propernoun = Pattern.compile("\\p{Lu}(\\w|\\p{Lu}|-|—)+('s)?"); //, Pattern.UNICODE_CHARACTER_CLASS);
    private double scoreClusterWordsAlt(HashMap<String, Integer> clusterwordct, ArrayList<HashSet<String>> sentencewordmap, int nSentenceUnique, int nTitleSentenceUnique, 
            int clusterid, SequenceFile.Writer scoresWriter, int isTemplate) throws IOException{
        // TODO Auto-generated method stub
        
        int numberct = 0;
        int properct = 0;
        int otherct = 0;
        HashSet<String> otherSet = new HashSet<String>();
        HashSet<String> numberSet = new HashSet<String>();
        HashSet<String> properSet = new HashSet<String>();
        //if(clusterwordct.get(w) <= nTitleSentenceUnique/2){
        for(String w : clusterwordct.keySet()){
            if(clusterwordct.get(w) <= nSentenceUnique/2){

                w = w.replaceAll("\\.|,|\\?|!|%|\\$|°|#|;","");
                //w = w.replace("-"," ");
                //w = w.replace("—"," ");
                if(w.equals("")) continue;
                if(w.matches("(\\d|½|¼|⅛|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fifteen|twenty|thirty|forty|fifty)+s?")
                        || w.matches("(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)+")
                        || w.matches("[0-9]+(th|st|rd|nd|d)")){
                    numberSet.add(w);
                    if(DEBUG) System.out.println("\tNumeric:\t" +  w);    
                //}else if(w.matches("^[A-Z](\\w|-)+")){
                }else if(propernoun.matcher(w).matches()){
                    properSet.add(w);
                    if(DEBUG) System.out.println("\tProper noun:\t" +  w);    
                }else{
                    otherSet.add(w);
                    if(DEBUG) System.out.println("\tOther:\t" +  w);    
                }
            }
        }
        
        // Count the number of sentences that contain each type of word
        if(DEBUG) System.out.println("numberSet " + numberSet);
        if(DEBUG) System.out.println("properSet " + properSet);
        if(DEBUG) System.out.println("otherSet " + otherSet);
        for(HashSet<String> wordSet : sentencewordmap){
            if(DEBUG) System.out.println("wordSet " + wordSet);
            if(!java.util.Collections.disjoint(wordSet, numberSet)){
                numberct++;
            }
            if(!java.util.Collections.disjoint(wordSet, properSet)){
                properct++;
            }
            if(!java.util.Collections.disjoint(wordSet, otherSet)){
                otherct++;
            }
            
        }
        
        if(DEBUG){
            System.out.println("numberct: " + numberct);
            System.out.println("properct: " + properct);
            System.out.println("otehrct: " + otherct);
        }
        ArrayListOfIntsWritable scoreList = new ArrayListOfIntsWritable();
        LongWritable clusterIdOut = new LongWritable();
        clusterIdOut.set(clusterid);
        int totalct = numberct + properct + otherct;
        scoreList.add(numberct);
        scoreList.add(properct);
        scoreList.add(otherct);
        scoreList.add(nSentenceUnique);
        scoreList.add(nTitleSentenceUnique);
        scoreList.add(totalct);
        scoreList.add(isTemplate);

        scoresWriter.append(clusterIdOut, scoreList);
        double score;
        if(totalct > 0){
            //score = (numberct + properct)*1.0/clustersize; //(numberct + properct + otherct);
            score = (numberct + properct)*1.0/(numberct + properct + otherct);
        }else{
            score = -1;
        }

        return score;
    }

    
    private static double scoreClusterWords(HashMap<String, Integer> clusterwordct, ArrayList<HashSet<String>> sentencewordmap, int nSentenceUnique, int nTitleSentenceUnique, 
            long clustcurr, SequenceFile.Writer scoresWriter, int isTemplate) throws IOException{
        // TODO Auto-generated method stub
        
        int numberct = 0;
        int properct = 0;
        int otherct = 0;
        //if(clusterwordct.get(w) <= nTitleSentenceUnique/2){
        for(String w : clusterwordct.keySet()){
            if(clusterwordct.get(w) <= nSentenceUnique/2){

                w = w.replaceAll("\\.|,|\\?|!|%|\\$|°|#|;","");
                //w = w.replace("-"," ");
                //w = w.replace("—"," ");
                if(w.equals("")) continue;
                if(w.matches("(\\d|½|¼|⅛|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fifteen|twenty|thirty|forty|fifty)+s?")
                        || w.matches("(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)+")
                        || w.matches("[0-9]+(th|st|rd|nd|d)")){
                    numberct++;
                    if(DEBUG) System.out.println("\tNumeric:\t" +  w);    
                //}else if(w.matches("^[A-Z](\\w|-)+")){
                }else if(propernoun.matcher(w).matches()){
                    properct++;
                    if(DEBUG) System.out.println("\tProper noun:\t" +  w);    
                }else{
                    otherct++;
                    if(DEBUG) System.out.println("\tOther:\t" +  w);    
                }
            }
        }
        
        if(DEBUG){
            System.out.println("numberct: " + numberct);
            System.out.println("properct: " + properct);
            System.out.println("otehrct: " + otherct);
        }
        ArrayListOfIntsWritable scoreList = new ArrayListOfIntsWritable();
        LongWritable clusterIdOut = new LongWritable();
        clusterIdOut.set(clustcurr);
        int totalct = numberct + properct + otherct;
        scoreList.add(numberct);
        scoreList.add(properct);
        scoreList.add(otherct);
        scoreList.add(nSentenceUnique);
        scoreList.add(nTitleSentenceUnique);
        scoreList.add(totalct);
        scoreList.add(isTemplate);

        scoresWriter.append(clusterIdOut, scoreList);
        double score;
        if(totalct > 0){
            //score = (numberct + properct)*1.0/clustersize; //(numberct + properct + otherct);
            score = (numberct + properct)*1.0/(numberct + properct + otherct);
        }else{
            score = -1;
        }

        return score;
    }

    private static double scoreClusterWords(HashMap<String, Integer> clusterwordct, ArrayList<HashSet<String>> sentencewordmap, int nSentenceUnique){ 
        int numberct = 0;
        int properct = 0;
        int otherct = 0;
        //if(clusterwordct.get(w) <= nTitleSentenceUnique/2){
        for(String w : clusterwordct.keySet()){
            if(clusterwordct.get(w) <= nSentenceUnique/2){

                w = w.replaceAll("\\.|,|\\?|!|%|\\$|°|#|;","");
                //w = w.replace("-"," ");
                //w = w.replace("—"," ");
                if(w.equals("")) continue;
                if(w.matches("(\\d|½|¼|⅛|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fifteen|twenty|thirty|forty|fifty)+s?")
                        || w.matches("(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)+")
                        || w.matches("[0-9]+(th|st|rd|nd|d)")){
                    numberct++;
                    if(DEBUG) System.out.println("\tNumeric:\t" +  w);    
                //}else if(w.matches("^[A-Z](\\w|-)+")){
                }else if(propernoun.matcher(w).matches()){
                    properct++;
                    if(DEBUG) System.out.println("\tProper noun:\t" +  w);    
                }else{
                    otherct++;
                    if(DEBUG) System.out.println("\tOther:\t" +  w);    
                }
            }
        }
        
        if(DEBUG){
            System.out.println("numberct: " + numberct);
            System.out.println("properct: " + properct);
            System.out.println("otehrct: " + otherct);
        }

        int totalct = numberct + properct + otherct;

        double score;
        if(totalct > 0){
            score = (numberct + properct)*1.0/(numberct + properct + otherct);
        }else{
            score = -1;
        }

        return score;
    }
    
    public TemplateClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TemplateClusters(), args);
    }
}
