package wikiduper.analysis;



import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
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

import wikiduper.utils.MergeClusters;

public class ClassifyClusters extends Configured implements Tool {
    public static enum ClusterTypes {
        NOT_SIMILAR, FACTUAL_DRIFT, TEMPLATE, REFERENCE, COPY_EDIT, OTHER, IDENTICAL
    };  
    
    private static final Logger LOG = Logger.getLogger(MergeClusters.class);
    //NOT_SIMILAR, FACTUAL_DRIFT, TEMPLATE, REFERENCE, COPY_EDIT, OTHER, IDENTICAL
    private static final String INPUT = "input";
    private static final String N = "n";
    private static final String NCLUSTERS = "nClusters";
    private static final String CLASSIFY_OUT = "classify_out";

    private static final boolean DEBUG = false;
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {

            Options options = new Options();
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("minhash pipeline output").create(INPUT));
            options.addOption(OptionBuilder.withArgName("number")
                    .hasArg().withDescription("number of samples").create(N));
            options.addOption(OptionBuilder.withArgName("number")
                    .hasArg().withDescription("number of clusters").create(NCLUSTERS));
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("output file for classification").create(CLASSIFY_OUT));

            CommandLine cmdline;
            CommandLineParser parser = new GnuParser();
            try {
                cmdline = parser.parse(options, args);
            } catch (ParseException exp) {
                System.err.println("Error parsing command line: " + exp.getMessage());
                return -1;
            }

            if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(CLASSIFY_OUT)
                    || !cmdline.hasOption(N) || !cmdline.hasOption(NCLUSTERS)){
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(120);
                formatter.printHelp(this.getClass().getName(), options);
                ToolRunner.printGenericCommandUsage(System.out);
                return -1;
            }

            String inputPath = cmdline.getOptionValue(INPUT);
            String classifyOutputPath = cmdline.getOptionValue(CLASSIFY_OUT);
            int n = Integer.parseInt(cmdline.getOptionValue(N));
            int nClusters = Integer.parseInt(cmdline.getOptionValue(NCLUSTERS));
            
            LOG.info("Tool name: " + this.getClass().getName());
            
            JobConf conf = new JobConf(getConf(), MergeClusters.class);
            conf.set(INPUT, inputPath);
            conf.set(CLASSIFY_OUT, classifyOutputPath);
            conf.setInt(N, n);
            conf.setInt(NCLUSTERS, nClusters);
            getClusterClassifications(conf);//inputPath,conf);

            return 0;
        }
    
    public void getClusterClassifications(JobConf conf){
        int typecounts[];
        ClusterTypes clist[] = ClusterTypes.values();
        typecounts = new int[clist.length];
        for(int i=0; i<clist.length;i++){
            typecounts[i] = 0;
        }
        
        String filein = conf.get(INPUT);
        String classifyOut = conf.get(CLASSIFY_OUT);
        int n = conf.getInt(N, 1000);
        int nClusters = conf.getInt(NCLUSTERS, -1);
        
        Random r = new Random();
        HashSet<Integer> sampleSet = new HashSet<Integer>();
        int ct = 0;
        while(ct<n){
            int s = r.nextInt(nClusters);
            sampleSet.add(s);
            ct++;
        }

        // Sets to keep track of overall unique title and sentences
        HashSet<String> titleset = new HashSet<String>();
        HashSet<String> sentenceset = new HashSet<String>();
        
        // Per cluster data structures
        ArrayList<String> cluster = new ArrayList<String>();
        HashSet<String> clustertitles = new HashSet<String>();
        TreeSet<String> clustersentences = new TreeSet<String>();
        TreeSet<String> clustertitlesentences = new TreeSet<String>();
        HashMap<String,Integer> clusterwordct = new HashMap<String,Integer>();
        
        int clusterct = 0;
        int linect = 0;
        int clustcurr = -1;
        int maxclustersize = 0;
        Pattern linepat = Pattern.compile("^([^\t]+)\t(.*)$");

        int templateCt = 0;
        int identicalCt = 0;
        int otherCt = 0;
        int speciesCt = 0;
        int otherCt2 = 0;
        
        
        IntWritable clusterid = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Writer classifyWriter  = SequenceFile.createWriter(conf, Writer.file(new Path(classifyOut)),
                    Writer.keyClass(IntWritable.class), Writer.valueClass(IntWritable.class));

            
        System.out.println("filein = " + filein);
        FileStatus[] infiles = fs.globStatus(new Path(filein + "/part-*"));
        for(FileStatus filestatus : infiles){
            System.out.println(filestatus.getPath().toString());
            try{
                FSDataInputStream in = fs.open(filestatus.getPath());
                SequenceFile.Reader reader;
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.stream(in));
                clusterid = new IntWritable();
                Text articlesentence = new Text();
            
            while(reader.next(clusterid, articlesentence)){
                String linetext = articlesentence.toString()
                        .replace("\n", " ")
                        .replaceAll(" ?\\[?http\\S+", "");
                Matcher m = linepat.matcher(linetext);
                String title = "";
                String sentence = "";
                if(m.matches()){
                    title = m.group(1);
                    sentence = m.group(2);

                    if(clustcurr == -1){
                        clustcurr = clusterid.get();  
                    }
                    //if(clusterct > 197000){
                      //  System.out.println("articlesentence " + articlesentence.toString());
                        //System.out.println("sentence = " + title + " " + sentence);
                    //}
                    
                    if(!(clustcurr == clusterid.get())){
                        if(clusterct % 10000 == 0) System.err.println("clusterct = " + clusterct);
                        // Once we've found a new cluster Update each histogram
                        
                        
                        
                        int size = cluster.size();
                        if(size > maxclustersize){
                            maxclustersize = size;
                        }
                        if(sampleSet.contains(clustcurr)){
                            int classification = 0;
                            if(clustersentences.size() > 1){
                                for(String s : clustertitlesentences){
                                    System.out.println(s);

                                }
                                classification = classifyCluster(typecounts,clusterct);
                                if(classification == -1){
                                    classifyWriter.close();
                                    for(int i=0;i<typecounts.length;i++){
                                        System.out.println(clist[i] + " " + typecounts[i]);
                                    }
                                    System.out.println("Number of clusters analyzed: " + clusterct);
                                    System.exit(-1);
                                }
                            }else{
                                // sentences are identical
                                typecounts[ClusterTypes.IDENTICAL.ordinal()] += 1;
                                classification = ClusterTypes.IDENTICAL.ordinal();
                            }
                            IntWritable classOut = new IntWritable();
                            IntWritable clusterOut = new IntWritable();
                            clusterOut.set(clustcurr);
                            classOut.set(classification);
                            classifyWriter.append(clusterOut, classOut);
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
                    clustertitlesentences.add(sentence + " [" + title + "]");
                    clustertitles.add(title);
                    
                    titleset.add(title);
                    sentenceset.add(sentence);
                }else{
                    System.err.println("Bad line " + linect + " : " + articlesentence.toString());
                    System.exit(-1);
               }

                
                linect++;
                clusterid = new IntWritable();
                articlesentence = new Text();

            }
            reader.close();
          }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
          }
            if(sampleSet.contains(clustcurr)){
          // Update one time at the end of each file input loop to add remaining cluster            
            int classification = 0;
            if(clustersentences.size() > 1){
                for(String s : clustertitlesentences){
                    System.out.println(s);

                }
                classification = classifyCluster(typecounts,clusterct);
                if(classification == -1){
                    classifyWriter.close();
                    for(int i=0;i<typecounts.length;i++){
                        System.out.println(clist[i] + " " + typecounts[i]);
                    }
                    System.out.println("Number of clusters analyzed: " + clusterct);
                    System.exit(-1);
                }
            }else{
                // sentences are identical
                typecounts[ClusterTypes.IDENTICAL.ordinal()] += 1;
                classification = ClusterTypes.IDENTICAL.ordinal();
            }
            IntWritable classOut = new IntWritable();
            IntWritable clusterOut = new IntWritable();
            clusterOut.set(clustcurr);
            classOut.set(classification);
            classifyWriter.append(clusterOut, classOut);
            
            }
            // Clear per cluster data structures
            cluster.clear();
            clustersentences.clear();
            clustertitlesentences.clear();
            clustertitles.clear();
            clusterwordct.clear();
            clusterct++;
        }
        
        classifyWriter.close();

        System.out.println("N lines: " + linect);
        System.out.println("N clusters: " + clusterct);            
        System.out.println("N unique titles: " + titleset.size());
        System.out.println("N unique sentences: " + sentenceset.size());
        
        System.out.println("N identical clusters: " + identicalCt);
        System.out.println("N template clusters: " + templateCt);
        System.out.println("N other clusters: " + otherCt);
        System.out.println("N other pair clusters: " + otherCt2);
        System.out.println("N species clusters: " + speciesCt);
        
        
    }catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    }
    

    public static void promptTypes(ClusterTypes clist[]){
        for(int i=0; i< clist.length - 1; i++){
            ClusterTypes type = clist[i];
            System.out.println((i+1) + " " + type);
        }
        System.out.println("\"q\" to exit");

    }
    
    public int classifyCluster(int typecounts[],int clusterct) throws IOException{
        String input;
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        ClusterTypes clist[] = ClusterTypes.values();
        promptTypes(clist);
        int type = -1;
        while(true){
            input = stdin.readLine();
            if(input != null && !(input.equals("q"))) {
                type = -1;
                try{
                    type = Integer.parseInt(input) - 1;
                    typecounts[type]++;
                    break;
                 }catch(NumberFormatException e){
                     System.out.println(input + " is not an number. Reenter");
                 }catch(ArrayIndexOutOfBoundsException e){
                     System.out.println(input + " is out of range. Reenter");
                 }
            }else{
                break;
            }
        }
        return type;

    }


    
    public ClassifyClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ClassifyClusters(), args);
    }
}
