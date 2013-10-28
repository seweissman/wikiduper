package wikiduper.analysis;



import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import wikiduper.application.MergeClusters;

public class HistogramClusters extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MergeClusters.class);

    private static final String INPUT = "input";
    private static final String THRESH = "large_cluster_threshold";
    private static final String BINSIZE = "bin_size";
    private static final String MAX = "max_bin";
    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {

            Options options = new Options();
            options.addOption(OptionBuilder.withArgName("path")
                    .hasArg().withDescription("minhash pipeline output").create(INPUT));
            options.addOption(OptionBuilder.withArgName("num")
                    .hasArg().withDescription("large cluster threshold").create(THRESH));
            options.addOption(OptionBuilder.withArgName("num")
                    .hasArg().withDescription("size of histogram bins").create(BINSIZE));
            options.addOption(OptionBuilder.withArgName("num")
                    .hasArg().withDescription("max bin to display in graph").create(MAX));

            CommandLine cmdline;
            CommandLineParser parser = new GnuParser();
            try {
                cmdline = parser.parse(options, args);
            } catch (ParseException exp) {
                System.err.println("Error parsing command line: " + exp.getMessage());
                return -1;
            }

            if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(THRESH) || !cmdline.hasOption(BINSIZE) || 
                    !cmdline.hasOption(MAX)){
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(120);
                formatter.printHelp(this.getClass().getName(), options);
                ToolRunner.printGenericCommandUsage(System.out);
                return -1;
            }

            String inputPath = cmdline.getOptionValue(INPUT);
            int thresh = Integer.parseInt(cmdline.getOptionValue(THRESH)); //30;
            int binsize = Integer.parseInt(cmdline.getOptionValue(BINSIZE));//30;
            int max = Integer.parseInt(cmdline.getOptionValue(MAX)); //30; 1501;
            
            
            LOG.info("Tool name: " + this.getClass().getName());
            
            JobConf conf = new JobConf(getConf(), MergeClusters.class);
            conf.set(INPUT, inputPath);
            conf.setInt(THRESH, thresh);
            conf.setInt(BINSIZE, binsize);
            conf.setInt(MAX, max);

            getHistogram(conf);//inputPath,conf);

            return 0;
        }
    
    public void getHistogram(JobConf conf){
        String filein = conf.get(INPUT);
        int max = conf.getInt(MAX, 30);
        int thresh = conf.getInt(THRESH, 30);
        int binsize = conf.getInt(BINSIZE, 10);
        
        
        //IntWritable, Text
        // Overall histogram: cluster sizes -> cluster cts
        TreeMap<Integer,Integer> histogram = new TreeMap<Integer,Integer>();
        
        // Unique title histogram: # unique titles in cluster -> cluster cts
        TreeMap<Integer,Integer> titlehistogram = new TreeMap<Integer,Integer>();
        
        // Unique sentence histogra: # unique sentences in cluster -> cluster cts
        TreeMap<Integer,Integer> sentencehistogram = new TreeMap<Integer,Integer>();
        
        // Sets to keep track of overall unique title and sentences
        HashSet<String> titleset = new HashSet<String>();
        HashSet<String> sentenceset = new HashSet<String>();
        
        // Per cluster data structures
        ArrayList<String> cluster = new ArrayList<String>();
        HashSet<String> clustertitles = new HashSet<String>();
        HashSet<String> clustersentences = new HashSet<String>();
        int bigclusterlinect = 0;
        int smallclusterlinect = 0;
        int clusterct = 0;
        int linect = 0;
        long clustcurr = -1;
        int maxclustersize = 0;
        //Pattern linepat = Pattern.compile("^([^\t]+)\t(.*)$");
        Pattern linepat = Pattern.compile("^([^\t]+)\t(\\p{L}*)$");

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
            LongWritable clusterid = new LongWritable();
            Text articlesentence = new Text();
            while(reader.next(clusterid, articlesentence)){
                String linetext = articlesentence.toString().replace("\n", " ");
                Matcher m = linepat.matcher(linetext);
                String title = "";
                String sentence = "";
                if(m.matches()){
                    title = m.group(1);
                    sentence = m.group(2);

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
                        if(size > thresh){
                            bigclusterlinect+=size;
                        }else{
                            smallclusterlinect+=size;
                        }
                        
                        if(!histogram.containsKey(size)){
                            histogram.put(size, 0);    
                        }
                        histogram.put(size, histogram.get(size) + 1);
                        
                        size = clustertitles.size();
                        if(!titlehistogram.containsKey(size)){
                            titlehistogram.put(size, 0);    
                        }
                        titlehistogram.put(size, titlehistogram.get(size) + 1);
                        
                        size = clustersentences.size();
                        if(!sentencehistogram.containsKey(size)){
                            sentencehistogram.put(size, 0);    
                        }
                        sentencehistogram.put(size, sentencehistogram.get(size) + 1);
                        
                        /*
                         // Code for examining cluster contents
                        if(cluster.size() > 10000){
                            int ct = 0;
                            System.out.println("Cluster size: " + cluster.size());
                            System.out.println("N unique sentences " +  clustersentences.size());
                            System.out.println("N unique titles " +  clustertitles.size());

                            for(String l : cluster){
                                if(ct > 10) break;
                                System.out.println(l);
                                ct++;
                            }
                            System.out.println();
                        }
                        */
                        
                        // Clear per cluster data structures
                        
                        cluster.clear();
                        clustersentences.clear();
                        clustertitles.clear();
                        clusterct++;
                    }
                    
                    clustcurr = clusterid.get();
                    cluster.add(articlesentence.toString());
                    clustersentences.add(sentence);
                    clustertitles.add(title);
                    
                    titleset.add(title);
                    sentenceset.add(sentence);
                }else{
                    //System.err.println("Bad line " + linect + " : " + articlesentence.toString());
                    System.err.println("Bad line " + linect + " : " + linetext);
                    System.out.println("Bad line " + linect + " : " + linetext);
                    Pattern linepat2 = Pattern.compile(".*([^\t]+).*");
                    Matcher m2 = linepat2.matcher(linetext);
                    if(m2.matches()){
                        System.out.println("Matches linepat2");
                        System.out.println("Group count " + m2.groupCount());
                        System.out.println(m2.group(1));
                    }else{
                        System.out.println("No match");
                    }
                    System.exit(-1);
               }

                
                linect++;
                clusterid = new LongWritable();
                articlesentence = new Text();

            }
            reader.close();
          }catch (EOFException e) {
           // For some reason it doesn't know when the input stream is done??
          }
            
            
            // Update one time at the end of each file input loop to add remaining cluster
            int size = cluster.size();
            
            if(size > thresh){
                bigclusterlinect+=size;
            }else{
                smallclusterlinect+=size;
            }
            
            
            if(!histogram.containsKey(size)){
               histogram.put(size, 0);    
            }
            histogram.put(size, histogram.get(size) + 1);
                
            size = clustertitles.size();
            if(!titlehistogram.containsKey(size)){
                titlehistogram.put(size, 0);    
            }
            titlehistogram.put(size, titlehistogram.get(size) + 1);
            
            size = clustersentences.size();
            if(!sentencehistogram.containsKey(size)){
                sentencehistogram.put(size, 0);    
            }
            sentencehistogram.put(size, sentencehistogram.get(size) + 1);
            
            // Clear per cluster data structures
            cluster.clear();
            clustersentences.clear();
            clustertitles.clear();
            clusterct++;
            clusterct++;
            
            
        }
        
        
        
        
        System.out.println("N lines: " + linect);
        System.out.println("N clusters: " + clusterct);            
        System.out.println("N unique titles: " + titleset.size());
        System.out.println("N unique sentences: " + sentenceset.size());
        
        
        StringBuffer histvals = new StringBuffer();
        StringBuffer histkeys = new StringBuffer();
        
        System.out.println("Cluster histogram");
        
        int sum = 0;
        int sumsentence = 0;
        int sumtitle = 0;
        
        int gtx = 0;
        int ltex = 0;
        for(int b : histogram.keySet()){
            //System.out.println(b + "\t" + histogram.get(b));
            if(b <= thresh){
                ltex += histogram.get(b);
            }else{
                gtx += histogram.get(b);
                
            }
        }
        
        System.out.println("Cluster threshold: " + thresh);
        System.out.println("N Lte " + thresh + ": " + ltex + " " + (1.0*ltex/clusterct));
        System.out.println("N gt " + thresh + ": " + gtx + " " + (1.0*gtx/clusterct));
        
        System.out.println("Number of lines in big clusters = " + bigclusterlinect + " " + (1.0*bigclusterlinect/linect));
        System.out.println("Number of lines in small clusters = " + smallclusterlinect + " " + (1.0*smallclusterlinect/linect));

        System.out.println("Size of largest cluster: " + maxclustersize);
        int binmax = max>0?max:histogram.lastKey();
        for(int i=1;i<=binmax;i++){
            int b = histogram.containsKey(i) ? histogram.get(i) : 0;
            sum+=b;
            b = sentencehistogram.containsKey(i) ? sentencehistogram.get(i) : 0;
            sumsentence+=b;
            b = titlehistogram.containsKey(i) ? titlehistogram.get(i) : 0;
            sumtitle+=b;
            
            if(i%binsize == 0){
                int rangel = i-binsize;
                int rangeh = i-1;
                histkeys.append(",");
                histkeys.append("\"");
                histkeys.append(rangel);
                histkeys.append("-");
                histkeys.append(rangeh);
                histkeys.append("\"");
            histvals.append(",");
//            histvals.append("{");
            histvals.append(sum);
//            histvals.append(",");
//            histvals.append(sumsentence);
//            histvals.append(",");
//            histvals.append(sumtitle);
//            histvals.append("}");

            sum = 0;
            sumsentence = 0;
            sumtitle = 0;
            }
        }
        
        // Calculate rest of distribution weight
        sum = 0;
        sumsentence = 0;
        sumtitle = 0;
        for(int i=binmax;i<=histogram.lastKey();i++){
            int b = histogram.containsKey(i) ? histogram.get(i) : 0;
            sum+=b;
            b = sentencehistogram.containsKey(i) ? sentencehistogram.get(i) : 0;
            sumsentence+=b;
            b = titlehistogram.containsKey(i) ? titlehistogram.get(i) : 0;
            sumtitle+=b;
        }
        int rangel = binmax;
        int rangeh = histogram.lastKey();
        histkeys.append(",");
        histkeys.append("\"");
        histkeys.append(rangel);
        histkeys.append("-");
        histkeys.append(rangeh);
        histkeys.append("\"");
        histvals.append(",");
      histvals.append(sum);
        
        //Mathematic output:
        System.out.println("BarChart[{"+histvals.substring(1)+"}, "
                    +"ChartLabels ->Placed[{(Rotate[#, Pi/4] & /@{"+histkeys.substring(1)+"}), "
                     +"{" + histvals.substring(1) + "}},{Axis,Above}], "
                    +"ScalingFunctions -> \"Log\", BaseStyle -> {FontSize -> 18},ImageSize->Scaled[.6]]");
        histkeys.setLength(0);
        histvals.setLength(0);

        /*
        System.out.println("Unique Title histogram");
        sum = 0;
        for(int i=titlehistogram.firstKey();i<500;i++){
            int b = titlehistogram.containsKey(i) ? titlehistogram.get(i) : 0;
            sum+=b;
            if(i%binsize == 0){
                int rangel = i-binsize;
                int rangeh = i-1;
                histkeys.append(",");
                histkeys.append("\"");
                histkeys.append(rangel);
                histkeys.append("-");
                histkeys.append(rangeh);
                histkeys.append("\"");
            histvals.append(",");
            histvals.append(sum);
//            System.out.println(sum + "\t" + sum);
            sum = 0;
            }
        }

        //Mathematica output
        System.out.println("BarChart[{"+histvals.substring(1)+"}, ChartLabels ->Placed[{"+histkeys.substring(1)+"},Below],ScalingFunctions -> \"Log\", ImageSize -> Full]");
        histkeys.setLength(0);
        histvals.setLength(0);
        
        System.out.println("Uniqe Sentence histogram");
        for(int i=sentencehistogram.firstKey();i<500;i++){
            int b = sentencehistogram.containsKey(i) ? sentencehistogram.get(i) : 0;
            sum+=b;
            if(i%binsize == 0){
                int rangel = i-binsize;
                int rangeh = i-1;
                histkeys.append(",");
                histkeys.append("\"");
                histkeys.append(rangel);
                histkeys.append("-");
                histkeys.append(rangeh);
                histkeys.append("\"");
            histvals.append(",");
            histvals.append(sum);
            //System.out.println(sum + "\t" + sum);
            sum = 0;
            }
        }
        
        //Mathematica output
        System.out.println("BarChart[{"+histvals.substring(1)+"}, ChartLabels ->Placed[{"+histkeys.substring(1)+"},Below],ScalingFunctions -> \"Log\", ImageSize -> Full]");
        histkeys.setLength(0);
        histvals.setLength(0);
        */


    }catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    }
    
    public HistogramClusters() {}

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HistogramClusters(), args);
    }
}
