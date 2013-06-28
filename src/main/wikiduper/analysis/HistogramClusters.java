package wikiduper.analysis;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HistogramClusters {

    public static void main(String args[]){
		
        int thresh = 30;
        
	    if(args.length != 1){
	        System.out.println("Usage: HistogramClusters <cluster output file>n");
			System.exit(-1);
	    }
		Pattern linepat = Pattern.compile("([0-9]+)\t(.*)\t(.*)$");
		    
		FileInputStream fin;
		
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
		try {

            File filedir = new File(args[0]);
            String line;

            for(File file : filedir.listFiles()){
                if(file.getName().startsWith("_")) continue;
                System.err.println("Reading file " + file.getName() + "...");
                fin = new FileInputStream(file);
                BufferedReader bin = new BufferedReader(new InputStreamReader(fin));
                
                String clustcurr = null;
                while((line = bin.readLine()) != null){
                    linect++;
	                Matcher m = linepat.matcher(line);
	                String clust = "";
	                String title = "";
	                String sentence = "";
	                if(m.matches()){
	                    clust = m.group(1);
	                    title = m.group(2);
	                    sentence = m.group(3);

                    }else{
                        System.err.println("Bad line: " + line);
                        System.exit(-1);
                    }

					if(clustcurr == null){
	                      clustcurr = clust;  
	                }
					
	                if(!clustcurr.equals(clust)){
	                    if(clusterct % 10000 == 0) System.err.println("clusterct = " + clusterct);

	                    // Once we've found a new cluster Update each histogram
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
	                
	                clustcurr = clust;
                    cluster.add(line);
                    clustersentences.add(sentence);
                    clustertitles.add(title);
                    
                    titleset.add(title);
                    sentenceset.add(sentence);
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

	            bin.close();
	            fin.close();
            }
		 	 } catch (FileNotFoundException e) {
		 		// TODO Auto-generated catch block
		 		e.printStackTrace();
		 	 } catch (IOException e) {
		 			// TODO Auto-generated catch block
		 			e.printStackTrace();
		     }
		    
        System.out.println("N lines: " + linect);
		System.out.println("N clusters: " + clusterct);            
		System.out.println("N unique titles: " + titleset.size());
		System.out.println("N unique sentences: " + sentenceset.size());

        StringBuffer histvals = new StringBuffer();
        StringBuffer histkeys = new StringBuffer();
		
        System.out.println("Cluster histogram");
        int binsize = 30;
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
        
        System.out.println("N Lte " + thresh + ": " + ltex + " " + (1.0*ltex/clusterct));
        System.out.println("N gt " + thresh + ": " + gtx + " " + (1.0*gtx/clusterct));
        
        System.out.println("Number of lines in big clusters = " + bigclusterlinect + " " + (1.0*bigclusterlinect/linect));
        System.out.println("Number of lines in small clusters = " + smallclusterlinect + " " + (1.0*smallclusterlinect/linect));

        int max = 1501;
        for(int i=1;i<max;i++){
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
        for(int i=max;i<=histogram.lastKey();i++){
            int b = histogram.containsKey(i) ? histogram.get(i) : 0;
            sum+=b;
            b = sentencehistogram.containsKey(i) ? sentencehistogram.get(i) : 0;
            sumsentence+=b;
            b = titlehistogram.containsKey(i) ? titlehistogram.get(i) : 0;
            sumtitle+=b;
        }
        int rangel = max;
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

	}


}
