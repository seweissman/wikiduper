package wikiduper.analysis;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClassifyClusters {
    private static enum ClusterTypes {
        NOT_SIMILAR, FACTUAL_DRIFT, TEMPLATE, REFERENCE, COPY_EDIT, OTHER, IDENTICAL
    };	
    private static int typecounts[];
    private static int clusterct = 0;
    public static void main(String args[]){
		
	    if(args.length != 1){
	        System.out.println("Usage: FilterClusters <cluster output file>n");
			System.exit(-1);
	    }
		Pattern linepat = Pattern.compile("([0-9]+)\t(.*)\t(.*)$");
		    
		FileInputStream fin;
		
		ClusterTypes clist[] = ClusterTypes.values();
		typecounts = new int[clist.length];
	    for(int i=0; i<clist.length;i++){
	        typecounts[i] = 0;
	    }

		try {

            File file = new File(args[0]);
            System.out.println("Reading files " + args[0] + "...");
            fin = new FileInputStream(file);
            BufferedReader bin = new BufferedReader(new InputStreamReader(fin));
	        String line;
	        
	        ArrayList<String> titlelist = new ArrayList<String>();
	        ArrayList<String> sentencelist = new ArrayList<String>();
	        
	        TreeSet<String> sentenceset = new TreeSet<String>();
	        String clustcurr = null;
	        while((line = bin.readLine()) != null){
	                Matcher m = linepat.matcher(line);
	                String clust = "";
	                String title = "";
	                String sentence = "";
	                if(m.matches()){
	                    clust = m.group(1);
	                    title = m.group(2);
	                    sentence = m.group(3);

                    }else{
                        System.out.println("Bad line: " + line);
                        System.exit(-1);
                    }

					if(clustcurr == null){
	                      clustcurr = clust;  
	                }
					
	                if(!clustcurr.equals(clust)){
	                    if(clusterct % 1000 == 0) System.out.println("clusterct = " + clusterct);

	                    if(sentenceset.size() > 1){
	                        for(int i=0;i<sentencelist.size();i++){
	                            String s = sentencelist.get(i);
	                            String t = titlelist.get(i);
	                            System.out.println(s + "\t[" + t + "]");

	                        }
	                        classifyCluster();
	                    }else{
	                        // sentences are identical
	                        typecounts[ClusterTypes.IDENTICAL.ordinal()] += 1;
	                    }
	                    titlelist.clear();
	                    sentencelist.clear();
	                    sentenceset.clear();
                        clusterct++;
	                }
	                
	                clustcurr = clust;
                    titlelist.add(title);
                    sentencelist.add(sentence);
                    sentenceset.add(sentence);
	            }
	              bin.close();
	              fin.close();

		 	 } catch (FileNotFoundException e) {
		 		// TODO Auto-generated catch block
		 		e.printStackTrace();
		 	 } catch (IOException e) {
		 			// TODO Auto-generated catch block
		 			e.printStackTrace();
		     }
		    
		    
            System.out.println("N input buckets: " + clusterct);            
            //System.out.println("Bad pairs: " + badct);
            //System.out.println("N components: " + componentct);

			//System.out.println("FP rate: " + badct*1.0/matchct);

	}

	public static void promptTypes(ClusterTypes clist[]){
	    for(int i=0; i< clist.length - 1; i++){
	        ClusterTypes type = clist[i];
	        System.out.println((i+1) + " " + type);
        }
	    System.out.println("\"q\" to exit");

	}
	
    public static void classifyCluster() throws IOException{
        String input;
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        ClusterTypes clist[] = ClusterTypes.values();
        promptTypes(clist);

        while(true){
            input = stdin.readLine();
            if(input != null && !(input.equals("q"))) {
                int type = -1;
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
        
        if(input.equals("q")){
            for(int i=0;i<typecounts.length;i++){
                System.out.println(clist[i] + " " + typecounts[i]);
            }
            System.out.println("Number of clusters analyzed: " + clusterct);
            System.exit(-1);
        }
    }



	
}
