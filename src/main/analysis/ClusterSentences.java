

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClusterSentences {
	
    static HashSet<String> getConnectedComponent(String entity, TreeMap<String, HashSet<String>> matchmap){
        HashSet<String> component = new HashSet<String>();
        component.add(entity);
        if(matchmap.isEmpty() || !matchmap.containsKey(entity)){
            return component;
        }

        HashSet <String> matches = matchmap.remove(entity);
        for(String m : matches){
            HashSet<String> c = getConnectedComponent(m, matchmap);
            component.addAll(c);
        }
        return component;
    }  
	
	public static void main(String args[]){
		
		  if(args.length != 2){
				System.out.println("Usage: ClusterSentences <filein> <file out>\n");
				System.exit(-1);
			}
		  Pattern linepat = Pattern.compile("(\\[[-0-9, ]+\\])\t\\((.*), \\d+:\\d+\\)");
		    //ArrayList<ArrayList<String>> clusterlist = new ArrayList<ArrayList<String>>();
		  TreeMap<String, HashSet<String>> matchmap = new TreeMap<String, HashSet<String>>();
		    HashMap<String, HashSet<String>> nomatchmap = new HashMap<String, HashSet<String>>();
		    
		    FileInputStream fin;
   		    int clusterct = 0;
            int uniquematchct = 0;
            int uniquefalseposct = 0;
            int nonuniquematchct = 0;
            int nonuniquefalseposct = 0;
		    try {
	              fin = new FileInputStream(args[0]);
	              BufferedReader bin = new BufferedReader(new InputStreamReader(fin));
		      String line;
		      ArrayList<String> cluster = new ArrayList<String>();
		      String sigcurr = null;
			  while((line = bin.readLine()) != null){
                  Matcher m = linepat.matcher(line);
                  String sig = "";
                  String sentence = "";
                  if(m.matches()){
                      sig = m.group(1);
                      sentence = m.group(2);
                      //System.out.println("sig = " + sig + " , sentence = " + sentence);
                    }else{
                        System.out.println("Bad line: " + line);
                        System.exit(-1);
                    }

					if(sigcurr == null){
	                      sigcurr = sig;  
	                }
	                if(!sigcurr.equals(sig)){
	                    clusterct++;
	                    if(clusterct % 1000 == 0) System.out.println("clusterct = " + clusterct);
	                    for(int i=0; i<cluster.size();i++){
	                            String m1 = cluster.get(i);
	                            for(int j=i+1;j<cluster.size();j++){                 
	                                String m2 = cluster.get(j);
	                                if((nomatchmap.containsKey(m1) && nomatchmap.get(m1).contains(m2))
	                                        || (nomatchmap.containsKey(m2) && nomatchmap.get(m2).contains(m1))
	                                        || (matchmap.containsKey(m2) && matchmap.get(m2).contains(m1))
	                                        || (matchmap.containsKey(m1) && matchmap.get(m1).contains(m2))){
	                                    continue;
	                                }
	                                long dl = Math.max(m1.length(), m2.length()) - Math.min(m1.length(), m2.length());
	                                long d = EditDistance.dist(m1,m2);
	                                long score = Math.round(100*(d - dl + 1)*1.0/Math.max(m1.length(), m2.length()));
	                                /*
	                                if(score > 5){
	                                    nonuniquefalseposct++;
	                                    //System.out.println(m1 + ">>>>>>" + m2  + ">>>>>> " + score);
	                                    if(!nomatchmap.containsKey(m1)){
                                            nomatchmap.put(m1, new HashSet<String> ());
                                          }
                                    
                                        if(!nomatchmap.containsKey(m2)){
                                            nomatchmap.put(m2, new HashSet<String> ());
                                            }
                                        if(!(nomatchmap.get(m2).contains(m1) || nomatchmap.get(m1).contains(m2))){
                                            //System.out.println(m1 + "\t" + m2);
                                            uniquefalseposct++;
                                        }

                                        nomatchmap.get(m2).add(m1);
                                        nomatchmap.get(m1).add(m2);
	                                }else{
	                                */
	                                    nonuniquematchct++;
	                                
	                                    if(!matchmap.containsKey(m1)){
                                            matchmap.put(m1, new HashSet<String> ());
                                          }
                                    
                                        if(!matchmap.containsKey(m2)){
                                            matchmap.put(m2, new HashSet<String> ());
                                            }
                                        if(!(matchmap.get(m2).contains(m1) || matchmap.get(m1).contains(m2))){
                                            uniquematchct++;
                                        }
                                        matchmap.get(m2).add(m1);
                                        matchmap.get(m1).add(m2);
	                              //  }
	                            }
	                        }
	                    //clusterlist.add(cluster);
	                    //cluster = new ArrayList<String>();
	                    cluster.clear();
	                }
	                sigcurr = sig;
	                
	                sentence = sentence.replace("External Links", "")
	                        .replace("External links", "")
	                        .replace("References","")
	                        .replace("Official site","")
	                        .replace("official site", "");
	                if(sentence.length() > 100){
	                    cluster.add(sentence);
	                }


		        //System.out.println("entity = " + entity);
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
		    
		    
            System.out.println("Cluster count:" +  clusterct);
            System.out.println("N unique input matchess: " +  uniquematchct);
            System.out.println("N unique input false positives: " + uniquefalseposct);
            System.out.println("N non-unique input matchess: " +  nonuniquematchct);
            System.out.println("N non-unique input false positives: " + nonuniquefalseposct);
            //System.exit(-1);            
		    int componentct = 0;
            long matchct = 0;
            long badct = 0;
            FileOutputStream fout;
            try {
                fout = new FileOutputStream(args[1]);
                BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(fout));
                while(!matchmap.isEmpty()){
                    String entity = matchmap.firstKey();
                    HashSet<String> comp = getConnectedComponent(entity, matchmap);
                    // System.out.println("entity = " + entity);
                    bout.write(comp.toString());

                    bout.write("\n");
                    componentct++;
                    //System.out.println("componentct = " + componentct + " " + comp.size());
                    //System.out.println("matchct = " + matchct + " ,'badct = " + badct);
                }

            	
            	bout.close();
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch blopck
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
		    
		    
            System.out.println("N input buckets: " + clusterct);            
            System.out.println("Total pairs: " + matchct);
            System.out.println("Bad pairs: " + badct);
            System.out.println("N components: " + componentct);

			System.out.println("FP rate: " + badct*1.0/matchct);
		//String s1 = "abcedfgh";
		//String s2 = "bcdefg";
		//System.out.println("distance: " + EditDistance.dist(s1, s2));
	}

	
}
