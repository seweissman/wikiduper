package analysis;



import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EditDistance {

	
	static public long dist(String s1, String s2){
		long distance[][] = new long[s1.length()+1][s2.length()+1];
		for(int i=0;i<=s1.length();i++){
			distance[i][0] = i;
		}

		for(int j=0;j<=s2.length();j++){
			distance[0][j] = j;
		}

		for(int i=1;i<=s1.length();i++){
			int cost;
			for(int j=1;j<=s2.length();j++){
			    if(s1.substring(i-1,i).equals(s2.substring(j-1,j))){
					cost = 0;
			    }else{
					cost = 1;
			    }

			    distance[i][j] = Math.min(distance[i-1][j] + 1,
					     Math.min(distance[i][j-1] + 1,
						 		distance[i-1][j-1] + cost));
			    //System.out.println("i= " + i + " j= " + j);
			    //System.out.println(s1.substring(i-1,i) + " " + s2.substring(j-1,j));
			    if(i>1 && j>1 && s1.substring(i-1,i).equals(s2.substring(j-2,j-1))
			       && s1.substring(i-2,i-1).equals(s2.substring(j-1,j))){
			    	distance[i][j] = Math.min(distance[i][j],distance[i-1][j-2] + cost);
			    }

			    //# Some way to weight differences at beginning of addressss more?
			    //#if($i < length($s1/2) && $j < length($s2/2)){
				//#$d{$i}{$j} += 1;
			    //#}
			}
	    }
/*
		     for(int i=0;i<=s1.length();i++){
		     	for(int j=0;j<=s2.length();j++){
		     	    System.out.println("d["+i+"]["+j+"] = " + distance[i][j]);
		     	}
		    }
*/
		    return distance[s1.length()][s2.length()];
	}
	
    static HashSet<String> getConnectedComponent(String entity, HashMap<String, HashSet<String>> matchmap){
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
		
		  if(args.length != 1){
				System.out.println("Usage: EditDistance <filein>\n");
				System.exit(-1);
			}
		    Pattern linepat = Pattern.compile("(\\[[-0-9, ]+\\])\t\\((.*), \\d+:\\d+\\)");
		    //ArrayList<ArrayList<String>> clusterlist = new ArrayList<ArrayList<String>>();
		    HashMap<String, HashSet<String>> matchmap = new HashMap<String, HashSet<String>>();
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
	                                long dl = Math.max(m1.length(), m2.length()) - Math.min(m1.length(), m2.length());
	                                long d = dist(m1,m2);
	                                long score = Math.round(100*(d - dl + 1)*1.0/Math.max(m1.length(), m2.length()));
	                                
	                                if(score > 25){
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
	                                }
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
		    HashMap<Long,Long> histogram = new HashMap<Long,Long>();
            long matchct = 0;
            long badct = 0;
		    while(!matchmap.isEmpty()){
	            String[] matchentities = matchmap.keySet().toArray(new String[0]);
	            String entity = matchentities[0];
	            HashSet<String> comp = getConnectedComponent(entity, matchmap);
	            //System.out.println("entity = " + entity);
	            String cluster[] = comp.toArray(new String[0]);
	            componentct++;
	            System.out.println("componentct = " + componentct + " " + comp.size());

	            int clustdisplayct = 0;
	            for(String m : cluster){
	                if(clustdisplayct > 20) break;
	                System.out.println("item = " + m);
	                clustdisplayct++;
	            }

	            for(int i=0; i<cluster.length;i++){
                    String entity1 = cluster[i];
                    for(int j=i+1;j<cluster.length;j++){
                        matchct++;
                        String entity2 = cluster[j];
                        long dl = Math.max(entity1.length(), entity2.length()) - Math.min(entity1.length(), entity2.length());
                        long d = dist(entity1,entity2);
                        long score = Math.round(100*(d - dl + 1)*1.0/Math.max(entity1.length(), entity2.length()));
                        //System.out.println(entity1 + ", " + entity2 + ", " + d + ", " + dl + ", " + Math.ceil( 100*score));
                        if(score > 0){
                            if(!histogram.containsKey(score)){
                                histogram.put(score, 0l);
                            }
                            histogram.put(score, histogram.get(score)+1);
                        }
                        //if(dl > d){
                            //System.out.println("Weird lines:");
                            //System.out.println(entity1 + ", " + entity2 + ", " + d + ", " + dl + ", " + Math.ceil( 100*score));
                        //}
                        
                        if(score > 5){
                            badct++;
                        }
                    }
                 }
	            System.out.println("matchct = " + matchct + " ,'badct = " + badct);
		    }
			for(long i=0;i<=100;i++){
				if(histogram.containsKey(i)){
					System.out.println(i+","+histogram.get(i));
				}else{
					System.out.println(i+","+0);
				}
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
