

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
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
	
	public static void main(String args[]){
		
		  if(args.length != 1){
				System.out.println("Usage: EditDistance <filein>\n");
				System.exit(-1);
			}
		    Pattern linepat = Pattern.compile("(\\[[0-9, ]+\\])\t\\((.*), \\d+:\\d+\\)");
		    ArrayList<ArrayList<String>> clusterlist = new ArrayList<ArrayList<String>>();
		    FileInputStream fin;
   		    int clusterct = 0;

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
	                    clusterlist.add(cluster);
	                    cluster = new ArrayList<String>();
	                }
	                sigcurr = sig;
	                cluster.add(sentence);


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
			
			System.out.println("Cluster ct: " + clusterct);
		
			long matchct = 0;
			long badct = 0;
			HashMap<Long,Long> histogram = new HashMap<Long,Long>();
			for(ArrayList<String >cluster : clusterlist){
				for(int i=0; i<cluster.size();i++){
					String entity1 = cluster.get(i);
					for(int j=i+1;j<cluster.size();j++){
						matchct++;
						String entity2 = cluster.get(j);
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
						
						if(score > 25){
							badct++;
						}
					}
					
					
				}
				//System.out.println();
			}
			for(long i=0;i<=100;i++){
				if(histogram.containsKey(i)){
					System.out.println(i+","+histogram.get(i));
				}else{
					System.out.println(i+","+0);
				}
			}
			
			System.out.println("Total pairs: " + matchct);
			System.out.println("Bad pairs: " + badct);
			System.out.println("FP rate: " + badct*1.0/matchct);
		String s1 = "abcedfgh";
		String s2 = "bcdefg";
		System.out.println("distance: " + EditDistance.dist(s1, s2));
	}

}
