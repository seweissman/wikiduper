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

package courseproj.wiki;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.tools.bzip2.CBZip2InputStream;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.WikipediaPageFactory;

/**
 * Class for working with bz2-compressed Wikipedia article dump files on local
 * disk.
 * 
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class WikiPageTopicFilter {

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("usage: [dump file] [language] [index] [term]");
			System.exit(-1);
		}
	  
		HashMap<Long, HashSet<String>> offsetmap = getTermOffsets(args[2],args[3],1000);
	  
		WikipediaPage p = WikipediaPageFactory.createWikipediaPage(args[1]);
		
		WikipediaPagesBz2InputStream stream = new WikipediaPagesBz2InputStream(args[0]);
	  ArrayList<Long> offsetList = new ArrayList<Long>(offsetmap.keySet());
		Collections.sort(offsetList);
		for(long offset: offsetList){
		  //System.out.println("offset = " + offset);
		  HashSet<String> idSet = offsetmap.get(offset);
      //for(String id: idSet){
      //      System.out.println("id = " + id);
    //}
		  ArrayList<Long> idList = new ArrayList<Long>();
		  for(String id: idSet){
		    idList.add(Long.parseLong(id));
		  }
		  Collections.sort(idList);
		  for(Long lid : idList){
		    //while (stream.readPage(p,offset,lid.toString()) && !idSet.isEmpty()) {
		    //System.out.println("id = " + lid);
		    stream.readPage(p,offset,lid.toString());
		    //if(Long.parseLong(p.getDocid())%1000 == 0) System.out.println(p.getDocid()); 
		      if(idSet.contains(p.getDocid())){
		        System.out.println(p.getContent().replace("\n", ""));
		        idSet.remove(p.getDocid());
		      }
		  //}
		  }
		  //System.out.println(p.getContent());
		  //System.exit(-1);
		}

	}
	
	private static HashMap<Long,HashSet<String>> getTermOffsets(String indexfile, String term, int maxresults) throws IOException {
    BufferedReader br = null;
    FileInputStream fis = new FileInputStream(indexfile);
    byte[] ignoreBytes = new byte[2];
    fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
    br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis)));
    String s = null;
    HashMap<Long,HashSet<String>> offsetmap = new HashMap<Long,HashSet<String>>();
    Pattern termpat = Pattern.compile(".*" + term + ".*");
    String title;
    long streamoffset;
    String pageid;
    long lastoffset = -1;
    int ct = 0;
    while((s = br.readLine()) != null){
      String index[] = s.split(":");
      title = index[2];
      if(termpat.matcher(title).matches()){
        streamoffset = Long.parseLong(index[0]);
        pageid = index[1];
        if(streamoffset != lastoffset){
          offsetmap.put(streamoffset, new HashSet<String> ());
          offsetmap.get(streamoffset).add(pageid);
          lastoffset = streamoffset;
          //System.out.println("index line: " + s + " " + title + " " + streamoffset + " " + pageid);
        }else{
          offsetmap.get(streamoffset).add(pageid);
        }
        ct++;
        if(ct > maxresults){
          return offsetmap;
        }
        

      }
    }
    //System.out.println("returning\n");
    return offsetmap;
  }

  
}
