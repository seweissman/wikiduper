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

package wikiduper.wiki;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.tools.bzip2.CBZip2InputStream;

import wikiduper.wikipedia.WikipediaPage;
import wikiduper.wikipedia.language.WikipediaPageFactory;


/**
 * To run:
 * 
 * java -cp build:lib/commons-lang-2.6.jar:lib/cloud9-1.4.13.jar:lib/bliki-core-3.0.16.jar:/Users/weissman/apache-ant/apache-ant-1.9.0/lib/ant.jar:lib/hadoop-common-2.0.0-cdh4.2.0.jar 
 *   courseproj.wiki.WikiPageTopicFilter ~/corpora/enwiki-20130403-pages-articles-multistream.xml.bz2 
 *   en ~/corpora/enwiki-20130403-pages-articles-multistream-index.txt.bz2 Maryland > Maryland.txt
 *
 * 
 * java -cp build:lib/commons-lang-2.6.jar:lib/cloud9-1.4.15.jar:lib/bliki-core-3.0.16.jar:lib/ant-1.9.1.jar:lib/hadoop-common-2.0.0-cdh4.2.1.jar wikiduper.wiki.WikiPageTopicFilter data/enwiki-20150304-pages-meta-current1.xml-p000000010p000010000.bz2  en data/enwiki-20150304-pages-articles-multistream-index.txt.bz2 Maryland > Maryland.txt
 * 
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
            HashSet<String> idSet = offsetmap.get(offset);
            ArrayList<Long> idList = new ArrayList<Long>();
            List<String> idListStr = new ArrayList<String>();
            for(String id: idSet){
                idList.add(Long.parseLong(id));
            }
            Collections.sort(idList);
            for(Long lid : idList){
                idListStr.add(lid.toString());
            }
            //while (stream.readPage(p,offset,lid.toString()) && !idSet.isEmpty()) {
            //System.out.println("id = " + lid);
            ArrayList<String> pages = stream.readPage(p,offset,idListStr);
            //if(Long.parseLong(p.getDocid())%1000 == 0) System.out.println(p.getDocid()); 
            for(String page : pages){
                System.out.println(page.replace("\n", " "));
            }
            //}
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
            if(ct % 10000 == 0){
                System.err.println("Read " + ct + " offsets.");
            }
            //System.out.println("Title = " + title);
            if(termpat.matcher(title).matches()){
                streamoffset = Long.parseLong(index[0]);
                pageid = index[1];
                if(streamoffset != lastoffset){
                    offsetmap.put(streamoffset, new HashSet<String> ());
                    offsetmap.get(streamoffset).add(pageid);
                    lastoffset = streamoffset;
                }else{
                    offsetmap.get(streamoffset).add(pageid);
                }
                ct++;
                if(ct > maxresults){
                    return offsetmap;
                }


            }
        }

        return offsetmap;
    }


}
