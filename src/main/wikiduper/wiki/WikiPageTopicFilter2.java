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
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
 * java -cp build:lib/commons-lang-2.6.jar:lib/cloud9-1.4.15.jar:lib/bliki-core-3.0.16.jar:lib/ant-1.9.1.jar:lib/hadoop-common-2.0.0-cdh4.2.1.jar 
 * wikiduper.wiki.WikiPageTopicFilter2 
 * data/wiki/enwiki-20150304-pages-meta-current1.xml-p000000010p000010000.bz2  
 * en 
 * Geography 
 * geography.txt
 * 
 */
public class WikiPageTopicFilter2 {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("usage: [dump file] [language] [term] [out file]");
            System.exit(-1);
        }
        String dumpFile = args[0];
        String language = args[1];
        String term = args[2];
        String outFile = args[3];
        WikipediaPage p = WikipediaPageFactory.createWikipediaPage(language);

        WikipediaPagesBz2InputStream stream = new WikipediaPagesBz2InputStream(dumpFile);
        Pattern termpat = Pattern.compile(".*" + term + ".*");
        //DataOutputStream dout = new DataOutputStream(new FileOutputStream(outFile));
        PrintWriter writer = new PrintWriter(outFile);
        while(stream.readNext(p)){
            String title = p.getTitle();
            String page = p.getRawXML();
            
            if(termpat.matcher(title).matches()){
                writer.write(page);
//                p.write(new DataOutputStream(dout));
//                System.out.println(p);
            }
            //}
        }
        writer.close();
        //dout.close();
  
    }


}
