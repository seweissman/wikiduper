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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tools.bzip2.CBZip2InputStream;

import wikiduper.wikipedia.WikipediaPage;
import wikiduper.wikipedia.language.WikipediaPageFactory;

/**
 * 
 */
public class WikipediaPagesBz2InputStream {
    private static int DEFAULT_STRINGBUFFER_CAPACITY = 1024;

    private BufferedReader br;
    private FileInputStream fis;
    private long curroffset = 0;
    /**
     * Creates an input stream for reading Wikipedia articles from a
     * bz2-compressed dump file.
     * 
     * @param file
     *            path to dump file
     * @throws IOException
     */
    public WikipediaPagesBz2InputStream(String file) throws IOException {
        br = null;
        fis = new FileInputStream(file);
        byte[] ignoreBytes = new byte[2];
        fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
        br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis)));
    }

    public boolean nextStream() throws IOException{
        byte[] ignoreBytes = new byte[2];
        if(fis.available() > 0){
            fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
            br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis)));
            return true;
        }else return false;
    }


    /**
     * Reads the next Wikipedia page.
     * 
     * @param page
     *            WikipediaPage object to read into
     * @return <code>true</code> if page is successfully read
     * @throws IOException
     */
    public boolean readNext(WikipediaPage page) throws IOException {

        String s = null;
        StringBuffer sb = new StringBuffer(DEFAULT_STRINGBUFFER_CAPACITY);

        while ((s = br.readLine()) != null || (nextStream() && (s = br.readLine()) != null)) {
            if (s.endsWith("<page>"))
                break;
        }


        if (s == null){
            fis.close();
            br.close();
            return false;
        }

        sb.append(s + "\n");

        while ((s = br.readLine()) != null) {
            //System.out.println("line: " + s);
            sb.append(s + "\n");

            if (s.endsWith("</page>"))
                break;
        }

        WikipediaPage.readPage(page, sb.toString());

        return true;
    }

    public ArrayList<String> readPage(WikipediaPage page, long streamoffset, List<String> idList) throws IOException {
        ArrayList<String> pages = new ArrayList<String>();
        //System.out.println("in id = " + id);
        //System.out.println("in offset = " + streamoffset);
        //System.out.println("start offset = " + this.curroffset);
        long skipn = streamoffset - this.curroffset;
        long n = br.skip(skipn);
        this.curroffset += n;

        while(skipn - n > 0){
            //System.out.println("asked skip = " + skipn + " actual = " + n);
            nextStream();
            skipn = streamoffset - this.curroffset;
            n = br.skip(skipn);
            this.curroffset += n;
        }
        Pattern idpat = Pattern.compile(".*<id>([0-9]+)</id>.*");

        //System.out.println("id list size " + idList.size());
        for(String id: idList){
            //System.out.println("Looking for id = " + id);
            //System.out.println("asked skip = " + skipn + " curroffset = " + this.curroffset);
            //fis.skip(streamoffset - this.curroffset);
            String s = null;
            StringBuffer sb = new StringBuffer(DEFAULT_STRINGBUFFER_CAPACITY);
            int ct = 0;
            while ((s = br.readLine()) != null || (nextStream() && (s = br.readLine()) != null)) {
                if (s.endsWith("<page>")){

                    ct++;
                    sb.setLength(0);
                    sb.append(s + "\n");
                    sb.append(br.readLine()); //title
                    sb.append(br.readLine()); //ns
                    s = br.readLine();
                    if(ct%1000 == 0){
                        //System.out.println("ct = " + ct + " " + s);

                        Matcher m = idpat.matcher(s);
                        if(m.matches() & m.groupCount() > 0){
                            String idstr = m.group(1);
                            //System.out.println("idstr " + idstr + " search id "+ id);
                            if(Long.parseLong(idstr) > Long.parseLong(id)){
                                return pages;
                            }
                        }
                    }
                    if(s.endsWith("</id>")){
                        if(s.endsWith(id+"</id>")){
                            break;
                        }
                    }
                }
            }


            if (s == null){
                //System.out.println("s null");
                //fis.close();
                //br.close();
                return pages;
            }

            sb.append(s + "\n");

            while ((s = br.readLine()) != null) {
                sb.append(s + "\n");
                if (s.endsWith("</page>"))
                    break;
            }

            WikipediaPage.readPage(page, sb.toString());
            // Useful for emitting XML:
            //System.out.println(sb.toString());
            pages.add(page.getContent());
        }
        return pages;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: [dump file] [language]");
            System.exit(-1);
        }

        WikipediaPage p = WikipediaPageFactory.createWikipediaPage(args[1]);

        WikipediaPagesBz2InputStream stream = new WikipediaPagesBz2InputStream(args[0]);

        while (stream.readNext(p)){
            System.out.println(p.getContent().replace("\n", ""));

            //System.out.println(p.getContent());
        }

    }


}
