#!/bin/bash

hadoopstreaming=/opt/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop-mapreduce/hadoop-streaming.jar 
scratchdir=/scratch0/sew
fin=$1
flang=$2
fout=$5
ftmp=${fout}.tmp
fpack=${fin}.pack
fsent=${fin}.docsentence
fmap=${fsent}.map
fsentproc=${fsent}.proc

ein=$3
elang=$4
eout=$6
etmp=${eout}.tmp
epack=${ein}.pack
esent=${ein}.docsentence
emap=${esent}.map
esentproc=${esent}.proc

echo "hadoop fs -rm -r ${fout}*"
hadoop fs -rm -r ${fout}*

echo "hadoop fs -rm -r ${eout}*"
hadoop fs -rm -r ${eout}*

echo "hadoop fs -rm -r ${fsent}"
hadoop fs -rm -r ${fsent}

echo "hadoop fs -rm -r ${esent}"
hadoop fs -rm -r ${esent}

echo "hadoop fs -rm -r ${fmap}"
hadoop fs -rm -r ${fmap}

echo "hadoop fs -rm -r ${emap}"
hadoop fs -rm -r ${emap}

echo "etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.eval.WikiSentence2Doc -elang ${elang} -ewiki ${epack} -flang ${flang} -fwiki ${fpack} -emap ${emap} -fmap ${fmap} -eout ${esent} -fout ${fsent}"
etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.eval.WikiSentence2Doc -elang ${elang} -ewiki ${epack} -flang ${flang} -fwiki ${fpack} -emap ${emap} -fmap ${fmap} -eout ${esent} -fout ${fsent}

echo "hadoop fs -rm -r ${fsentproc}"
hadoop fs -rm -r ${fsentproc}

echo "hadoop jar $hadoopstreaming -input ${fsent} -output ${fsentproc} -mapper 'cut -f 2'"
hadoop jar $hadoopstreaming -input ${fsent} -output ${fsentproc} -mapper 'cut -f 2'

echo "hadoop fs -rm -r ${esentproc}"
hadoop fs -rm -r ${esentproc}

echo "hadoop jar $hadoopstreaming -input ${esent} -output ${esentproc} -mapper 'cut -f 2'"
hadoop jar $hadoopstreaming -input ${esent} -output ${esentproc} -mapper 'cut -f 2'

echo "hadoop fs -cat ${esentproc}/part-* > ${scratchdir}/${etmp}"
hadoop fs -cat ${esentproc}/part-* > ${scratchdir}/${etmp}

echo "cat ${scratchdir}/enwiki-header.xml ${scratchdir}/${etmp} ${scratchdir}/wikifooter.xml > ${scratchdir}/${eout}"
cat ${scratchdir}/enwiki-header.xml ${scratchdir}/${etmp} ${scratchdir}/wikifooter.xml > ${scratchdir}/${eout}

echo "rm ${scratchdir}/${etmp}"
rm ${scratchdir}/${etmp}

echo "hadoop fs -cat ${fsentproc}/part-* > ${scratchdir}/${ftmp}"
hadoop fs -cat ${fsentproc}/part-* > ${scratchdir}/${ftmp}

echo "cat ${scratchdir}/dewiki-header.xml ${scratchdir}/${ftmp} ${scratchdir}/wikifooter.xml > ${scratchdir}/${fout}"
cat ${scratchdir}/dewiki-header.xml ${scratchdir}/${ftmp} ${scratchdir}/wikifooter.xml > ${scratchdir}/${fout}

echo "rm ${scratchdir}/${ftmp}"
rm ${scratchdir}/${ftmp}

echo "hadoop fs -rm ${fout}"
hadoop fs -rm ${fout}

echo "hadoop fs -put ${scratchdir}/${fout}"
hadoop fs -put ${scratchdir}/${fout}

echo "hadoop fs -rm ${eout}"
hadoop fs -rm ${eout}

echo "hadoop fs -put ${scratchdir}/${eout}"
hadoop fs -put ${scratchdir}/${eout}