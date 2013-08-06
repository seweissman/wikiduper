files=$1*
for f in $files
do
echo hadoop fs -rm -r enwiki-20130503-sentences
hadoop fs -rm -r enwiki-20130503-sentences
echo hadoop fs -rm -r enwiki-20130503-scores
hadoop fs -rm -r enwiki-20130503-scores
echo hadoop fs -put $f enwiki-20130503-sentences
hadoop fs -put $f enwiki-20130503-sentences
echo etc/hadoop-cluster.sh wikiduper.analysis.EditDistanceClusters -input enwiki-20130503-sentences -output enwiki-20130503-scores -numReducers 20
etc/hadoop-cluster.sh wikiduper.analysis.EditDistanceClusters -input enwiki-20130503-sentences -output enwiki-20130503-scores -numReducers 20
echo hadoop fs -get enwiki-20130503-scores $f-scores
hadoop fs -get enwiki-20130503-scores $f-scores
done
