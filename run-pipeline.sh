
nReducers = 20
lang = en
# input = $1
# output $2
# nHash $3
# k $4
# n $5
# bits $6
# shingleLen $7

echo etc/hadoop-cluster.sh courseproj.application.MinhashWikipediaPages -wiki_language $lang -input $1 -output enwiki-20130503-pairs -numReducers $nReducers -nHash $3 -k $4 -n $5 -bits $6 -shingleLen $7
echo etc/hadoop-cluster.sh courseproj.application.DedupSentencePairs -input enwiki-20130503-pairs -output enwiki-20130503-pairsdedup
echo rm -rf enwiki-20130503-pairsdedup
echo hadoop fs -get enwiki-20130503-pairsdedup
echo hadoop fs -rm -r ewiki-20130503-clusters
echo etc/run.sh courseproj.application.MergeClusters -pairfile enwiki-20130503-pairsdedup -output enwiki-20130503-clusters
echo hadoop fs -put ewiki-20130503-clusters
echo etc/hadoop-cluster.sh courseproj.application.GetSentenceClusters -input $1 -wiki_language en -clustermap enwiki-20130503-clusters -output $2 -numReducers $nReducers
echo hadoop fs -get $2
