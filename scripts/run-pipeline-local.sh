
nReducers=20
lang=en
# input = $1
# output $2
# nHash $3
# bits $4
# k $5
# n $6
# shingleLen $7
pairs=$1.pairs
clusters=$1.clusters
tmppath=tmppath
wikipack=$1.pack
mapping=$1.mapping

echo "etc/hadoop-local.sh wikiduper.wikipedia.BuildWikipediaDocnoMapping -input $1 -keep_all -output_file ${emapping} -output_path ${tmppath} -wiki_language ${lang}"
etc/hadoop-local.sh wikiduper.wikipedia.BuildWikipediaDocnoMapping -input $1 -keep_all -output_file $mapping -output_path $tmppath -wiki_language $lang
echo "etc/hadoop-local.sh wikiduper.wikipedia.RepackWikipedia -compression_type block -input $1 -mapping_file ${mapping} -output ${wikipack} -wiki_language ${lang}"
etc/hadoop-local.sh wikiduper.wikipedia.RepackWikipedia -compression_type block -input $1 -mapping_file $mapping -output $wikipack -wiki_language $lang
echo rm -rf $pairs
rm -rf $pairs
echo etc/hadoop-local.sh wikiduper.application.MinhashWikipediaPages -wiki_language $lang -input $wikipack -output $pairs -numReducers $nReducers -nHash $3 -k $5 -n $6 -bits $4 -shingleLen $7
etc/hadoop-local.sh wikiduper.application.MinhashWikipediaPages -wiki_language $lang -input $wikipack -output $pairs -numReducers $nReducers -nHash $3 -k $5 -n $6 -bits $4 -shingleLen $7
#echo hadoop fs -get $pairs
#hadoop fs -get $pairs
echo rm -rf $clusters
rm -rf $clusters
echo etc/run.sh wikiduper.utils.MergeClusters -input $pairs -output $clusters
etc/run.sh wikiduper.utils.MergeClusters -input $pairs -output $clusters
echo etc/hadoop-local.sh wikiduper.application.GetSentenceClusters -input $wikipack -wiki_language en -clustermap $clusters -output $2 -numReducers $nReducers
etc/hadoop-local.sh wikiduper.application.GetSentenceClusters -input $wikipack -wiki_language en -clustermap $clusters -output $2 -numReducers $nReducers
echo "etc/hadoop-text-local.sh $2/part-* > $2-$3-$5-$6-$7-$4"
etc/hadoop-text-local.sh $2/part-* > $2-$3-$5-$6-$7-$4
#echo mv $2-$3-$5-$6-$7-$4 /scratch0/sew
#mv $2-$3-$5-$6-$7-$4 /scratch0/sew
