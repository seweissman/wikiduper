
tmppath=tmppath
fin=$1
flang=$2
fmapping=${fin}.mapping
fpack=${fin}.pack

ein=$3
elang=$4
emapping=${ein}.mapping
epack=${ein}.pack

#first language
echo "etc/hadoop-cluster.sh wikiduper.wikipedia.BuildWikipediaDocnoMapping -input ${fin} -keep_all -output_file ${fmapping} -output_path ${tmppath} -wiki_language ${flang}"
etc/hadoop-cluster.sh wikiduper.wikipedia.BuildWikipediaDocnoMapping -input ${fin} -keep_all -output_file ${fmapping} -output_path ${tmppath} -wiki_language ${flang}
echo "etc/hadoop-cluster.sh wikiduper.wikipedia.RepackWikipedia -compression_type block -input ${fin} -mapping_file ${fmapping} -output ${fpack} -wiki_language ${flang}"
etc/hadoop-cluster.sh wikiduper.wikipedia.RepackWikipedia -compression_type block -input ${fin} -mapping_file ${fmapping} -output ${fpack} -wiki_language ${flang}

#second language
#first language
echo "etc/hadoop-cluster.sh wikiduper.wikipedia.BuildWikipediaDocnoMapping -input ${ein} -keep_all -output_file ${emapping} -output_path ${tmppath} -wiki_language ${elang}"
etc/hadoop-cluster.sh wikiduper.wikipedia.BuildWikipediaDocnoMapping -input ${ein} -keep_all -output_file ${emapping} -output_path ${tmppath} -wiki_language ${elang}
echo "etc/hadoop-cluster.sh wikiduper.wikipedia.RepackWikipedia -compression_type block -input ${ein} -mapping_file ${emapping} -output ${epack} -wiki_language ${elang}"
etc/hadoop-cluster.sh wikiduper.wikipedia.RepackWikipedia -compression_type block -input ${ein} -mapping_file ${emapping} -output ${epack} -wiki_language ${elang}

