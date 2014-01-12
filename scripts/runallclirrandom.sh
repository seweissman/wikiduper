#!/bin/bash

fin=$1
flang=$2
fpack=${fin}.pack

ein=$3
elang=$4
epack=${ein}.pack

ivoryDataDir=$5

ecompressed=${ein}.compressed
eindex=${ein}.index
fcompressed=${fin}.compressed
findex=${fin}.index


f2eprobs=${ivoryDataDir}/ttable.${flang}-${elang}
e2fprobs=${ivoryDataDir}/ttable.${elang}-${flang}
fvocabsrc=${ivoryDataDir}/vocab.${flang}-${elang}.${flang}
fvocabtgt=${ivoryDataDir}/vocab.${elang}-${flang}.${flang}
evocabsrc=${ivoryDataDir}/vocab.${elang}-${flang}.${elang}
evocabtgt=${ivoryDataDir}/vocab.${flang}-${elang}.${elang}
fstopwords=${ivoryDataDir}/token/${flang}.stop
estopwords=${ivoryDataDir}/token/${elang}.stop
ftokens=${ivoryDataDir}/token/${flang}-token.bin
etokens=${ivoryDataDir}/token/${elang}-token.bin

nbits=100
ntables=10
threshold=30
overlap=10000
window=2000

echo "hadoop fs -rm -r ${eindex}"
hadoop fs -rm -r ${eindex}

echo "hadoop fs -rm -r ${findex}"
hadoop fs -rm -r ${findex}

echo "etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingE -xml=${ein} -compressed=${ecompressed} -index=${eindex} -lang=${elang} -tokenizermodel=${etokens} -collectionvocab=${evocabtgt} -e_stopword=${estopwords}"
etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingE -xml=${ein} -compressed=${ecompressed} -index=${eindex} -lang=${elang} -tokenizermodel=${etokens} -collectionvocab=${evocabtgt} -e_stopword=${estopwords}

echo "etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingF -xml=${fin} -compressed=${fcompressed} -index=${findex} -targetindex=${eindex} -lang=${flang} -target_lang=${elang} -tokenizermodel=${ftokens} -e_tokenizermodel=${etokens} -f_f2e_vocab=${fvocabsrc} -e_f2e_vocab=${evocabtgt} -f2e_ttable=${f2eprobs} -e_e2f_vocab=${evocabsrc} -f_e2f_vocab=${fvocabtgt} -e2f_ttable=${e2fprobs} -e_stopword=${estopwords} -f_stopword=${fstopwords}"
etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingF -xml=${fin} -compressed=${fcompressed} -index=${findex} -targetindex=${eindex} -lang=${flang} -target_lang=${elang} -tokenizermodel=${ftokens} -e_tokenizermodel=${etokens} -f_f2e_vocab=${fvocabsrc} -e_f2e_vocab=${evocabtgt} -f2e_ttable=${f2eprobs} -e_e2f_vocab=${evocabsrc} -f_e2f_vocab=${fvocabtgt} -e2f_ttable=${e2fprobs} -e_stopword=${estopwords} -f_stopword=${fstopwords}

echo "etc/hadoop-cluster.sh ivory.lsh.driver.RunComputeSignatures -index=${findex} -num_bits=${nbits} -type=random"
etc/hadoop-cluster.sh ivory.lsh.driver.RunComputeSignatures -index=${findex} -num_bits=${nbits} -type=random

echo "hdfs dfs -cp ${findex}/randomvectors_D=${nbits}/ ${eindex}/"
hdfs dfs -cp ${findex}/randomvectors_D=${nbits}/ ${eindex}/

echo "etc/hadoop-cluster.sh ivory.lsh.driver.RunComputeSignatures -index=${eindex} -num_bits=${nbits} -type=random"
etc/hadoop-cluster.sh ivory.lsh.driver.RunComputeSignatures -index=${eindex} -num_bits=${nbits} -type=random

echo "etc/hadoop-cluster.sh ivory.lsh.pwsim.GenerateChunkedPermutedTables  -sourceindex=${findex} -index=${eindex} -num_bits=${nbits} -type=random -Q=${ntables} -overlap=${overlap} -B=${window}"
etc/hadoop-cluster.sh ivory.lsh.pwsim.GenerateChunkedPermutedTables  -sourceindex=${findex} -index=${eindex} -num_bits=${nbits} -type=random -Q=${ntables} -overlap=${overlap} -B=${window}

echo "etc/hadoop-cluster.sh ivory.lsh.pwsim.cl.CLSlidingWindowPwsim -index=${eindex} -num_bits=${nbits} -type=random -Q=${ntables} -overlap=${overlap} -B=${window} -T=${threshold}"
etc/hadoop-cluster.sh ivory.lsh.pwsim.cl.CLSlidingWindowPwsim -index=${eindex} -num_bits=${nbits} -type=random -Q=${ntables} -overlap=${overlap} -B=${window} -T=${threshold}

echo "etc/hadoop-cluster.sh ivory.lsh.eval.SampleIntDocVectors -index=${findex} -size=1000 -docnos=${findex}/sample-docnos"
etc/hadoop-cluster.sh ivory.lsh.eval.SampleIntDocVectors -index=${findex} -size=1000 -docnos=${findex}/sample-docnos

echo "etc/hadoop-cluster.sh ivory.lsh.eval.FilterResults -input=${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=${window} -output=${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=${window}-filtered -docnos=${findex}/sample-docnos"
etc/hadoop-cluster.sh ivory.lsh.eval.FilterResults -input=${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=${window} -output=${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=${window}-filtered -docnos=${findex}/sample-docnos

echo "etc/hadoop-cluster.sh ivory.lsh.eval.BruteForcePwsim  -input=${eindex}/wt-int-doc-vectors  -sample=${findex}/wt-int-doc-vectors_sample=1000/part-00000 -output=${eindex}/groundtruth_T=0.3 -cosineT=0.3 -type=intdocvector"
etc/hadoop-cluster.sh ivory.lsh.eval.BruteForcePwsim  -input=${eindex}/wt-int-doc-vectors  -sample=${findex}/wt-int-doc-vectors_sample=1000/part-00000 -output=${eindex}/groundtruth_T=0.3 -cosineT=0.3 -type=intdocvector

echo "hadoop dfs -get ${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=${window}-filtered/part-00000 ./pwsim-filtered_${threshold}-${nbits}-${ntables}-${window}"
hadoop dfs -get ${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=${window}-filtered/part-00000 ./pwsim-filtered_${threshold}-${nbits}-${ntables}-${window}

echo "rm -rf ./ground_0.3"
rm -rf ./ground_0.3

echo "hadoop dfs -get ${eindex}/groundtruth_T=0.3/part-00000 ./ground_0.3"
hadoop dfs -get ${eindex}/groundtruth_T=0.3/part-00000 ./ground_0.3

cd etc
echo "perl etc/eval.pl ../ground_0.3 ../pwsim-filtered_${threshold}-${nbits}-${ntables}-${window}"
perl eval.pl ../ground_0.3 ../pwsim-filtered_${threshold}-${nbits}-${ntables}-${window}

# #etc/hadoop-cluster.sh edu.umd.cloud9.util.CombineSequenceFiles  ${eindex}/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=2000  pwsim.results/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=2000.single  100 1 edu.umd.cloud9.io.pair.PairOfInts org.apache.hadoop.io.IntWritable sequence

#  #  etc/hadoop-cluster.sh ivory.lsh.eval.Docnos2Titles  -pwsim_output=pwsim.results/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=2000.single/part-00000  -output=pwsim.results/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=2000-filtered.titles  -e_collection=${ecompressed}  -f_collection=${fcompressed}  -f_lang=de -e_lang=en -docnos=${findex}/sample-docnos
# # 1022  hdfs dfs -cat pwsim.results/similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=2000-filtered.titles/part-00000 | less

# etc/hadoop-cluster.sh ivory.lsh.bitext.Docs2Sentences -e_collection=${ecompressed} -f_collection=${fcompressed} -sentences=pwsim.sentences.de-en -e_index=${eindex} -f_index=${findex} -data=${ivoryDataDir} -e_lang=${elang} -f_lang=${flang}

# #hdfs dfs -put $IVORYDIR/data/classifier/classifier-simple.de-en $datadir/
# #hdfs dfs -put $IVORYDIR/data/classifier/classifier-complex.de-en $datadir/

# etc/hadoop-cluster.sh ivory.lsh.bitext.FindParallelSentencePairs -e_collection=${ecompressed} -f_collection=${fcompressed} -sentences=pwsim.sentences.de-en -pwsim_output=pwsim.similardocs_random_maxdst=${threshold}_D=${nbits}_Q=${ntables}_B=2000.single/part-00000 -bitext=pwsim.bitext.F1=90 -e_index=${eindex} -f_index=${findex} -data=${ivoryDataDir} -e_lang=${elang} -f_lang=${flang} -threshold=0.9 -classifier_id=1

# etc/hadoop-cluster.sh ivory.lsh.bitext.FilterSentencePairs -input=pwsim.bitext.F1=90 -output=pwsim.bitext.F1=90.F2=50 -e_index=${eindex} -f_index=${findex} -data=${ivoryDataDir} -e_lang=${elang} -f_lang=${flang} -threshold=0.5 -classifier_id=1
