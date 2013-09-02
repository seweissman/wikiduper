
fin=$!
flang=$2
ein=$3
elang=$4

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

echo"etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingE -xml=data/${ein} -compressed=${ecompressed} -index=${eindex} -lang=${elang} -tokenizermodel=${etokens} -collectionvocab=${evocabtgt} -e_stopword=${estopwords}"
etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingE -xml=${ein} -compressed=${ecompressed} -index=${eindex} -lang=${elang} -tokenizermodel=${etokens} -collectionvocab=${evocabtgt} -e_stopword=${estopwords}

etc/hadoop-cluster.sh ivory.app.PreprocessWikipedia -mode=crosslingF -xml=${fin} -compressed=${fcompressed} -index=${findex} -targetindex=${eindex} -lang=${flang} -target_lang=${elang} -tokenizermodel=${ftokens} -e_tokenizermodel=${etokens} -f_f2e_vocab=${fvocabsrc} -e_f2e_vocab=${evocabtgt} -f2e_ttable=${f2eprobs} -e_e2f_vocab=${evocabsrc} -f_e2f_vocab=${fvocabtgt} -e2f_ttable=${e2fprobs} -e_stopword=${estopwords} -f_stopword=${fstopwords}

etc/hadoop-cluster.sh ivory.lsh.driver.RunComputeSignatures -index=${findex} -num_bits=1000 -type=random

hdfs dfs -cp ${findex}/randomvectors_D=1000/ ${eindex}/

etc/hadoop-cluster.sh ivory.lsh.driver.RunComputeSignatures -index=${eindex} -num_bits=1000 -type=random

etc/hadoop-cluster.sh ivory.lsh.pwsim.GenerateChunkedPermutedTables  -sourceindex=${findex} -index=${eindex} -num_bits=1000 -type=random -Q=300 -overlap=10000 -B=2000

etc/hadoop-cluster.sh ivory.lsh.pwsim.cl.CLSlidingWindowPwsim -index=${eindex} -num_bits=1000 -type=random -Q=300 -overlap=10000 -B=2000 -T=400

#etc/hadoop-cluster.sh edu.umd.cloud9.util.CombineSequenceFiles  ${eindex}/similardocs_random_maxdst=400_D=1000_Q=300_B=2000  pwsim.results/similardocs_random_maxdst=400_D=1000_Q=300_B=2000.single  100 1 edu.umd.cloud9.io.pair.PairOfInts org.apache.hadoop.io.IntWritable sequence

# etc/hadoop-cluster.sh ivory.lsh.eval.SampleIntDocVectors -index=${findex} -size=1000 -docnos=${findex}/sample-docnos
 #  etc/hadoop-cluster.sh ivory.lsh.eval.Docnos2Titles  -pwsim_output=pwsim.results/similardocs_random_maxdst=400_D=1000_Q=300_B=2000.single/part-00000  -output=pwsim.results/similardocs_random_maxdst=400_D=1000_Q=300_B=2000-filtered.titles  -e_collection=${ecompressed}  -f_collection=${fcompressed}  -f_lang=de -e_lang=en -docnos=${findex}/sample-docnos
# 1022  hdfs dfs -cat pwsim.results/similardocs_random_maxdst=400_D=1000_Q=300_B=2000-filtered.titles/part-00000 | less

etc/hadoop-cluster.sh ivory.lsh.bitext.Docs2Sentences -e_collection=${ecompressed} -f_collection=${fcompressed} -sentences=pwsim.sentences.de-en -e_index=${eindex} -f_index=${findex} -data=${ivoryDataDir} -e_lang=${elang} -f_lang=${flang}

#hdfs dfs -put $IVORYDIR/data/classifier/classifier-simple.de-en $datadir/
#hdfs dfs -put $IVORYDIR/data/classifier/classifier-complex.de-en $datadir/

etc/hadoop-cluster.sh ivory.lsh.bitext.FindParallelSentencePairs -e_collection=${ecompressed} -f_collection=${fcompressed} -sentences=pwsim.sentences.de-en -pwsim_output=pwsim.similardocs_random_maxdst=400_D=1000_Q=300_B=2000.single/part-00000 -bitext=pwsim.bitext.F1=90 -e_index=${eindex} -f_index=${findex} -data=${ivoryDataDir} -e_lang=${elang} -f_lang=${flang} -threshold=0.9 -classifier_id=1

etc/hadoop-cluster.sh ivory.lsh.bitext.FilterSentencePairs -input=pwsim.bitext.F1=90 -output=pwsim.bitext.F1=90.F2=50 -e_index=${eindex} -f_index=${findex} -data=${ivoryDataDir} -e_lang=${elang} -f_lang=${flang} -threshold=0.5 -classifier_id=1
