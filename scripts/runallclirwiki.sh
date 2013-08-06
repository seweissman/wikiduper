#k=12
#bits=30
#nHash=50
#n=80
#nSamples=100
ivoryDataDir=../Ivory/data
fin=$1
flang=$2
fmapping=${fin}.mapping
fpack=${fin}.pack
fpreproc=${fin}.preproc

ein=$3
elang=$4
emapping=${ein}.mapping
epack=${ein}.pack
epreproc=${ein}.preproc

sentencemap=$5
samples=$6

f2eprobs=${ivoryDataDir}/vocab/ttable.${flang}-${elang}
e2fprobs=${ivoryDataDir}/vocab/ttable.${elang}-${flang}
fvocabsrc=${ivoryDataDir}/vocab/vocab.${flang}-${elang}.${flang}
fvocabtgt=${ivoryDataDir}/vocab/vocab.${elang}-${flang}.${flang}
evocabsrc=${ivoryDataDir}/vocab/vocab.${elang}-${flang}.${elang}
evocabtgt=${ivoryDataDir}/vocab/vocab.${flang}-${elang}.${elang}
fstopwords=${ivoryDataDir}/tokenizer/${flang}.stop
estopwords=${ivoryDataDir}/tokenizer/${elang}.stop
ftokens=${ivoryDataDir}/tokenizer/${flang}-token.bin
etokens=${ivoryDataDir}/tokenizer/${elang}-token.bin

bits=$7
k=$8
n=$9
nhash=${10}

output=${11}

#echo "etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.PreprocessWikiInput -elang ${elang} -ewiki ${epack} -flang ${flang} -fwiki ${fpack} -eout ${epreproc} -fout ${fpreproc}"
#etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.PreprocessWikiInput -elang ${elang} -ewiki ${epack} -flang ${flang} -fwiki ${fpack} -eout ${epreproc} -fout ${fpreproc}
echo "etc/hadoop-cluster.sh wikiduper.clir.minhash.PreprocessTextInput -elang ${elang} -ein ${ein} -flang ${flang} -fin ${fin} -eout ${epreproc} -fout ${fpreproc}"
etc/hadoop-cluster.sh wikiduper.clir.minhash.PreprocessTextInput -elang ${elang} -ein ${ein} -flang ${flang} -fin ${fin} -eout ${epreproc} -fout ${fpreproc}
echo "etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.SampleSentenceTranslations -fVocabSrc ${fvocabsrc} -fVocabTgt ${fvocabtgt} -eVocabSrc ${evocabsrc} -eVocabTgt ${evocabtgt} -e2fprobs ${e2fprobs} -f2eprobs ${f2eprobs} -fLang ${flang} -eLang ${elang} -fStopWords ${fstopwords} -eStopWords ${estopwords} -fTokens ${ftokens} -eTokens ${etokens} -ein ${epreproc} -fin ${fpreproc} -output ${sentencemap} -M ${samples}"
etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.SampleSentenceTranslations -fVocabSrc ${fvocabsrc} -fVocabTgt ${fvocabtgt} -eVocabSrc ${evocabsrc} -eVocabTgt ${evocabtgt} -e2fprobs ${e2fprobs} -f2eprobs ${f2eprobs} -fLang ${flang} -eLang ${elang} -fStopWords ${fstopwords} -eStopWords ${estopwords} -fTokens ${ftokens} -eTokens ${etokens} -ein ${epreproc} -fin ${fpreproc} -output ${sentencemap} -M ${samples}
echo "etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.MinhashCLIRTest -bits ${bits} -input ${sentencemap} -k ${k} -M ${samples} -n ${n} -nHash ${nhash} -numReducers 20 -output ${output}"
etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.MinhashCLIRTest -bits ${bits} -input ${sentencemap} -k ${k} -M ${samples} -n ${n} -nHash ${nhash} -numReducers 20 -output ${output}


