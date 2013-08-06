#k=12
#bits=30
#nHash=50
#n=80
#nSamples=100
ivoryDataDir=../Ivory/data
k=$1
bits=$2
nHash=$3
n=$4
nSamples=$5
#echo "etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.PreprocessWikiInput -elang en -ewiki data/enwiki.test.pack/ -flang de -fwiki data/dewiki.test.pack/ -output data/endewiki.preproc"
#etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.PreprocessWikiInput -elang en -ewiki data/enwiki.test.pack/ -flang de -fwiki data/dewiki.test.pack/ -output data/endewiki.preproc
echo "etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.SampleSentenceTranslations -f2eprobs $ivoryDataDir/vocab/ttable.de-en -fVocabSrc $ivoryDataDir/vocab/vocab.de-en.de -fVocabTgt $ivoryDataDir/vocab/vocab.en-de.de -eVocabSrc $ivoryDataDir/vocab/vocab.en-de.en -eVocabTgt $ivoryDataDir/vocab/vocab.de-en.en -e2fprobs $ivoryDataDir/vocab/ttable.en-de -f2eprobs $ivoryDataDir/vocab/ttable.de-en -fLang "de" -eLang "en" -fStopWords $ivoryDataDir/tokenizer/de.stop -eStopWords $ivoryDataDir/tokenizer/en.stop -fTokens $ivoryDataDir/tokenizer/de-token.bin -eTokens $ivoryDataDir/tokenizer/en-token.bin  -ein e-tmp -fin f-tmp -output data/id2sentence.map -M 100"
etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.SampleSentenceTranslations -f2eprobs $ivoryDataDir/vocab/ttable.de-en -fVocabSrc $ivoryDataDir/vocab/vocab.de-en.de -fVocabTgt $ivoryDataDir/vocab/vocab.en-de.de -eVocabSrc $ivoryDataDir/vocab/vocab.en-de.en -eVocabTgt $ivoryDataDir/vocab/vocab.de-en.en -e2fprobs $ivoryDataDir/vocab/ttable.en-de -f2eprobs $ivoryDataDir/vocab/ttable.de-en -fLang "de" -eLang "en" -fStopWords $ivoryDataDir/tokenizer/de.stop -eStopWords $ivoryDataDir/tokenizer/en.stop -fTokens $ivoryDataDir/tokenizer/de-token.bin -eTokens $ivoryDataDir/tokenizer/en-token.bin  -ein e-tmp -fin f-tmp -output data/id2sentence.map -M 100
#"etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.MinhashCLIR -bits 32 -input data/id2sentence.map -k 12 -M 10 -n 20 -nHash 20 -numReducers 20 -output mhwikiout"
#etc/hadoop-cluster.sh wikiduper.clir.minhashwiki.MinhashCLIR -bits 32 -input data/id2sentence.map -k 12 -M 10 -n 20 -nHash 20 -numReducers 20 -output mhwikiout


