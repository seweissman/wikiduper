#k=12
#bits=30
#nHash=50
#n=80
#nSamples=100
k=$1
bits=$2
nHash=$3
n=$4
nSamples=$5
echo "etc/hadoop-local.sh wikiduper.clir.minhash.SampleSentenceTranslations -f2eprobs ../Ivory/data/vocab/ttable.de-en -eVocabSrc ../Ivory/data/vocab/vocab.en-de.en -eVocabTgt ../Ivory/data/vocab/vocab.de-en.en -fVocabSrc ../Ivory/data/vocab/vocab.de-en.de -fVocabTgt ../Ivory/data/vocab/vocab.en-de.de -f2eprobs ../Ivory/data/vocab/ttable.de-en -e2fprobs ../Ivory/data/vocab/ttable.en-de -eLang "en" -fLang "de" -eStopWords ../Ivory/data/tokenizer/en.stop -fStopWords ../Ivory/data/tokenizer/de.stop -eTokens ../Ivory/data/tokenizer/en-token.bin -fTokens ../Ivory/data/tokenizer/de-token.bin -input europarl.seq -output id2sentence.map -M $nSamples"
etc/hadoop-local.sh wikiduper.clir.minhash.SampleSentenceTranslations -f2eprobs ../Ivory/data/vocab/ttable.de-en -eVocabSrc ../Ivory/data/vocab/vocab.en-de.en -eVocabTgt ../Ivory/data/vocab/vocab.de-en.en -fVocabSrc ../Ivory/data/vocab/vocab.de-en.de -fVocabTgt ../Ivory/data/vocab/vocab.en-de.de -f2eprobs ../Ivory/data/vocab/ttable.de-en -e2fprobs ../Ivory/data/vocab/ttable.en-de -eLang "en" -fLang "de" -eStopWords ../Ivory/data/tokenizer/en.stop -fStopWords ../Ivory/data/tokenizer/de.stop -eTokens ../Ivory/data/tokenizer/en-token.bin -fTokens ../Ivory/data/tokenizer/de-token.bin -input europarl.seq -output id2sentence.map -M $nSamples
echo "etc/hadoop-local.sh wikiduper.clir.minhash.CreateSentenceIdTranslationIdMap -input id2sentence.map -output sentence2translation.map -M $nSamples"
etc/hadoop-local.sh wikiduper.clir.minhash.CreateSentenceIdTranslationIdMap -input id2sentence.map -output sentence2translation.map -M $nSamples
echo "etc/hadoop-local.sh wikiduper.clir.minhash.MinhashCLIR -bits $bits -k $k -n $n -nHash $nHash -input id2sentence.map -output sentencepairs.out -M $nSamples"
etc/hadoop-local.sh wikiduper.clir.minhash.MinhashCLIR -bits $bits -k $k -n $n -nHash $nHash -input id2sentence.map -output sentencepairs.out -M $nSamples
echo "etc/hadoop-local.sh wikiduper.clir.minhash.DedupCLIRMHPairs -input sentencepairs.out -output sentencematchpairs.map -M $nSamples"
etc/hadoop-local.sh wikiduper.clir.minhash.DedupCLIRMHPairs -input sentencepairs.out -output sentencematchpairs.map -M $nSamples
echo "etc/run.sh wikiduper.clir.minhash.JaccardCompare -matchesout match-$k-$bits-$n-$nHash-$nSamples.out -nomatchesout nomatch-$k-$bits-$n-$nHash-$nSamples.out -M $nSamples"
etc/run.sh wikiduper.clir.minhash.JaccardCompare -matchesout match-$k-$bits-$nHash-$n-$nSamples.out -nomatchesout nomatch-$k-$bits-$nHash-$n-$nSamples.out -M $nSamples

