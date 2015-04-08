# Wikiduper #

Package for similar sentence detection on Wikipedia with hadoop.

To produce similar sentence clusters, run the pipeline script:

    scripts/run-pipeline.sh <wiki-dump-file> <output-prefix-string> <nHash> <bits> <k> <n> <l>

E.g.,

     scripts/run-pipeline.sh enwiki-20130708-pages-articles-multistream.xml enwiki-20130708-sentences 20 60 10 10 12


The pipeline script preprocess wikipedia data, runs the minhash algorithm, and performs a transitive closure to merge clusters that share at least one sentence. Finally, it maps clusters back to human-readable sentences.

the parameters of the pipeline script are as follows
* *wiki-dump-file* - A wikipedia dump (enwiki), can be obtained from https://dumps.wikimedia.org/backup-index.html.
* *output-prefix-string* - A prefix for naming output files
* *nHash* - The size of the hash family. The hashes that make up the minhash signatures will be drawn from this pool.
* *bits* - The number of bits in each hash.
* *k* - The number of hashes in each signature.
* *n* - The number of signatures to generate for each sentence.
* *l* - The length of a shingle.

Exact tuning of parameters for minhash is difficult. The authors have found parameters in the range of n=10, k=8, and l=10 to be decent. For fixed k, as n increases, the number of false positives increases, meaning you will have more bad sentence clusters. For fixed n, as k increases, the number of false negatives increases, which means that you will have fewer sentence clusters, but more accuracy. Best practice would be to allow for more false positives, but have a secondary processing step (e.g. an edit distance test) to weed out bad matches.

The output of the pipeline script is a single text file named with the scheme  *output-prefix-string-nHash-k-n-l-bits*, e.g. enwiki-20130708-sentences-20-10-10-12-60. The text file contains key, value pairs where the key is a cluster number, and the value is a pair giving the title of the artible, followed by the sentence. The file should be grouped by cluster number.

## Running locally ##

For convenience, a script to run the pipeline locally is also provided. This script is scripts/run-pipeline-local.sh and takes the same arguments as above. The local version of the script uses the local version of hadoop that can be run out of the box without a cluster set-up. A smaller set of test data taken from a larger Wikipedia dump and suitable for use with the local version of the pipeline  can be found in data/Maryland.xml.

Code licensed under Apache License v2.0 (http://www.apache.org/licenses/LICENSE-2.0)
