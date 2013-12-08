
if($#ARGV < 2){
    die "Usage: jaccardstats.pl <pairwise jaccard scores> <mh results> \n";
}

$scores = $ARGV[0];
$europarlenall = $ARGV[0];
$europarlende = $ARGV[0];

my %scores;
open(FILEIN,"<$scores");
for $line (<FILEIN>){
    chomp $line;
    $line =~ /^(.*),(.*),(.*)$/;
    $i = $1;
    $j = $2;
    $sim = $3;
    $scores{"$i,$j"} = $sim;
}
close(FILEIN);

%simhist;

open(FILEIN,"<$europarlenall");
my $lastcluster;
my @clustersentences;
for $line (<FILEIN>){
    chomp $line;
    $line =~ /^(.*)\t(.*)\t(.*)\t(.*)\t(.*)$/;
    $cluster = $1;
    $sentence = $2;
    $count = $3;
    $language = $4;
    $sentence = $5;
    if(!$lastcluster){
	$lastcluster = $cluster;
    }
    if($lastcluster != $cluster){
	for($i=0;$i<$#clustersentences;$i++){
	    $sentence1 = $clustersentences[$i];
	    for($j=0;$j<$#clustersentences;$j++){
		$sentence2 = $clustersentences[$j];
		$sim = $scores{"$sentence1,$sentence2"};
		$simhist{$sim} += 1;
	    }
	}
	@clustersentences = ();
    }
    push(@clustersentences,$sentence);

}
close(FILEIN);

for $sim (keys %simhist){
    print $sim, $simhist{$keys},"\n";
}
