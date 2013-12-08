
if($#ARGV < 1){
    die "Usage: jaccardstats.pl <pairwise jaccard scores> <mh results> \n";
}

$scores = $ARGV[0];
$europarlenall = $ARGV[0];

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

open(SENTIN,"<$europarlenall");
my $lastcluster;
my @clustersentences;
for $line (<SENTIN>){
    chomp $line;
    print $line,"\n";
    $line =~ /^(.*)\t(.*)\t(.*)\t(.*)\t(.*)$/;
    $cluster = $1;
    $id = $2;
    $count = $3;
    $language = $4;
    $sentence = $5;
    print "CLUSTER $cluster\n";
    print "CLUSTER $cluster\n";
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
    $lastcluster = $cluster;
    push(@clustersentences,$id);

}
close(FILEIN);

for $sim (keys %simhist){
    print $sim, $simhist{$keys},"\n";
}
