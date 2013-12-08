
if($#ARGV < 1){
    die "Usage: jaccardstats.pl <pairwise jaccard scores> <mh results> \n";
}

$scores = $ARGV[0];
$europarlenall = $ARGV[1];
$matchout = "$europarlenall.matches";
$nonmatchout = "$europarlenall.nonmatches";

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

open(MATCHOUT,">$matchout");
open(FILEIN,"<$europarlenall");
my $lastcluster;
my @clustersentences;
my %matchset;
for $line (<FILEIN>){
    chomp $line;
    #print $line,"\n";
    $line =~ /^(.*)\t(.*)\t(.*)\t(.*)\t(.*)$/;
    $cluster = $1;
    $id = $2;
    $count = $3;
    $language = $4;
    $sentence = $5;
    #print "CLUSTER $cluster\n";
    #print "ID $id\n";
    if(!$lastcluster){
	$lastcluster = $cluster;
    }
    if($lastcluster != $cluster){
	$maxsim = 0.0;
	for($i=0;$i<$#clustersentences;$i++){
	    $id1 = $clustersentences[$i];
	    for($j=0;$j<$#clustersentences;$j++){
		$id2 = $clustersentences[$j];
		$sim = $scores{"$id1,$id2"};
		$simalt = $scores{"$id2,$id1"};
		if($sim && $simalt){
		    die "Non symmetric scores found!\n";
		}

		if(!$sim && !$simalt){
		    print "No score for pair $id1 $id2\n";
		}
		$matchset{$id1,$id2} = 1;
		$matchset{$id2,$id1} = 1;
		if($sim){
		    if($sim > $maxsim){
			$maxsim = $sim;
		    }
		}
		if($simalt){
		    if($simalt > $maxsim){
			$maxsim = $sim;
		    }
		}
	    }
	}
	print MATCHOUT "$maxsim\n";
	@clustersentences = ();
    }
    $lastcluster = $cluster;
    push(@clustersentences,$id);

}
close(MATCHOUT);
close(FILEIN);
open(NONMATCHOUT,">$nonmatchout");

for($i=1;$i<=1000;$i++){
    for($j=1000;$j<=2000;$j++){
	if(!$matchset{"$i,$j"} && !$matchset{"$j,$i"}){
	    $sim = $scores{"$i,$j"};
	    $simalt = $scores{"$j,$i"};
	    if($sim && $simalt){
		die "Non symmetric scores found!\n";
	    }
	    if($sim){
		print NONMATCHOUT "$sim\n";
	    }
	    if($simalt){
		print NONMATCHOUT "$simalt\n";
	    }
	    
	}
    }
}
close(NONMATCHOUT);

