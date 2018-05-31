#!/usr/bin/perl
#Eric Morrison
#053118

use strict;
use warnings;

#SUBS#
sub usage{
        print STDERR q(
Usage: perl split_contigs_bins.pl [contigs.fasta] [clusters.csv]

The script takes a fasta file of contigs as input,along with a file denoting cluster that the contigs belong to. The clusters file should list contig IDs along with cluster number in comma-seaprated format (e.g. the output of CONCOCT genome binning). Each set of contigs is writeen to a new fasta file with "cluster_#" appended to the file name. Cluster designations are assumed to be numeric.
    
);
        exit;
}
sub cluster_hash{
    my $clusters = $_[0];
    open(CLUST, "$clusters") || die "Can't open clusters file\n";
    my %clust;
    while(my $contig = <CLUST>){
        my @contig = split(",", $contig);
        $contig[1] =~ s/\D*//g;
        $clust{$contig[0]} = $contig[1];
    }
    return(\%clust);
}

sub fasta_hash{
    my $fasta = $_[0];
    open(FASTA, "$fasta") || die "Can't open fasta\n";
    chomp(my @fasta = <FASTA>);
    
    my %fas;
    while(defined($fasta[0]) == 1){
        my $id = $fasta[0];
        $id =~ s/>//;
        my $i = 1;
        while(defined($fasta[$i]) == 1 and $fasta[$i] !~ /^>/){
            $fas{$id} .= $fasta[$i];
            $i++;
        }
        splice(@fasta, 0, $i);
    }
    return(\%fas);
}

sub print_bins{
    my($clusRef, $fasRef, $basename, $ext) = @_;
    my %cluster = %$clusRef;
    my %fasta = %$fasRef;
    foreach my $seq (keys %cluster){
        open(FAS, ">>$basename.cluster_$cluster{$seq}.$ext") || die "Can't append to cluster file $cluster{$seq}\n";
        print FAS ">$seq\n$fasta{$seq}\n";
    }
}
#MAIN
{
    if(scalar(@ARGV) == 0 || $ARGV[0] =~ /-h/){
        usage;
    }

    my $fasta = $ARGV[0];
    my $clusters = $ARGV[1];
    
    $fasta =~ /(.+)\.(.+)/;
    my $basename = $1;
    my $ext = $2;

    my $clusterRef = cluster_hash($clusters);
    my $fastaRef = fasta_hash($fasta);
    print_bins($clusterRef, $fastaRef, $basename, $ext);
}
