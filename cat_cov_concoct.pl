#!/usr/bin/perl
#Eric Morrison
#052518

use strict;
use warnings;

#SUBS#
sub read_file_list_extract_sample_names{
    my($dir, $covFileBase) = @_;
    #system "ls $dir";
    chomp(my @files = `ls $dir`);
    my @samples;
    foreach my $file (@files){
        $file =~ /(.+)$covFileBase/;
        push(@samples, $1);
    }
    return(\@samples);
}

sub cat_len_cov{
    my($samplesRef, $dir, $covFileBase) = @_;
    my @samples = @$samplesRef;
    
    my %cov;
    foreach my $sample (@samples){
        open(IN, $dir.$sample.$covFileBase) || die "Can't open coverage file for $sample.\n";
        chomp(my @cov = <IN>);
        foreach my $cov (@cov){
            my @node = split("\t", $cov);
            if($node[0] =~ /genome/i){next;}
            $node[0] =~ /NODE_(\d+)_/;
            my $nodeNum = $1;
            $cov{$nodeNum}{$node[0]}{$sample} = $node[2];
        }
    }
    return(\%cov);
}

sub print_cov{
    my($samplesRef, $covRef, $out) = @_;
    my @samples = @$samplesRef;
    my %cov = %$covRef;
    open(OUT, ">$out") || die "Can't open output.\n";
    print OUT "contig\tlength\t";
    foreach my $sample (@samples){
        print OUT "cov_mean_sample_", $sample, "\t";
    }
    print OUT "\n";
    foreach my $num (sort {$a <=> $b} keys %cov){
        foreach my $node (keys %{ $cov{$num} }){
            $node =~ /length_(\d+)_/;
            my $length = $1;
            print OUT $node, "\t", $length, "\t";
            foreach my $sample (@samples){
                if(defined($cov{$num}{$node}{$sample}) == 0){
                    print OUT 0, "\t";
                }else{
                    print OUT $cov{$num}{$node}{$sample}, "\t";
                }
            }
        }
        print OUT "\n";
    }
}
#MAIN#
{
    my $dir = $ARGV[0];
    my $out = $ARGV[1];
    
    my $covFileBase = "_coverage_by_sequence.txt";
    my $samplesRef = read_file_list_extract_sample_names($dir, $covFileBase);
    my $covRef = cat_len_cov($samplesRef, $dir, $covFileBase);
    print_cov($samplesRef, $covRef, $out);
}
