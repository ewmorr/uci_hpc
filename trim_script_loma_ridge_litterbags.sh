#!/bin/bash
#$ -N trimLRlitterbags
#$ -q free64
#$ -m eas #email me job status
#$ -o trim_loma_ridge_litterbags_out.txt
#$ -e trim_loma_ridge_litterbags_err.txt
#$ -pe openmp 32-64
#$ -R y

module purge
module load trimmomatic/0.35
module load BBMap/37.50


#do adapter and quality trimming with trimmomatic
#do phiX filter with bbduk.sh

#point TAR to full path to a tar ball of fastq.gz files that will be extracted to dir
#TAR=$BIODIR/clim_grad_test_set.tar

#FILE=${TAR##*/}
#DIR=${TAR%/*}
#SUBDIR=${FILE%.tar}


#or instead of tarball above just point to a dir

DIR=$BIODIR
cd $DIR

SUBDIR=loma_ridge_litterbags

#tar -xf $FILE

mkdir ${SUBDIR}_trim

for s in $SUBDIR/*_R1.fastq.gz
do

s=${s##*/}
sampleID=${s%_R1.fastq.gz}

FORWARD=${sampleID}_R1.fastq.gz
REVERSE=${sampleID}_R2.fastq.gz


>&2 echo "SAMPLE $sampleID"

time java -jar /data/apps/trimmomatic/0.35/trimmomatic-0.35.jar PE -threads $CORES $SUBDIR/$FORWARD $SUBDIR/$REVERSE\
        ${SUBDIR}_trim/paired_$FORWARD ${SUBDIR}_trim/unpaired_$FORWARD\
        ${SUBDIR}_trim/paired_$REVERSE ${SUBDIR}_trim/unpaired_$REVERSE\
        ILLUMINACLIP:/data/apps/trimmomatic/0.35/adapters/NexteraPE-PE.fa:2:30:10\
        SLIDINGWINDOW:4:15 MINLEN:36

#phiX filtering
time bbduk.sh -Xmx1g in1=${SUBDIR}_trim/paired_$FORWARD in2=${SUBDIR}_trim/paired_$REVERSE out1=${SUBDIR}_trim/qcd_$FORWARD out2=${SUBDIR}_trim/qcd_$REVERSE ref=/data/apps/BBMap/37.50/resources/phix174_ill.ref.fa.gz k=31 hdist=1

done


