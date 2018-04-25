#!/usr/bin/env python

import sys, getopt
import glob, os

# MergeHash can maybe go on the hour queue

JobParams = {
	'CreateHash': {
		'outfile': """CreateHash_Job.sge""",
		'header': ["""#$ -N CreateHash""","""#$ -o PROJECT_HOME/Logs/CreateHash-Out.out""","""#$ -e PROJECT_HOME/Logs/CreateHash-Err.err""","""#$ -q pub8i""","""#$ -m beas""","""## -W 23:58"""],
		'body': ["""python LSA/create_hash.py -i PROJECT_HOME/original_reads/ -o PROJECT_HOME/hashed_reads/ -k 33 -s 31"""]},
	'HashReads': {
		'outfile': """HashReads_ArrayJob.sge""",
		'array': ["""original_reads/""","""*.fastq.*"""],
		'header': ["""#$ -t 1-""","""#$ -N HashReads""","""#$ -o PROJECT_HOME/Logs/HashReads-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/HashReads-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:56""","""## -M 8"""],
		# add -z option to omit reverse complimenting
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/hash_fastq_reads.py -r ${SGE_TASK_ID} -i PROJECT_HOME/original_reads/ -o PROJECT_HOME/hashed_reads/"""]},
	'MergeHash': {
		'outfile': """MergeHash_ArrayJob.sge""",
		'array': ["""original_reads/""","""*.fastq""",5],
		'header': ["""#$ -t 1-""","""#$ -N MergeHash""","""#$ -o PROJECT_HOME/Logs/MergeHash-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/MergeHash-Err-$TASK_ID.err""","""#$ -q pub8i""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 53:58""","""## -R 'rusage[mem=4]'""","""## -M 8"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/merge_hashq_files.py -r ${SGE_TASK_ID} -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/hashed_reads/"""]},
	'CombineFractions': {
		'outfile': """CombineFractions_ArrayJob.sge""",
		'array': ["""original_reads/""","""*.fastq""",1],
		'header': ["""#$ -t 1-""","""#$ -N CombineFractions""","""#$ -o PROJECT_HOME/Logs/CombineFractions-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/CombineFractions-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 23:58""","""## -R 'rusage[mem=8]'""","""## -M 20"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/merge_hashq_fractions.py -r ${SGE_TASK_ID} -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/hashed_reads/"""]},
	'GlobalWeights': {
		'outfile': """GlobalWeights_Job.sge""",
		'header': ["""#$ -N GlobalWeights""","""#$ -o PROJECT_HOME/Logs/GlobalWeights-Out.out""","""#$ -e PROJECT_HOME/Logs/GlobalWeights-Err.err""","""#$ -q pub8i""","""#$ -m beas""","""## -W 71:10""","""## -R 'rusage[mem=25]'""","""## -M 75"""],
		'body': ["""python LSA/tfidf_corpus.py -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/"""]},
	'KmerCorpus': {
		'outfile': """KmerCorpus_ArrayJob.sge""",
		'array': ["""hashed_reads/""","""*.count.hash"""],
		'header': ["""#$ -t 1-""","""#$ -N KmerCorpus""","""#$ -o PROJECT_HOME/Logs/KmerCorpus-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/KmerCorpus-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:58""","""## -R 'rusage[mem=32]'""","""## -M 45"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/kmer_corpus.py -r ${SGE_TASK_ID} -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/"""]},
	'KmerLSI': {
		'outfile': """KmerLSI_Job.sge""",
		'header': ["""#$ -N KmerLSI""","""#$ -o PROJECT_HOME/Logs/KmerLSI-Out.out""","""#$ -e PROJECT_HOME/Logs/KmerLSI-Err.err""","""#$ -q pub8i""","""#$ -m beas""","""#$ -pe openmp 7""","""## -R 'rusage[mem=4] span[hosts=1]'""","""## -M 10"""],#the header originally had gensim commands, if this fails it may be necessary to move them back to the header and manually modify the placement of the module load calls
		'body': ["""export PYRO_SERIALIZERS_ACCEPTED=serpent,json,marshal,pickle""","""export PYRO_SERIALIZER=pickle""","""python -m Pyro4.naming -n 0.0.0.0 > PROJECT_HOME/Logs/nameserver.log 2>&1 &""","""P1=$!""","""python -m gensim.models.lsi_worker > PROJECT_HOME/Logs/worker1.log 2>&1 &""","""P2=$!""","""python -m gensim.models.lsi_worker > PROJECT_HOME/Logs/worker2.log 2>&1 &""","""P3=$!""","""python -m gensim.models.lsi_worker > PROJECT_HOME/Logs/worker3.log 2>&1 &""","""P4=$!""","""python -m gensim.models.lsi_worker > PROJECT_HOME/Logs/worker4.log 2>&1 &""","""P5=$!""","""python -m gensim.models.lsi_worker > PROJECT_HOME/Logs/worker5.log 2>&1 &""","""P6=$!""","""python -m gensim.models.lsi_dispatcher 5 > PROJECT_HOME/Logs/dispatcher.log 2>&1 &""","""P7=$!""","""python LSA/kmer_lsi.py -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/""","""kill $P1 $P2 $P3 $P4 $P5 $P6 $P7"""]},
	'KmerClusterIndex': {
		'outfile': """KmerClusterIndex_Job.sge""",
		'header': ["""#$ -N KmerClusterIndex""","""#$ -o PROJECT_HOME/Logs/KmerClusterIndex-Out.out""","""#$ -e PROJECT_HOME/Logs/KmerClusterIndex-Err.err""","""#$ -q pub8i""","""## -R 'rusage[mem=1]'""","""## -M 35"""],
		# adjust cluster thresh (-t) as necessary
		'body': ["""python LSA/kmer_cluster_index.py -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/ -t 0.7""","""python LSFScripts/create_jobs.py -j KmerClusterParts -i ./""","""X=`sed -n 1p hashed_reads/hashParts.txt`""","""sed -i 's/%parts%/$X/g' LSFScripts/KmerClusterParts_ArrayJob.sge""","""python LSFScripts/create_jobs.py -j LSFScripts/KmerClusterMerge -i ./""","""X=`sed -n 1p cluster_vectors/numClusters.txt`""","""sed -i 's/%clusters%/$X/g' LSFScripts/KmerClusterMerge_ArrayJob.sge"""]},
	'KmerClusterParts': {
		'outfile': """KmerClusterParts_ArrayJob.sge""",
		# number of tasks is 2**hash_size/10**6 + 1
		#'array': ["""hashed_reads/""","""*.hashq.*"""],
		'header': ["""#$ -t 1-%parts%""","""#$ -N KmerClusterParts%parts%]""","""#$ -o PROJECT_HOME/Logs/KmerClusterParts-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/KmerClusterParts-Err-$TASK_ID.err""","""#$ -q pub8i""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:59""","""## -R 'rusage[mem=1:argon_io=3]'""","""## -M 4"""],
		###!!!
		# adjust cluster thresh (-t) as necessary - probably same as Index step (maybe slightly higher)
		###!!!
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/kmer_cluster_part.py -r ${SGE_TASK_ID} -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/ -t 0.7"""]},
	'KmerClusterMerge': {
		'outfile': """KmerClusterMerge_ArrayJob.sge""",
		# number of tasks is number of clusters
		#'array': ["""hashed_reads/""","""*.hashq.*"""],
		'header': ["""#$ -t 1-%clusters%""","""#$ -N KmerClusterMerge%clusters%]""","""#$ -o PROJECT_HOME/Logs/KmerClusterMerge-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/KmerClusterMerge-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:59""","""## -R 'rusage[mem=1]'""","""## -M 8"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/kmer_cluster_merge.py -r ${SGE_TASK_ID} -i PROJECT_HOME/cluster_vectors/ -o PROJECT_HOME/cluster_vectors/"""]},
	'KmerClusterCols': {
		'outfile': """KmerClusterCols_Job.sge""",
		'header': ["""#$ -N KmerClusterCols""","""#$ -o PROJECT_HOME/Logs/KmerClusterCols-Out.out""","""#$ -e PROJECT_HOME/Logs/KmerClusterCols-Err.err""","""#$ -q pub8i""","""## -W 71:58""","""## -R 'rusage[mem=40]'""","""## -M 70"""],
		'body': ["""python LSA/kmer_cluster_cols.py -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/"""]},
	'ReadPartitions': {
		'outfile': """ReadPartitions_ArrayJob.sge""",
		'array': ["""hashed_reads/""","""*.hashq.*"""],
		# MAKE SURE TO SET TMP FILE LOCATION
		'header': ["""#$ -t 1-""","""#$ -N ReadPartitions""","""#$ -o PROJECT_HOME/Logs/ReadPartitions-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/ReadPartitions-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 45:10""","""## -R 'rusage[mem=3:argon_io=3]'""","""## -M 20"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/write_partition_parts.py -r ${SGE_TASK_ID} -i PROJECT_HOME/hashed_reads/ -o PROJECT_HOME/cluster_vectors/ -t TMPDIR"""]},
	'MergeIntermediatePartitions': {
		'outfile': """MergeIntermediatePartitions_ArrayJob.sge""",
		'array': ["""cluster_vectors/""","""*.cluster.npy"""],
		'header': ["""#$ -t 1-""","""#$ -N MergeIntermediatePartitions""","""#$ -o PROJECT_HOME/Logs/MergeIntermediatePartitions-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/MergeIntermediatePartitions-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 1:55""","""## -M 2""","""## -R 'rusage[argon_io=3]'"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/merge_partition_parts.py -r ${SGE_TASK_ID} -i PROJECT_HOME/cluster_vectors/ -o PROJECT_HOME/read_partitions/"""]},
	# Check to make sure there are no files remaining in cluster_vectors/PARTITION_NUM/
	'SplitPairs': {
		'outfile': """SplitPairs_ArrayJob.sge""",
		'array': ["""cluster_vectors/""","""*.cluster.npy"""],
		'header': ["""#$ -t 1-""","""#$ -N SplitPairs""","""#$ -o PROJECT_HOME/Logs/SplitPairs-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/SplitPairs-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:59""","""## -R 'rusage[argon_io=3]'""","""## -M 8"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python LSA/split_read_pairs.py -r ${SGE_TASK_ID} -i PROJECT_HOME/read_partitions/ -o PROJECT_HOME/read_partitions/"""]},
	'PhylerClassify': {
		'outfile': """PhylerClassify_ArrayJob.sge""",
		'array': ["""cluster_vectors/""","""*.cluster.npy"""],
		'header': ["""#$ -t 1-""","""#$ -N PhylerClassify""","""#$ -o PROJECT_HOME/Logs/PhylerClassify-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/PhylerClassify-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:55""","""## -M 4""","""source /broad/software/scripts/useuse""","""reuse BLAST"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python misc/phyler_classify.py -r ${SGE_TASK_ID} -i PROJECT_HOME/read_partitions/ -o PROJECT_HOME/phyler/"""]},
	'PhylerIdentify': {
		'outfile': """PhylerIdentify_ArrayJob.sge""",
		'array': ["""cluster_vectors/""","""*.cluster.npy"""],
		'header': ["""#$ -t 1-""","""#$ -N PhylerIdentify""","""#$ -o PROJECT_HOME/Logs/PhylerIdentify-Out-$TASK_ID.out""","""#$ -e PROJECT_HOME/Logs/PhylerIdentify-Err-$TASK_ID.err""","""#$ -q free64""","""#$ -m as""",""""#$ -ckpt restart""","""## -W 3:55""","""## -M 2"""],
		'body': ["""sleep $(($SGE_TASK_ID % 60))""","""python misc/phyler_identify.py -r ${SGE_TASK_ID} -i PROJECT_HOME/read_partitions/ -o PROJECT_HOME/phyler/"""]},
	'PhylerSummary': {
		'outfile': """PhylerSummary_Job.sge""",
		'header': ["""#$ -N PhylerSummary""","""#$ -o PROJECT_HOME/Logs/PhylerSummary-Out.out""","""#$ -e PROJECT_HOME/Logs/PhylerSummary-Err.err""","""#$ -q free64""","""## -W 1:55""","""## -M 2"""],
		'body': ["""python misc/phyler_summary.py -i PROJECT_HOME/phyler/"""]}
}

CommonElements = {
	'header': ["""#!/bin/bash"""],
	'body': ["""module purge""","""module load anaconda/2.7-4.3.1""","""module load gnu_parallel/20170622""","""echo Date: `date`""","""t1=`date +%s`"""],
	'footer': ["""[ $? -eq 0 ] || echo 'JOB FAILURE: $?'""","""echo Date: `date`""","""t2=`date +%s`""","""tdiff=`echo 'scale=3;('$t2'-'$t1')/3600' | bc`""","""echo 'Total time:  '$tdiff' hours'"""]
}
					
help_message = 'usage example: python create_jobs.py -j HashReads -i /project/home/'
if __name__ == "__main__":
	job = 'none specified'
	try:
		opts, args = getopt.getopt(sys.argv[1:],'hj:i:',["--jobname","inputdir="])
	except:
		print help_message
		sys.exit(2)
	for opt, arg in opts:
		if opt in ('-h','--help'):
			print help_message
			sys.exit()
		elif opt in ('-j',"--jobname"):
			job = arg
		elif opt in ('-i','--inputdir'):
			inputdir = arg
			if inputdir[-1] != '/':
				inputdir += '/'
	try:
		params = JobParams[job]
	except:
		print job+' is not a known job.'
		print 'known jobs:',JobParams.keys()
		print help_message
		sys.exit(2)
	if params.get('array',None) != None:
		FP = glob.glob(os.path.join(inputdir+params['array'][0],params['array'][1]))
		if len(params['array']) == 3:
			FP = [fp[fp.rfind('/')+1:] for fp in FP]
			if params['array'][2] == -1:
				suffix = params['array'][1].replace('*','').replace('.','')
				FP = set([fp[:fp.index(suffix)] for fp in FP])
			else:
				FP = set([fp[:fp.index('.')] for fp in FP])
			FP = [None]*len(FP)*abs(params['array'][2])
		array_size = str(len(FP))
		params['header'][0] += array_size#+']'
		print job+' array size will be '+array_size
	f = open(inputdir+'LSFScripts/'+params['outfile'],'w')
	f.write('\n'.join(CommonElements['header']) + '\n')
	f.write('\n'.join(params['header']).replace('PROJECT_HOME/',inputdir) + '\n')
	f.write('\n'.join(CommonElements['body']) + '\n')
	f.write('\n'.join(params['body']).replace('PROJECT_HOME/',inputdir) + '\n')
	f.write('\n'.join(CommonElements['footer']) +'\n')
	f.close()
	
