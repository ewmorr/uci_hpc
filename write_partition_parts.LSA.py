#!/usr/bin/env python
#this is mod of script from Cleary et al. 2015 LSA pipeline (EW Morrison 061318)
### THIS MAY OCCUPY ~10-50GB OF /tmp SPACE PER JOB (Doesn't seem likely actually, more like a few Gb and 10Gb max -EWM)

import glob,os
import sys,getopt
import gzip
import numpy as np
from collections import defaultdict
from fastq_reader import Fastq_Reader

def max_log_lik_ratio(s,bkg,h1_prob=0.8,thresh1=3.84,thresh2=np.inf):
	LLR = [(None,None)]
	read_match_sum = s[-1]
	del s[-1]
	v1 = read_match_sum*h1_prob*(1-h1_prob)
	m1 = read_match_sum*h1_prob
	for k,sect_sum in s.items():
		if sect_sum > read_match_sum*bkg[k]:
			v2 = read_match_sum*bkg[k]*(1-bkg[k])
			m2 = read_match_sum*bkg[k]
			llr = np.log(v2**.5/v1**.5) + .5*((sect_sum-m2)**2/v2 - (sect_sum-m1)**2/v1)
			LLR.append((llr,k))
	LLR.sort(reverse=True)
	K = []
	if LLR[0][0] > thresh1:
		K.append(LLR[0][1])
	for llr,k in LLR[1:]:
		if llr > thresh2:
			K.append(k)
		else:
			break
	return K

help_message = 'usage example: python write_partition_parts.py -r 1 -i /project/home/hashed_reads/ -o /project/home/cluster_vectors/ -t /tmp/dir/ -s /tmp/hashdir/ -n /node/scratch/ Scratch dir -n should nicely point to /scratch/username on the UCI HPC so as to write to local compute node in a directory assigned to username. The subdir need not (cannot) already exist (unless one has hacked the compute node...). Tmp dirs (-t and -s) should kindly point to /fast-scratch/somedirs/ that already exist'
if __name__ == "__main__":
	try:
        	opts, args = getopt.getopt(sys.argv[1:],'hr:i:o:t:s:n:',["--filerank=","inputdir=","outputdir=","tmpdir=","hashdir=","nodescratch="])
	except:
		print help_message
		sys.exit(2)
	for opt, arg in opts:
		if opt in ('-h','--help'):
			print help_message
			sys.exit()
		elif opt in ('-r',"--filerank"):
			fr = int(arg)-1
		elif opt in ('-i','--inputdir'):
			inputdir = arg
			if inputdir[-1] != '/':
				inputdir += '/'
		elif opt in ('-o','--outputdir'):
			outputdir = arg
			if outputdir[-1] != '/':
				outputdir += '/'
		elif opt in ('-t','--tmpdir'):
			tmpdir = arg
			if tmpdir[-1] != '/':
				tmpdir += '/'
		elif opt in ('-s','--tmphashdir'):
			hashdir = arg
			if hashdir[-1] != '/':
				hashdir += '/'
        	elif opt in ('-n','--nodescratch'):
			nodescratch = arg
			if nodescratch[-1] == '/':
				nodescratch = nodescratch[:-1]
	hashobject = Fastq_Reader(inputdir,outputdir)
	cp = np.load(hashobject.output_path+'cluster_probs.npy')
	cluster_probs = dict(enumerate(cp))
	Hashq_Files = glob.glob(os.path.join(hashobject.input_path,'*.hashq.*'))
	Hashq_Files = [fp for fp in Hashq_Files if '.tmp' not in fp]
	Hashq_Files.sort()
	infile = Hashq_Files[fr]
	#new code to copy hashq.gz to /fast-scratch EWM
	hashdir += str(fr) + '/'
	os.system('mkdir '+hashdir)
	os.system('cp '+infile+' '+hashdir)
	infileScratch = os.path.join(hashdir,os.path.basename(infile))
	#Add path to node local /scratch dir for writes EWM
	#each job is writing to as many files as N paritions (with a large dataset this can be thousands), writing across cluster network read by read causes problems..
	nodescratch += '_' + str(fr) + '/'
	os.system("mkdir "+nodescratch)
	outpart = infile[-6:-3]
	sample_id = infile[infile.rfind('/')+1:infile.index('.hashq')]
	tmpdir += str(fr) + '/'
	os.system('mkdir '+tmpdir)
	G = [open('%s%s.%s.cols.%d' % (tmpdir,sample_id,outpart,i),'w') for i in range(0,2**hashobject.hash_size,2**hashobject.hash_size/50)]
	
	f = gzip.open(infileScratch)
	r_id = 0
	for a in hashobject.hash_read_generator(f):
		for x in a[2]:
			G[int(x*50/2**hashobject.hash_size)].write('%d\t%d\n' % (x,r_id))
		r_id += 1
	R = r_id
	f.close()
	for g in G:
		g.close()
	if R < 50:
		print 'Fewer than 50 reads...doing nothing'
        	os.system('rm -rf '+tmpdir)
        	os.system('rm -rf '+hashdir)
		os.system('rm -rf '+nodescratch)
	else:
		ClusterFile = open(hashobject.output_path+'cluster_cols.npy')
		ValueFile = open(hashobject.output_path+'cluster_vals.npy')
		G = [open('%s%s.%s.ids.%d' % (tmpdir,sample_id,outpart,i),'w') for i in range(0,R,R/50)]
		# If sharing ClusterFile among many jobs is not practical, we may aggregate jobs below by 1/50 ClusterFile fractions across samples (so each job reads 1 fraction)
		for i in range(0,2**hashobject.hash_size,2**hashobject.hash_size/50):
			os.system('sort -nk 1 %s%s.%s.cols.%d -o %s%s.%s.cols.%d' % (tmpdir,sample_id,outpart,i,tmpdir,sample_id,outpart,i))
			f = open('%s%s.%s.cols.%d' % (tmpdir,sample_id,outpart,i))
			ColId = np.fromfile(f,dtype=np.int64,sep='\t')
			f.close()
			os.system('rm %s%s.%s.cols.%d' % (tmpdir,sample_id,outpart,i))
			C = np.fromfile(ClusterFile,dtype=np.int16,count=5*min(2**hashobject.hash_size/50,2**hashobject.hash_size-i))
			V = np.fromfile(ValueFile,dtype=np.float32,count=min(2**hashobject.hash_size/50,2**hashobject.hash_size-i))
			c0 = None
			outlines = [[] for _ in G]
			for j in range(0,len(ColId),2):
				col,id = ColId[j:j+2]
				if col != c0:
					ci = col % (2**hashobject.hash_size/50)
					c = C[ci*5:(ci+1)*5]
					c = c[np.nonzero(c)[0]] - 1
					c0 = col
				if len(c) > 0:
					v = V[ci]
					newline = '%d\t%f' % (id,v)
					for x in c:
						newline += '\t%d' % (x)
					outlines[id*50/R].append(newline+'\n')
			for g,l in zip(G,outlines):
				g.writelines(l)
			del C
			del V
		ClusterFile.close()
		ValueFile.close()
		for g in G:
			g.close()
		for i in range(0,R,R/50):
			os.system('sort -nk 1 %s%s.%s.ids.%d -o %s%s.%s.ids.%d' % (tmpdir,sample_id,outpart,i,tmpdir,sample_id,outpart,i))
		f = gzip.open(infileScratch)
		r_id = 0
		G = iter(open('%s%s.%s.ids.%d' % (tmpdir,sample_id,outpart,i)) for i in range(0,R,R/50))
		g = G.next()
		id_vals = np.fromstring(g.readline(),sep='\t')
		EOF = False
		CF = {}
		reads_written = 0
		unique_reads_written = 0
		
		#The for loop processes each read individually until break at EOF; read is a[0]
		for a in hashobject.hash_read_generator(f):
			while id_vals[0] < r_id:
				id_vals = np.fromstring(g.readline(),sep='\t')
				# revert to gross old behavior
				if len(id_vals) == 0:
					id_vals = [-1]
				if id_vals[0] == -1:
					try:
						g = G.next()
						id_vals = np.fromstring(g.readline(),sep='\t')
						# revert to gross old behavior
						if len(id_vals) == 0:
							id_vals = [-1]
					except:
						EOF = True
			if EOF:
				break
			
			D = defaultdict(float)
			while id_vals[0] == r_id:
				D[-1] += id_vals[1]
				for clust in id_vals[2:]:
					D[clust] += id_vals[1]
				try:
					id_vals = np.fromstring(g.readline(),sep='\t')
					# revert to gross old behavior
					if len(id_vals) == 0:
						id_vals = [-1]
				except:
					break
			best_clusts = max_log_lik_ratio(D,cluster_probs)
			for best_clust in best_clusts: #Append seqs to hash value instead of writing line by line EWM
				try:
					CF[best_clust] += a[0]+'\n'
				except:
					CF[best_clust] = a[0]+'\n'
				reads_written += 1
			if len(best_clusts) > 0:
				unique_reads_written += 1
			r_id += 1
		f.close()
		#write files one by one and all data, in one write (should only be a few 10s of Kb), old way held open as many as 200 files from pop. of files == N partitions, and wrote seq. by seq. (yuck) EWM
		for cluster,seqs in CF.items():
			try: #The try is probably mostly pointless bc on an uninitialized run the dirs should not be there...
				os.system('mkdir %s%d/' % (nodescratch,cluster))
				clustFile = open('%s%d/%s.fastq.%s' % (nodescratch,cluster,sample_id,outpart),'w')
				clustFile.write(seqs)
				clustFile.close()
			except:
				clustFile = open('%s%d/%s.fastq.%s' % (nodescratch,cluster,sample_id,outpart),'w')
				clustFile.write(seqs)
				clustFile.close()
			clustFilePath = ('%s%d/%s.fastq.%s' % (nodescratch,cluster,sample_id,outpart))
			clustDir = os.path.dirname(clustFilePath)
			fileName = os.path.basename(clustFilePath)
			outDir = ('%s%d' % (hashobject.output_path, cluster))
			outPath = os.path.join(outDir, fileName)
			
			try:
				os.system('mv '+clustFilePath+' '+outPath)
				os.system('rmdir '+clustDir)
			except:
				os.system('mkdir '+outDir)
				os.system('mv '+clustFilePath+' '+outPath)
				os.system('rmdir '+clustDir)	
		os.system('rm -rf '+tmpdir)
		os.system('rm -rf '+hashdir)
		os.system('rm -rf '+nodescratch)
		print 'total reads written:',reads_written
		print 'unique reads written:',unique_reads_written
		
