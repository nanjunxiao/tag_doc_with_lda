# coding=utf-8
#__author__ = 'nanjunxiao'

import sys
import os
import time
import math
import pymongo
import jieba
import json
from mylog import Logger
from simhash import Simhash,SimhashIndex

#==========================================
timestep=86400
mongoip='***'; mongoport=27017; mongodb='***'; mongocollection='***'
mongouser='***'; mongopwd='***'

exedirname=os.path.dirname(os.path.abspath(sys.argv[0]) )
stopwordsfilename = '%s/extra_dict/stop_words.utf8' %exedirname
docfile='%s/result/weibo_doc.txt'%exedirname; cutfile='%s/result/weibo_cut.txt'%exedirname
thetafile='%s/result/model-final.theta'%exedirname;topicfile='%s/result/model-final.twords'%exedirname

topicnum=100;iternum=3000
#==========================================
def dict2json(djson):
	try:
		text = json.dumps(djson,ensure_ascii = False)
	except Exception,e:
		print >> sys.stderr, djson, e
		return ''
	return text

def json2dict(text):
	try:
		djson = json.loads(text,encoding='utf-8')
	except Exception,e:
		print >> sys.stderr, text , e
		return None
	return djson

'''consine'''
def consinelen(sorted_list):
	powerone = 0.0
	for k, v in sorted_list:
		powerone += v * v
	return math.sqrt(powerone)

def consine(dictone, dicttwo):
	if len(dictone) == 0 or len(dicttwo) == 0:
		return 0.0
	# sorted_one = sorted(dictone.items(),key=lambda d:d[0])
	sorted_one = sorted(dictone.items())
	sorted_two = sorted(dicttwo.items())
	sum = 0.0
	i = 0
	j = 0
	while i < len(sorted_one) and j < len(sorted_two):
		if sorted_one[i][0] == sorted_two[j][0]:
			sum += sorted_one[i][1] * sorted_two[j][1]
			i += 1
			j += 1
		elif sorted_one[i][0] < sorted_two[j][0]:
			i += 1
		else:
			j += 1
	# if sum-0.0 < 0.001:
	if sum <= 0.0:
		return 0.0
	else:
		powerone = consinelen(sorted_one)
		powertwo = consinelen(sorted_two)
		if powerone > 0.0 and powertwo > 0.0:
			return sum / (powerone * powertwo)
		else:
			return 0.0

"""文档属于那个类"""
def whichcluster(listweight):
	return max(enumerate(listweight), key=lambda x: x[1])[0]

"""文档向类中心向量叠加"""
def addcenter(clustercenters, which, lineweight):
	for i in range(len(lineweight)):
		clustercenters[which][i] += lineweight[i]

"""adapter"""
def cosineadapter(vlist, centerlist):
	vdict = {}
	centerdict = {}
	id = 0
	for i in range(len(vlist)):
		vdict[id] = vlist[i]
		centerdict[id] = centerlist[i]
		id += 1
	return consine(vdict, centerdict)

"""每个类中所有文档距中心sim"""
def cossim2center(k, clusterdict_list, clustercenters):
	cossim_dict = {}#lineid-sim
	for i, vlist in clusterdict_list[k].items():
		cossim = cosineadapter(vlist, clustercenters[k])
		# print cossim
		cossim_dict[i]=cossim
	return sorted(cossim_dict.items(),key=lambda d:d[1],reverse=True)

"""topic,topic下文档按相似度降序排列"""
def showtopicwithdocs(K,thetafile,topicfile,docfile,showfile):
	ftheta = open(thetafile,'r')
	ftopic = open(topicfile,'r')
	fdoc = open(docfile,'r')
	fshow = open(showfile,'w')

	clustercenters = []#[[],[] ]
	clusterdict_list = []#[{2:[w1,w2,w3,w4],3:[] },{}]
	for i in range(K):
		clustercenters.append([0.0]*K)
		clusterdict_list.append({})
	lineno = 0
	for line in ftheta:
		line_list = line.strip().split()
		line_list = [float(w) for w in line_list]
		which = whichcluster(line_list)
		clusterdict_list[which][lineno] = line_list
		addcenter(clustercenters,which,line_list)
		lineno+=1
	################
	doc_list = fdoc.readlines()
	topicid = -1
	topicwords = ''
	for line in ftopic:
		if line.startswith('Topic'):
			if topicid==-1:
				topicid += 1;topicwords = ''
				continue
			else:
				#fshow.write(topicwords+'\n')
				#sorted_doc2center_sims = cossim2center(topicid,clusterdict_list,clustercenters)
				#for lineno,sim in sorted_doc2center_sims:
					#fshow.write('cosinesim:%f\tlineno:%d\n' %(sim,lineno))
					#fshow.write('%s\n' %(doc_list[lineno].strip() ))
				#fshow.write('===========================================================\n')
				topic_json = {}
				topic_json['topicid'] = topicid; topic_json['topicwords'] = topicwords; topic_json['doclist'] = []
				sorted_doc2center_sims = cossim2center(topicid,clusterdict_list,clustercenters)
				for lineno,sim in sorted_doc2center_sims:
					one_doc = doc_list[lineno].strip().split()
					topic_json['doclist'].append({'docid':one_doc[0],'consinesim':sim,'lineno':lineno,'text':one_doc[1] })
				#print dict2json(topic_json)
				#sys.exit(1)
				fshow.write(dict2json(topic_json)+'\n' )
			topicid += 1;topicwords = ''
			continue
		topicwords += line.strip().split()[0]+' '

	topic_json = {}
	topic_json['topicid'] = topicid; topic_json['topicwords'] = topicwords; topic_json['doclist'] = []
	sorted_doc2center_sims = cossim2center(topicid,clusterdict_list,clustercenters)
	for lineno,sim in sorted_doc2center_sims:
		one_doc = doc_list[lineno].strip().split()
		topic_json['doclist'].append({'docid':one_doc[0],'consinesim':sim,'lineno':lineno,'text':one_doc[1] })
	fshow.write(dict2json(topic_json)+'\n' )

	fshow.close()
	fdoc.close()
	ftopic.close()
	ftheta.close()

def loadstopwords(filename):
	fp = open(filename, 'rb')
	stopwords = set()
	for line in fp:
		#jieba cut is unicode
		stopwords.add(line.strip('\n').decode('utf-8'))
	fp.close()
	return stopwords

def getweibo_cut(curtimestamp,lasttimestamp):
	try:
		uri = 'mongodb://%s:%s@%s:%d/%s' % (mongouser,mongopwd,mongoip,mongoport,mongodb)
		logger.info('try to connect mongo. %s' %uri)
		connection = pymongo.MongoClient(uri)
		weibodb = connection.weibo_status
		weibocollection = weibodb.status
	except Exception,e:
		logger.critical('connect mongo error: %s' %(e) )
		sys.exit(-1)

	logger.info('connect mongo ok.' )

	try:
		logger.info('{create_time:{$gte:%ld,$lt:%ld} }' %(lasttimestamp,curtimestamp) )
		status_count = weibocollection.find({'create_time':{'$gte':lasttimestamp,'$lt':curtimestamp} }).count()
		logger.info('status_count: %d' %status_count)
		if status_count < 10:
			connection.close();mylogger.close()
			sys.exit(0)
		stopwords = loadstopwords(stopwordsfilename)
		fdoc=open(docfile,'w');fcut=open(cutfile,'w')
		num=0;simnum=0;cutnum=0
		#simhash
		index = SimhashIndex({})
		for one in weibocollection.find({'create_time':{'$gte':lasttimestamp,'$lt':curtimestamp} }):
			weibo_id = str(one['_id'])
			weibo_text = one['data']['text'].strip()
			text_sh = Simhash(weibo_text )
			if len(index.get_near_dups(text_sh) ) == 0: #not find sim
				#cut
				text_seg = jieba.cut(weibo_text)
				text_result = list(set(text_seg) - stopwords)
				content = ' 1 '.join(text_result)
				if content != '':
					fdoc.write(weibo_id+'\t'+weibo_text.encode('utf-8')+'\n');fcut.write(content.encode('utf-8')+' 1\n')
					cutnum += 1
				simnum += 1
			num += 1
			index.add(num,text_sh)
	except pymongo.errors,e:
		logger.critical('mongo find error: %s' %e)
		sys.exit(-2)

	logger.info('simnum: %d ' %simnum);
	logger.info('cutnum: %d ' %cutnum);
	connection.close()
	fdoc.close();fcut.close()

def main():
	curtimestamp=0;lasttimestamp=0
	if len(sys.argv)==4 and sys.argv[1]=='-BETime':
		lasttimestamp = long(sys.argv[2])
		curtimestamp = long(sys.argv[3])
	elif len(sys.argv)==2 and sys.argv[1]=='-SYSTime':
		curtimestamp = time.time()
		lasttimestamp = curtimestamp - timestep
	else:
		print 'usage: \n' \
				'exe [-BETime] [begintime] [endtime]\n' \
				'exe [-SYSTime]'
		sys.exit(-3)

	getweibo_cut(curtimestamp,lasttimestamp)
	logger.info('----------------------------------------------------->')
	logger.info('K:%d\tIter:%d\t%s\n' %(topicnum,iternum,time.asctime()))
	alpha = 50.0/topicnum
	#os.system('time %s/lda -est -ntopics %d -niters %d -savestep %d -twords %d -dfile %s'\
			#%(exedirname,topicnum,iternum,10000,15,cutfile) )

	logger.info('time %s/ompi_lda --num_topics=%d --alpha=%f --beta=0.1 --compute_loglikelihood=false --training_data_file=%s --model_file=%s/result/model_file --twords_file=%s --theta_file=%s --burn_in_iterations=%d --accumulating_iterations=%d --num_openmp_threads=8 '\
		%(exedirname,topicnum,alpha,cutfile,exedirname,topicfile,thetafile,iternum,iternum*0.5) )
	os.system('time %s/ompi_lda --num_topics=%d --alpha=%f --beta=0.1 --compute_loglikelihood=false --training_data_file=%s --model_file=%s/result/model_file --twords_file=%s --theta_file=%s --burn_in_iterations=%d --accumulating_iterations=%d --num_openmp_threads=8 '\
		%(exedirname,topicnum,alpha,cutfile,exedirname,topicfile,thetafile,iternum,iternum*0.5) )
	showfile='%s/result/%s-%s.result' %(exedirname, time.strftime('%Y%m%d%H%M%S',time.localtime(lasttimestamp) ),time.strftime('%Y%m%d%H%M%S',time.localtime(curtimestamp) ) )
	showtopicwithdocs(topicnum,thetafile,topicfile,docfile,showfile)
	logger.info('K:%d\tIter:%d\t%s\n' %(topicnum,iternum,time.asctime()))

if __name__ == '__main__':
	#logger
	mylogger = Logger(logname='%s/cluster.log'%exedirname, loglevel=3, callfile=__file__)
	logger = mylogger.get_logger()

	main()

	#logger
	mylogger.close()
