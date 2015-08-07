#!/usr/bin/env python
# coding=utf-8
import sys, glob
import os
import math
import time
import json
import glob
import jieba

sys.path.append('gen-py')
#sys.path.insert(0, glob.glob('../../lib/py/build/lib.*')[0])
from rpc import DocServlet
from rpc.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
#from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol
from thrift.server import TServer

#=================================
exedirname=os.path.dirname(os.path.abspath(sys.argv[0]) )
stopwordsfilename = '%s/extra_dict/stop_words.utf8' %exedirname
inference_data_file = '%s/result/toinfer.doc'%exedirname
inference_result_file = '%s/result/inference_result'%exedirname
model_file = '%s/result/model_file'%exedirname
#=================================

def json2dict(text):
	try:
		djson = json.loads(text,encoding='utf-8')
	except Exception,e:
		print >> sys.stderr, text , e
		return None
	return djson
def loadstopwords(filename):
	fp = open(filename, 'rb')
	stopwords = set()
	for line in fp: 
		#jieba cut is unicode
		stopwords.add(line.strip('\n').decode('utf-8'))
	fp.close()
	return stopwords
def loadtrainresult(showfilename):
	fp = open(showfilename,'r')
	topic_list = []
	for line in fp:
		topic_list.append( json2dict(line.strip() ) )
	fp.close()
	return topic_list
"""文档属于那个类"""
def whichcluster(listweight):
	return max(enumerate(listweight), key=lambda x: x[1])[0]

class DocServletHandler:
	def __init__(self):
		self.stopwords = loadstopwords(stopwordsfilename)
		filebeg = time.strftime('%Y%m%d',time.localtime(time.time() - 86400) ); fileend = '.result'; showfilename = glob.glob('%s/result/%s*%s'%(exedirname,filebeg,fileend) )[0]
		#print 'show: ',showfilename
		self.topic_list = loadtrainresult(showfilename)
		self.topicnum = len(self.topic_list);self.alpha = 50.0/self.topicnum
		pass
	def Ping(self, sth):
		print 'receive: %s\n' % (sth)
		return 'Pang...'

	def Infer(self, docs, burn_in_iterations=15, accumulating_iterations=10, docnumoftopic=15):
		topics = []; toinfer_index= []
		try:
			fp = open(inference_data_file,'w')
			inferdocnum = 0
			for doc in docs:
				text_seg = jieba.cut(doc.text.strip() )
				text_result = list(set(text_seg) - self.stopwords)
				content = ' 1 '.join(text_result)
				if content != '':
					toinfer_index.append(inferdocnum)	
					fp.write(content.encode('utf-8')  +'\n')
				inferdocnum += 1
			fp.close()
			print '----------------------------------------------------->'
			print 'N:%d\t%s\n' %(len(docs),time.asctime())
			print ('time ./infer --alpha %f --beta 0.1 --inference_data_file %s --inference_result_file %s --model_file %s --burn_in_iterations %d --accumulating_iterations %d '\
				%(self.alpha,inference_data_file,inference_result_file,model_file, burn_in_iterations,accumulating_iterations) )
			os.system('time ./infer --alpha %f --beta 0.1 --inference_data_file %s --inference_result_file %s --model_file %s --burn_in_iterations %d --accumulating_iterations %d '\
				%(self.alpha,inference_data_file,inference_result_file,model_file, burn_in_iterations,accumulating_iterations) )
			fp = open(inference_result_file,'r')
			infer_result_which = []
			for line in fp:
				line_list = line.strip().split()
				infer_result_which.append(whichcluster(line_list) )
			fp.close()	
			counter=0
			for i,index in enumerate(toinfer_index):
				for j in range(index-counter ):
					topics.append(Topic_Info() )
				onetopic = Topic_Info();which=infer_result_which[i];onetopic.topicid=which;onetopic.topicwords=self.topic_list[which]['topicwords'].encode('utf-8');
				docnumoftopic = min(docnumoftopic,len(self.topic_list[which]['doclist'] ) )
				for jj in range(docnumoftopic):
					onetopic.doclist.append(Doc_Info(self.topic_list[which]['doclist'][jj]['docid'], self.topic_list[which]['doclist'][jj]['text'].encode('utf-8'),self.topic_list[which]['doclist'][jj]['consinesim'] ) )
				topics.append(onetopic);counter=index+1
			if counter<len(docs):
				for j in range(len(docs)-counter ):
					topics.append(Topic_Info() )
			print 'N:%d\t%s\n' %(len(docs),time.asctime())
		except Thrift.TException, tx:
			print "%s" % (tx.message)
		except Exception,e :
			print >> sys.stderr, e
		
		return topics

	def Sender2(self,docs):
		for doc in docs:
			print doc.text

handler = DocServletHandler()
processor = DocServlet.Processor(handler)
transport = TSocket.TServerSocket("127.0.0.1",port=9099)
tfactory = TTransport.TBufferedTransportFactory()
#pfactory = TBinaryProtocol.TBinaryProtocolFactory()
pfactory = TCompactProtocol.TCompactProtocolFactory()
server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

print 'Starting the server...'
server.serve()
print 'done.'
