import sys, glob
sys.path.append('gen-py')
#sys.path.insert(0, glob.glob('../../lib/py/build/lib.*')[0])

from rpc import DocServlet
from rpc.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol

try:
  # Make socket
	transport = TSocket.TSocket('localhost', 9099)
  # Buffering is critical. Raw sockets are very slow
	transport = TTransport.TBufferedTransport(transport)
  # Wrap in a protocol
	#protocol = TBinaryProtocol.TBinaryProtocol(transport)
	protocol = TCompactProtocol.TCompactProtocol(transport)
  # Create a client to use the protocol encoder
	client = DocServlet.Client(protocol)
  # Connect!
	transport.open()

	doc_list = []
	fp = open('./result/infer.txt','r')
	for line in fp:
		doc = Doc_Info('1',line.strip() )
		doc_list.append(doc);
	fp.close()
	topics =   client.Infer(doc_list,15,10,15)
	fw = open('result.txt','w')
	for topic in topics:
		#print tag
		fw.write(str(topic.topicid)+':\t');fw.write(topic.topicwords+'\n')
		for one in topic.doclist:
			fw.write(one.docid+'\t'+str(one.consinesim)+'\t'+one.text+'\n' )
		fw.write('===================================\n')
	fw.close()

	transport.close()
except Thrift.TException, tx:
	print '%s' % (tx.message)
