//include "doc.thrift"

struct Doc_Info
{
1: string docid = ''
2: string text
3: double consinesim = 0.0
}

typedef list<Doc_Info> Docs

struct Topic_Info
{
1: i32 topicid = -1
2: string topicwords = ''
3: Docs doclist = []
}

typedef list<Topic_Info> Topics

service DocServlet 
{
string Ping(1: string sth);
Topics Infer(1: Docs docs, 2: i32 burn_in_iterations=15, 3: i32 accumulating_iterations=10, 4: i32 docnumoftopic=15);
Topics GetTopics(1: i32 docnumoftopic=0)
oneway void Sender2(1: Docs docs);
}
