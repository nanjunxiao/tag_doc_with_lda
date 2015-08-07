tag doc using topN words with lda
---
利用LDA的历史训练，为新文章自动打Tag的thrift服务。

该方法通过infer得到文档的theta分布，找到该文档概率最大主题编号，然后根据phi分布获取主题编号的topN词，作为文章标签输出。

collapsed Gibbs LDA reference : [my blog](http://nanjunxiao.github.io/2015/08/07/Topic-Model-LDA%E7%90%86%E8%AE%BA%E7%AF%87/ )






 


