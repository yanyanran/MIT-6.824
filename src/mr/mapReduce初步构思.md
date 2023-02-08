
master负责:  
启动时根据测试文件和nreduce生成map任务和reduce任务；
响应worker任务的rpc请求，然后分配任务给worker；
超过10s没有接收到worker完成的通知，判定此worker已寄，重新把此任务发给其他worker（记得原worker要处理，防止两个worker同时写一个文件导致冲突）；
（map任务全部执行完成才开始分发reduce任务）所以master还需追踪一类任务的完成情况；（初步思考维护一个队列）

worker负责：
空闲的时候通过rpc向master申请任务并执行；

rpc通信过程：
worker空闲时向master发送请求，master给它响应然后分配一个任务；
worker完成任务后通知给master
