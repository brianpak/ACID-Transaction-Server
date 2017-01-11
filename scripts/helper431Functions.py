#!/usr/bin/python

import socket,re, time, commands, os, signal
HOST = "localhost"

nl = "\r\n"

def getMsg(msg):
	r = re.search(r"(ACK|ack|Ack) .+ .+",msg)
	if r:
		return r.group(0)
	r = re.search(r"(ACK_RESEND|ASK_RESEND|ask_resend|Ask_resend|Ask_Resend) .+ .+ .+ .+",msg)
	if r:
		return r.group(0)
	r = re.search(r"(ERROR|error|Error)[\W]+.+[\W]+.+[\W]+.+[\W]+.+[\W]+.*",msg)
	if r:
		return r.group(0)
	return False


class Client:
	def __init__(self, port, display = False):
		self.port = port
		self.display = display

	def connect(self):
		client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		attempts = 0
		while attempts < 10:
			try:
				time.sleep(0.1)
				client_socket.connect((HOST,self.port))
				return client_socket
			except:
				attempts = attempts + 1
		print HOST,self.port
		client_socket.connect((HOST,self.port))
		return client_socket

	def abort(self, txn):
		sock = self.connect()
		msg = " ".join(map(str,["ABORT",txn,0,"0"+nl*3]))
		sock.send(msg+"\n")
		return self.getACKreply(sock)

	def new_txn(self,fileName):
		sock = self.connect()
		sock.send("NEW_TXN -1 0 " + str(len(fileName)) + nl*2 + fileName+"\n")
		fields = self.getACKreply(sock)
		if self.display:
			print "File", fileName, "new transaction:", fields[1]
		return fields[1]

	def read(self, fileName):
		sock = self.connect()
		sock.send("READ 1 1 " + str(len(fileName)) + nl*2 + fileName+"\n")
		data = sock.recv(4996)
		# fields = self.getACKreply(sock)
		print "File", fileName, "content:\n", data
		return data

	def write(self,txn,seq,data):
		sock = self.connect()
		msg = " ".join(map(str,["WRITE",txn,seq,str(len(data))+nl*2+data+"\n"]))
		sock.send(msg)
		return self.getACKreply(sock)
	
	def writeAck(self, txn, seq, data):
		sock = self.connect()
		msg = " ".join(map(str,["WRITE",txn,seq,str(len(data))+nl*2+data+"\n"]))
		sock.send(msg)
		fields = self.getACKreply(sock)
		return fields

	def getACKreply(self,sock):
		reply = ""
		errorCount = 0
		while errorCount < 5:
			time.sleep(0.1)
			try:
				data = sock.recv(512)
				reply = reply + data
				if self.display:
					print "ack reply attempt:",errorCount
					cpy = reply
					if len(cpy.strip()) > 0:
						print cpy.strip()
				msgReply = getMsg(reply)
				if msgReply:
					#print msgReply
					fields = msgReply.split(" ")
				return fields
			except:
				errorCount = errorCount + 1
		return ["ERROR","connection reset by peer"]

	def commit(self,txn,seq):
		sock = self.connect()
		msg = " ".join(map(str,["COMMIT",txn,seq,"0"+nl*3]))
		if self.display:
			print "Sending Commit"
			print msg
		sock.send(msg)
		return self.getACKreply(sock)


def cleanupProcesses(port = 12345):    
	psRet = commands.getoutput("ps")
	psLines = psRet.split("\n")
	psLines = psLines[1:]		# remove header
	for pLine in psLines:
		words = pLine.split()
		[pid, tty, time, cmd] = words[0:4]
		if cmd == "java" or cmd == "a.out" or cmd =="sh" or cmd == "node" or cmd[-3:] == ".pl" or cmd == "/usr/bin/java":
			try:
				#print "killing", pid
				os.kill(int(pid), signal.SIGTERM)
			except:
				pass
	bindedRet = commands.getoutput("lsof -i:" + str(port))
	


