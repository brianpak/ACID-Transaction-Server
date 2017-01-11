#!/usr/bin/python

from scripts.helper431Functions import *
import commands, sys, os, time, signal
from multiprocessing import Pool
#for edit distance checking
import Levenshtein

ERRORS = {"No Error": 0, "Invalid transaction ID": 201, "Invalid operation": 202, "File I/O error" : 205, "File not found" :206}

#set up environment
userID = sys.argv[1]
userID.strip("/")
startSequence = 1
commitOffset = 0
ipaddress = "127.0.0.1"
if len(sys.argv) > 2:
	ipaddress = int(sys.argv[2])
os.chdir(userID)

port = 12345
if len(sys.argv) > 3:
	port = int(sys.argv[3])


def testContents(fileName, contents, display=False):
	try:
		f = open(fileName,'r')
		print "found file"
	except:
		try:
			print "file not not found in specified dir"
			fileParts = fileName.split("/")
			f = open(fileParts[1])
			print "file found in current path"
		except:
			print "file not found in current path"
			return False
	fileContents = f.read()
	d = Levenshtein.distance(fileContents, contents)
	if d == 0:
		if display:
			print "files contents match"
		return True
	else:
		if display:
			print "files differ by Levenshtein distance", d
			print "expect:"
			print contents
			print "server:"
			print fileContents
			print "-----------"
		return False

def launchServer(port,clean=True):
	if clean:
		print "clean up old server"
		ret = commands.getoutput("sh cleanup.sh")
	print "starting server"
	ret = commands.getoutput("sh run.sh " + ipaddress + " " + str(port) + " ./tmp/" + " > testingLog.log")
	#print "error starting server!!!!!!!!!!!!!!!!!!!!!!!!!!!"

def commonCaseTest(startSequence=1,commitOffset=0):
	print 
	print "****** Common Case Test *********"
	client = Client(port)
	fileName = "common.txt"
	txn = client.new_txn(fileName)
	print "Starting transaction with id", txn
	msg1 = "this is a test message"
	print "Writing message with seq#", startSequence
	client.write(txn, startSequence, msg1)
	ret = client.commit(txn, 1+commitOffset)
	if ret[0] == "ACK" or ret[0] == "ack":
		print "Recieved ACK"
		print "Testing file contents"
		if testContents("tmp/"+fileName,msg1,True):
			print "PASS\n"
		else:
			print "FAIL\n"
	else:
		print "FAIL\n"


	print "Writing same commited transaction with id", txn
	print "Writing message with seq#", startSequence
	ret=client.write(txn, startSequence, msg1)
	checker(ret, ["ERROR", "error"], ERRORS["Invalid operation"])

	txn=-2
	startSequence=1
	print "Writing to unknown transaction with id", txn
	print "Writing message with seq#", startSequence
	ret=client.write(txn, startSequence, msg1)
	checker(ret, ["ERROR", "error"], ERRORS["Invalid transaction ID"])

	txn = client.new_txn(fileName)
	startSequence=0
	print "Writing to transaction with id", txn
	print "Writing message with seq#", startSequence
	ret=client.write(txn, startSequence, msg1)
	checker(ret, ["ERROR", "error"], ERRORS["Invalid operation"])


	txn=-2
	startSequence=1
	print "Commiting unknown txn", txn
	print "Writing message with seq#", startSequence
	ret = client.commit(txn, 1+startSequence)
	checker(ret, ["ERROR", "error"], ERRORS["Invalid transaction ID"])

	txn = client.new_txn(fileName)
	startSequence=0
	print "Commiting new txn", txn
	print "Commiting message with seq#", startSequence
	ret = client.commit(txn, startSequence)
	checker(ret, ["ACK", "ack"], ERRORS["No Error"])

	txn = client.new_txn(fileName)
	startSequence=1
	print "Commiting new txn", txn
	print "Writing message with seq#", startSequence
	client.write(txn, startSequence, msg1)
	ret = client.commit(txn, startSequence)
	checker(ret, ["ACK", "ack"], ERRORS["No Error"])


	print "Reading from file", fileName
	fileContent = client.read(fileName)
	if fileContent==msg1*2:
		print "PASS\n"
	else:
		print "FAIL"
		print "File not found\n"



def checker(ret, method, code):
	if ret[0] == method[0] or ret[0] == method[1]:
		if int(ret[3])==code:
			print "PASS\n"
		else:
			print "FAIL"
			print "Analysis:", ret, '\n'
	else:
		print "FAIL"
		print "Analysis:", ret, '\n'


def abortedTransactionsTest(startSequence=1,commitOffset=0):
	print
	print "****** abortedTransactions *********"
	client = Client(port)
	fileName = "aborted1.txt"
	txn = client.new_txn(fileName)
	print "Starting transaction with id", txn
	msg1 = "this is a test message"
	print "Writing message with seq#", startSequence
	client.write(txn, startSequence, msg1)
	print "Sending Abort Message for txn",txn
	client.abort(txn)
	print "Checking if file was created"
	time.sleep(0.1)
	exists = os.path.isfile("tmp/"+fileName)
	print "File Exist:", exists

	print "Writing same aborted transaction with id", txn
	print "Writing message with seq#", startSequence
	ret=client.write(txn, startSequence, msg1)
	checker(ret, ["ERROR", "error"], ERRORS["Invalid transaction ID"])

	print

	fileName = "aborted2.txt"
	print "Creating empty file '", fileName, "' on server"
	tmpFile = open("tmp/" + fileName,'w')
	tmpFile.write('')
	tmpFile.close()
	print "Checking file status"
	print os.path.isfile("tmp/" + fileName)

	txn = client.new_txn(fileName)
	print "Starting transaction with id", txn, "for file", fileName
	print "Writing message with seq#", startSequence
	client.write(txn, startSequence, msg1)
	print "Sending Abort Message for txn",txn
	client.abort(txn)
	print "Checking if file was modified"
	tmpFile = open("tmp/" + fileName,'r')
	fileContents = tmpFile.read()
	modified = False
	if len(fileContents) > 0:
		modified = True
	print "File modified:", modified
	print
	if exists or modified:
		print "FAIL"
	else:
		print "PASS"

	txn = client.new_txn(fileName)
	print "Starting transaction with id", txn, "for file", fileName
	print "Writing message with seq#", startSequence
	client.write(txn, startSequence, msg1)
	print "Comitting txn",txn
	client.commit(txn, 1+commitOffset)
	print "Sending Abort Message for txn",txn
	ret = client.abort(txn)
	checker(ret, ["ERROR", "error"], ERRORS["Invalid operation"])
	
def omissionFailureTest(startSequence=1,commitOffset=0):
	print
	print "****** omission failures *********"
	client = Client(port,True)
	fileName = "ommision.txt"
	txn = client.new_txn(fileName)
	print "Starting transaction with id", txn, "for file", fileName
	msg1 = "this is a test message"
	print "Writing message with seq#", startSequence
	client.write(txn, startSequence, msg1)
	client.write(txn, startSequence+1, msg1)
	client.write(txn, startSequence+5, msg1)
	client.write(txn, startSequence+10, msg1)
	client.write(txn, startSequence+7, msg1)
	ret=client.commit(txn, startSequence+11)

	checker(ret, ["ASK_RESEND", "ask_resend"], ERRORS["No Error"])

	client.write(txn, startSequence+2, msg1)
	client.write(txn, startSequence+3, msg1)
	client.write(txn, startSequence+4, msg1)
	client.write(txn, startSequence+6, msg1)
	client.write(txn, startSequence+8, msg1)
	client.write(txn, startSequence+9, msg1)
	ret=client.write(txn, startSequence+100, msg1)

	checker(ret, ["ACK", "ack"], ERRORS["No Error"])


def concurrencyOutput(sentences):
	sent_parts=sentences.split(' ')
	client = Client(port)
	fileName = "concurrency.txt"
	txn = client.new_txn(fileName)
	print "Starting transaction with id", txn, "for file", fileName
	startSequence=1
	for sent in sent_parts:
		print "Writing message with seq#", startSequence
		client.write(txn, startSequence, sent)
		startSequence+=1
	startSequence-=1
	print "Commiting txn", txn
	ret = client.commit(txn, startSequence)
	checker(ret, ["ACK", "ack"], ERRORS["No Error"])
	return


def concurrencyFailureTest(startSequence=1,commitOffset=0):
	print
	print "****** concurrency failures *********"

	text1="""As Barry works on improving his speed through various training exercises, Leonard Snart returns to Central City with the cold gun and a new partner, Mick Rory, to set a trap for the Flash. Barry discovers his plan, and agrees with Wells not to engage Snart in the hope that he goes away and no one gets hurt like the last time. Snart and Rory, who now has a gun that can emit absolute hot temperatures, kidnap Caitlin to force Barry out of hiding. Cisco and Barry find a way to defeat Snart and Rory. The Flash faces the duo in the city for a showdown, exposing himself to the public. Barry eventually gets them to cross their streams with Eddie's help, successfully damaging the weapons and disabling the pair, who are arrested and the guns delivered to S.T.A.R. Labs. While in transport to Iron Heights, Snart and Rory are broken out by Snart's sister. Meanwhile, Caitlin investigates the cause of Ronnie's transformation and finds out that the Army is covering up the incident. After Iris moves in with Eddie, Barry decides to move back in with Joe."""
	text2="""Thawne warns Barry not to change any event for fear that he will create a bigger problem. Barry does not listen and instead captures Mardon and puts him in the particle accelerator. Snart and Rory return to Central City. Snart sends his sister, Lisa after Cisco, whom they force to rebuild the cold and heat guns, and a third gun shooting gold for Lisa, by threatening to kill Cisco's brother. Iris rejects Barry's romantic approach and later Eddie punches him. Barry realizes that Thawne was right about not to manipulate the timeline. Cisco returns and reveals that Snart forced him to reveal the Flash's true identity. Barry goes after Snart, and the two come to a truce: Snart will not reveal Barry's identity, will no longer kill innocent people, and stay away from Barry's loved ones in exchange for Barry not locking him away in the particle accelerator. Caitlin tells Eddie and Iris that Barry is suffering from psychosis as a result of the lightning, making the duo reconcile with him and settling the tension. The Reverse-Flash kills Mason and destroys the evidence linking Thawne to Stagg's death. Mason's vanishing causes Barry to accept Joe was right about "Wells"."""
	output=[text1, text2]

	p = Pool(2)
	p.map(concurrencyOutput, output)



def serialFailureTest(startSequence=1,commitOffset=0):
	print
	print "****** serial failures *********"

	text1="""As Barry works on improving his speed through various training exercises, Leonard Snart returns to Central City with the cold gun and a new partner, Mick Rory, to set a trap for the Flash. Barry discovers his plan, and agrees with Wells not to engage Snart in the hope that he goes away and no one gets hurt like the last time. Snart and Rory, who now has a gun that can emit absolute hot temperatures, kidnap Caitlin to force Barry out of hiding. Cisco and Barry find a way to defeat Snart and Rory. The Flash faces the duo in the city for a showdown, exposing himself to the public. Barry eventually gets them to cross their streams with Eddie's help, successfully damaging the weapons and disabling the pair, who are arrested and the guns delivered to S.T.A.R. Labs. While in transport to Iron Heights, Snart and Rory are broken out by Snart's sister. Meanwhile, Caitlin investigates the cause of Ronnie's transformation and finds out that the Army is covering up the incident. After Iris moves in with Eddie, Barry decides to move back in with Joe."""
	text2="""Thawne warns Barry not to change any event for fear that he will create a bigger problem. Barry does not listen and instead captures Mardon and puts him in the particle accelerator. Snart and Rory return to Central City. Snart sends his sister, Lisa after Cisco, whom they force to rebuild the cold and heat guns, and a third gun shooting gold for Lisa, by threatening to kill Cisco's brother. Iris rejects Barry's romantic approach and later Eddie punches him. Barry realizes that Thawne was right about not to manipulate the timeline. Cisco returns and reveals that Snart forced him to reveal the Flash's true identity. Barry goes after Snart, and the two come to a truce: Snart will not reveal Barry's identity, will no longer kill innocent people, and stay away from Barry's loved ones in exchange for Barry not locking him away in the particle accelerator. Caitlin tells Eddie and Iris that Barry is suffering from psychosis as a result of the lightning, making the duo reconcile with him and settling the tension. The Reverse-Flash kills Mason and destroys the evidence linking Thawne to Stagg's death. Mason's vanishing causes Barry to accept Joe was right about "Wells"."""
	output=[''.join([text1, text2])]
	for sentences in output:
		sent_parts=sentences.split(' ')
		client = Client(port)
		fileName = "concurrency.txt"
		txn = client.new_txn(fileName)
		print "Starting transaction with id", txn, "for file", fileName
		startSequence=1
		for sent in sent_parts:
			print "Writing message with seq#", startSequence
			client.write(txn, startSequence, sent)
			startSequence+=1
		startSequence-=1
		print "Commiting txn", txn
		ret = client.commit(txn, startSequence)
		checker(ret, ["ACK", "ack"], ERRORS["No Error"])
	return


def basicTests(startSequence=1,commitOffset=0):
	print "testing with startSequence,", startSequence, "and commit offset:", commitOffset
	commonCaseTest(startSequence, commitOffset)
	abortedTransactionsTest(startSequence, commitOffset)
	omissionFailureTest(startSequence, commitOffset)
	#concurrencyFailureTest(startSequence, commitOffset)
	#serialFailureTest(startSequence, commitOffset)
	time.sleep(5)

# Fork server and Run Tests

cleanupProcesses(port)
child_pid = os.fork()
if child_pid == 0:
	print "Forking server with pid", os.getpid()
	launchServer(port, True)
	sys.exit(0)
else:
	time.sleep(0.5)
	basicTests(startSequence, commitOffset)
	cleanupProcesses(port)
	










