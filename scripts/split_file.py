
import sys
from subprocess import call, Popen
import threading

def splitFile(n, fileName, hdfsPath):
    head = n * 134217728 # get the first x MB of the file
    tail = (n - 1) * 134217728 # then get the last x MB of the first x MB of the file

    partFileName = fileName + ".a" + str(n)
    if n == 13: # the last part
        #f = open(partFileName, "wb")
        #call(["tail", "-c", "+" + str(tail) + "M", fileName], stdout = f)
        #f.close()
        #call(["hdfs", "dfs", "-put", partFileName, hdfsPath + "/" + partFileName])
        cmd = "tail -c +" + str(tail + 1) + " " + fileName + " | hdfs dfs -put - " + hdfsPath + "/" + partFileName
        Popen(cmd, shell = True).wait()
    elif n > 1 and n < 13:
        #cmd = "head -c " + str(head) + "M " + fileName + " | tail -c 128M - > " + fileName + ".a" + str(n)
        cmd = "head -c " + str(head) + " " + fileName + " | tail -c 134217728 - | hdfs dfs -put - " + hdfsPath + "/" + partFileName
        Popen(cmd, shell = True).wait()
        #call(["head", "-c", str(head) + "M", fileName, "|", "tail", "-c", "128M", "-"], stdout = f)
    else:
        #f = open(partFileName, "wb")
        #call(["head", "-c", str(head) + "M", fileName], stdout = f)
        #f.close()
        #call(["hdfs", "dfs", "-put", partFileName, hdfsPath + "/" + partFileName])
        cmd = "head -c " + str(head) + " " + fileName + " | hdfs dfs -put - " + hdfsPath + "/" + partFileName
        Popen(cmd, shell = True).wait()

threads = []

def main():
    fileName = sys.argv[1]
    hdfsPath = sys.argv[2]
    for n in range(13):
        thread = threading.Thread(target=splitFile, args = (n + 1, fileName, hdfsPath))
        thread.start()

        threads.append(thread)

    # to wait until all three functions are finished

    print "Waiting..."

    for thread in threads:
        thread.join()

    print "Complete."

if __name__ == "__main__":
    main()

