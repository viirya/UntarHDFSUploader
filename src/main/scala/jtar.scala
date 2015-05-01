
import java.io._
import java.util._
import java.net._
import java.util.zip.GZIPInputStream

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.hadoop.fs.{FileSystem => HDFSFileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

import org.kamranzafar.jtar.{TarInputStream, TarEntry}

class readHandler(dataQueue: ConcurrentLinkedQueue[Array[Byte]], tis: TarInputStream) extends Runnable {
  val BUFFSIZE: Int = 100 * 1024 * 1024

  def run() {
    var entry: TarEntry = tis.getNextEntry()

    if (entry != null) {
       var count: Int = 0
       val data: Array[Byte] = new Array[Byte](BUFFSIZE)
       count = tis.read(data, 0, BUFFSIZE)
       while (count != -1) {
          dataQueue.add(data.slice(0, count))

          // Also output to console
          print(new String(data))

          count = tis.read(data, 0, BUFFSIZE)
       }
    }
    tis.close()
  }
}
 
class writeHandler(
    dataQueue: ConcurrentLinkedQueue[Array[Byte]],
    dest: FSDataOutputStream) extends Runnable {
  def run() {
    while (true) {
      val data = dataQueue.poll()
      if (data != null) {
        dest.write(data)
      }
      if (Thread.interrupted()) {
        dest.flush()
        dest.close()
        return
      }
    }
  }
}
 
object UntarHDFSUploader {

  val BUFFSIZE: Int = 8 * 1024 * 1024

  val dataQueue = new ConcurrentLinkedQueue[Array[Byte]]()

  def unTar(tarFile: String): TarInputStream = {
    val tis: TarInputStream =
      if (tarFile == "-") {
        new TarInputStream(new BufferedInputStream(System.in))
      } else if (tarFile.endsWith(".gz")) {
        new TarInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(tarFile))))
      } else {
        new TarInputStream(new BufferedInputStream(new FileInputStream(tarFile)))
      }
    return tis
  }
 
  def setUpHDFSDest(user: String, uri: String, filePath: String): FSDataOutputStream = {
    System.setProperty("HADOOP_USER_NAME", user)

    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = HDFSFileSystem.get(conf)
    return fs.create(path)
  }
 

  def readAndWrite(tis: TarInputStream, dest: FSDataOutputStream): Unit = {
    var entry: TarEntry = tis.getNextEntry()

    if (entry != null) {
       var count: Int = 0
       val data: Array[Byte] = new Array[Byte](BUFFSIZE)
       count = tis.read(data, 0, BUFFSIZE)
       while (count != -1) {
          dest.write(data, 0, count)

          // Also output to console
          print(new String(data))

          count = tis.read(data, 0, BUFFSIZE)
       }
    
       dest.flush()
       dest.close()
    }
    
    tis.close()
  }

  def keepWrite(dataQueue: ConcurrentLinkedQueue[Array[Byte]], dest: FSDataOutputStream, rWorker: Thread) = { 
    while (rWorker.getState() != Thread.State.TERMINATED) {
      val data = dataQueue.poll()
      if (data != null) {
        dest.write(data)
      }
    }
  }
 
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("Usage: UntarHDFSUploader <tar.gz File> <HDFS URI> <Hadoop username> <HDFS Destination>")
      sys.exit(0)
    } 

    val tarFile = args(0)
    val hdfsURI = args(1)
    val hadoopUser = args(2)
    val hdfsDest = args(3)

    val inStream = unTar(tarFile)
    val outStream = setUpHDFSDest(hadoopUser, hdfsURI, hdfsDest)
    readAndWrite(inStream, outStream)    
    /*
    val rWorker = new Thread(new readHandler(dataQueue, inStream))
    val wWorker = new Thread(new writeHandler(dataQueue, outStream))

    rWorker.start()
    wWorker.start()
    rWorker.join()
    while(dataQueue.size != 0) { }
    wWorker.interrupt()
    */
  }
}

