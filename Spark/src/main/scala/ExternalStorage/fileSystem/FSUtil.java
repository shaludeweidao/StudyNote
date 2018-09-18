package ExternalStorage.fileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.net.URISyntaxException;

/**
 * 获取hdfs的文件对象工具类
 * @author lyd
 *
 */
public class FSUtil implements Serializable{
    static FileSystem fs = null;

    /**
     * 初始化fs工具类
     * @param spark
     */
    public  static void initFSUtil(SparkSession spark){
        try {
            Configuration conf = spark.sparkContext().hadoopConfiguration();
            conf.setInt("io.file.buffer.size", 8192);
//			fs = FileSystem.newInstance(conf);
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            //do noting
            System.out.println("initFSUtil is error");
        }
    }


	/**
	 * 获取FileSystem对象
	 * @return
	 */
	public static FileSystem getFS(){
		FileSystem fs = null;
		try {
            Configuration conf = new Configuration();
			conf.setInt("io.file.buffer.size", 8192);
            //conf.set("fs.defaultFS", "hdfs://hadoop01:9000/");

            //几种不同的获取fs的api
            //FileSystem fs = FileSystem.get(conf);
            //FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000/"), conf, "root");
            //FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000/"), conf);
//			fs = FileSystem.newInstance(conf);
			fs = FileSystem.get(conf);
		} catch (Exception e) {
			//do noting
		}
		return fs;
	}
	
	/**
	 * 获取LocalFileSystem对象
	 * @return
	 */
	public static LocalFileSystem getLFS(){
		LocalFileSystem lfs = null;
		try {
			Configuration conf = new Configuration();
			lfs = FileSystem.newInstanceLocal(conf);
		} catch (Exception e) {
			//do noting
		}
		return lfs;
	}
	
	/**
	 * 关闭文件对象
	 * @param fs
	 */
	public static void closeFS(FileSystem fs){
		if(fs != null){
			try {
				fs.close();
			} catch (Exception e) {
				// do nothing
			}
		}
	}





    /**
     * 读取hdfs的文件写到console
     * @param fileName
     * @throws IOException
     */
    public static void catFileToLocal(String fileName) throws IOException{
        //1、获取Configuration
        Configuration conf = new Configuration();
        //2、配置conf
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000/");
        //3、获取hdfs的fs文件对象
        FileSystem fs = FileSystem.get(conf);
        //4、具体的对文件系统的操作
        FSDataInputStream fis = fs.open(new Path(fileName));
        OutputStream os = new FileOutputStream(new File("E:\\hadoopdata\\123.txt"));
        IOUtils.copyBytes(fis, os, 4096, true);
        //5、关闭流操作
        fs.close();
    }

    /**
     * 创建目录
     * @param dirName
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public static void createMKdir(String dirName) throws IOException, InterruptedException, URISyntaxException{
        //1、获取Configuration
        Configuration conf = new Configuration();
        //2、配置conf
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000/");
        //3、获取hdfs的fs文件对象
        FileSystem fs = getFS();//FileSystem.get(conf);
        //创建目录
        boolean isok = fs.mkdirs(new Path(dirName));
        System.out.println("finished..."+isok);

    }



    /**
     * 列出hdfs中的文件列表
     *
     * 递归列出文件文件列表？？？
     * @param dirName
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    public static void listFile(String dirName) throws FileNotFoundException, IllegalArgumentException, IOException {
        //FileSystem fs = FSUtil.getFS();
        LocalFileSystem lfs = FSUtil.getLFS();
        //列出状态列表
        FileStatus[] fss = lfs.listStatus(new Path(dirName));
        for (FileStatus f : fss) {
            System.out.print("文件路径:"+f.getPath().toString()+"   ");
            System.out.print("文件名:"+f.getPath().getName()+"   ");
            System.out.print("文件大小:"+f.getLen()/1024.0 +"kb"+"   ");
            System.out.print("文件用户:"+f.getOwner()+"   ");
            System.out.println("文件的权限:"+f.getPermission());
        }
    }

    /**
     * 获取集群磁盘情况
     * @throws IOException
     */
    public static void getDiskSource() throws IOException{
//		FileSystem fs = FSUtil.getFS();
        FsStatus fsstatus = fs.getStatus();
        System.out.println("总容量："+fsstatus.getCapacity()/1024/1024/1024.0+"GB");
        System.out.println("已经使用的容量："+fsstatus.getUsed()/1024/1024.0+"MB");
        System.out.println("维持容量："+fsstatus.getRemaining()/1024/1024/1024.0+"GB");
    }

    /**
     * 获取DataNode信息
     * @throws IOException
     */
    public static void getDataNodeInfo() throws IOException{
//		 FileSystem fs = FSUtil.getFS();
        //强转为分布式文件系统
        DistributedFileSystem dis = (DistributedFileSystem) fs;
        DatanodeInfo[] dinfo = dis.getDataNodeStats();
        for (DatanodeInfo info : dinfo) {
            System.out.print(info.getHostName()+"   ");
            System.out.print(info.getName()+"   ");
            System.out.println(info.getCapacity());
        }
    }

    /**
     * 查看块位置信息
     * @param fileName
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void getBlockLocation(String fileName) throws IllegalArgumentException, IOException{
//		FileSystem fs = FSUtil.getFS();
        //获取文件状态
        FileStatus fss = fs.getFileStatus(new Path(fileName));
        //获取块位置信息
        BlockLocation[] bls = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation bl : bls) {
            for (int i = 0; i < bl.getHosts().length; i++) {
                System.out.print(bl.getHosts()[i] +"   ");
                System.out.print(bl.getTopologyPaths()[i] +"   ");
                System.out.println(bl.getNames()[i]);
            }
        }

    }

    /**
     * 待进度的文件上传
     * @param fileName
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static void uploadWithProcess(String fileName) throws IllegalArgumentException, IOException{
//		FileSystem fs = FSUtil.getFS();
        FileInputStream fis = new FileInputStream(new File("D:\\software\\apache-maven-3.3.9-bin.zip"));
        FSDataOutputStream fos = fs.create(new Path(fileName), new Progressable() {
            public void progress() {
                try {
                    System.out.print("* ");
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    //
                }
            }
        });
        IOUtils.copyBytes(fis, fos, 4096, true);
        System.out.println("finished...");
    }

    /**
     * 使用seek对hdfs中的文件内容进行截取
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void seek() throws IllegalArgumentException, IOException{
//		FileSystem fs = FSUtil.getFS();
        FSDataInputStream fis = fs.open(new Path("/test/seek"));
        fis.seek(6);
        IOUtils.copyBytes(fis, System.out, 4096, true);
    }

    /**
     * 将本地文件copy到hdfs中(上传目录和文件均可以)
     * @param localsrc
     * @param destination
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static void copyFromLocal(String localsrc, String destination) throws IllegalArgumentException, IOException{
        fs.copyFromLocalFile(new Path(localsrc), new Path(destination));
    }


    /**
     * 将本地文件copy到hdfs中(上传不同目录下的多个文件)
     * @param localsrc
     * @param destination
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void copyFromLocalOfMulti(Path[] localsrc, String destination) throws IllegalArgumentException, IOException{
        fs.copyFromLocalFile(false, false, localsrc , new Path(destination));
    }

    /**
     * 下载到本地(下载多个不同目录的文件？？)
     * @param src
     * @param destination
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void copyToLocal(String src,String destination) throws IllegalArgumentException, IOException{
        fs.copyToLocalFile(new Path(src), new Path(destination));
    }

    //moveFromLocal  MoveToLocal


    /**
     * 存在、删除
     * @param fileName
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static void delete(String fileName) throws IllegalArgumentException, IOException{
        //判断文件是否存在
        //如果存在，在看是目录还是是文件
        Path fileOrDir = new Path(fileName);
        if(fs.exists(fileOrDir)){
            if(fs.isDirectory(fileOrDir)){
                //递归删除
                fs.delete(fileOrDir, true);
            } else {
                fs.deleteOnExit(fileOrDir);
            }
        } else {
            System.out.println("文件或者目录不存在");
        }
    }

    /**
     * 和并本地小文件并上传hdfs
     * @param fileName
     */
    public static void uploadWithMerge(String fileName){
        LocalFileSystem lfs = null;
        FileSystem fs = null;
        try {
            lfs = FSUtil.getLFS();
//			fs = FSUtil.getFS();
            FileStatus[] fss = lfs.listStatus(new Path(fileName));
            FSDataOutputStream fos = fs.create(new Path("/test/merge"));
            FileInputStream fis = null;
            for (FileStatus f : fss) {
                System.out.println(f.getPath().toString().replace("file:/", ""));
                //获取本地文件列表的路径
                String localFilePath = f.getPath().toString().replace("file:/", "");
                //获取本地文件的数据流
                fis = new FileInputStream(new File(localFilePath));
                byte [] bytes = new byte[1024];
                int length = 0;
                while ((length = fis.read(bytes)) != -1) {
                    fos.write(bytes, 0, length);
                }
            }
            //关闭输出流
            fos.close();
            fis.close();
            System.out.println("finished...");
        } catch (Exception e) {
            //nothing
        }
    }




    /**
     * 重命名或者移动
     * @param src
     * @param destination
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void rename(String src, String destination) throws IllegalArgumentException, IOException{
        fs.rename(new Path(src), new Path(destination));
    }


    /**
     * 创建并写入
     * @param fileName
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void touchzWithWrite(String fileName) throws IllegalArgumentException, IOException{
        FSDataOutputStream fos = fs.create(new Path(fileName),true);
        //写如数据
        //fos.writeBytes("hello world,你好!");
//		fos.write(97);
        fos.writeByte(65);
        fos.close();
    }


    /**
     * 追加
     * @param fileName
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public static void append(String fileName) throws IllegalArgumentException, IOException{
        FSDataOutputStream fos = fs.append(new Path(fileName), 4096);
        InputStream is = new FileInputStream(new File("E:\\hadoopdata\\1.txt"));
        IOUtils.copyBytes(is, fos, 4096, true);

    }

}
