/**
 * Project Name:ftpManager
 * File Name:ftpManager.java
 * Package Name:ftpManager
 * Date:2014年7月11日上午12:21:16
 * Copyright (c) 2014, jingma@iflytek.com All Rights Reserved.
 *
 */

package com.metl.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * ClassName:FtpManager <br/>
 * Function: ftp管理工具<br/>
 * Date: 2014年7月11日 上午12:21:16 <br/>
 * 
 * @author jingma
 * @see
 */
public class FtpUtil {
    public static final String ISO_8859_1 = "ISO-8859-1";
    /**
     * 文件分隔符-反斜杠
     */
    public static final String FILE_SEPARATOR = "/";
    /**
     * 异常描述
     */
    private static final String EXCEPTION = "ftp处理中的异常";
    /**
     * 日志工具
     */
    private static Logger logger = Logger.getLogger(FtpUtil.class);

    /**
     * ftp客服端实例
     */
    private FTPClient ftpClient;

    /**
     * 默认ftp操作对象
     */
    public static FtpUtil ftp;

    /**
     * ftp列表
     */
    private static Map<String, FtpUtil> ftpList = new HashMap<String, FtpUtil>();

    /**
     * 使用其他ftp <br/>
     * 
     * @author jingma@iflytek.com
     * @param name
     * @return
     * @throws IOException
     */
    public static FtpUtil use(String name) throws IOException {
        FtpUtil ftpUtil = ftpList.get(name);
        if (ftpUtil != null) {
            try {
                setPath(ftpUtil.ftpClient, FILE_SEPARATOR);
            } catch (Exception e) {
                ftpUtil.disConnection();
                ftpUtil = null;
            }
        }
        return ftpUtil;
    }

    /**
     * 构造函数
     * 
     * @param name
     *            名称
     * @param isPrintCommmand
     *            是否输出处理详情
     * @param confJson
     *            配置JSON串
     */
    public FtpUtil(String name, boolean isPrintCommmand,String confJson) {
        ftpClient = new FTPClient();
        // if (isPrintCommmand) {
        // ftp.addProtocolCommandListener(new PrintCommandListener(
        // new PrintWriter(System.out)));
        // }
        if (ftp == null) {
            ftp = this;
        }
        try {
            this.login(confJson);
        } catch (IOException e) {
            logger.error("ftp登录失败："+confJson, e);
        }
        ftpList.put(name, this);
    }

    /**
     * 通过配置JSON串登录 <br/>
     * 
     * @author jingma@iflytek.com
     * @param confJson
     *            配置JSON串
     * @return
     * @throws IOException
     */
    public boolean login(String confJson) throws IOException {
        JSONObject conf = JSON.parseObject(confJson);
        return login(conf.getString("address"), conf.getIntValue("port"),
                conf.getString("username"), conf.getString("password"),
                conf.getString("encoding"), null, null);
    }

    /**
     * 登录ftp服务器
     * 
     * @param host
     *            主机
     * @param port
     *            端口
     * @param username
     *            用户名
     * @param password
     *            密码
     * @param code
     *            编码
     * @param syst
     *            ftp所在系统
     * @param serverLanguageCode
     *            服务器语言代码
     * @return 是否登录成功
     * @throws IOException
     *             io异常
     */
    public boolean login(String host, int port, String username,
            String password, String code, String syst, String serverLanguageCode)
            throws IOException {
        if (StringUtil.isBlank(code)) {
            code = "utf8";
        }
        if (StringUtil.isBlank(syst)) {
            syst = "WINDOWS";
        }
        if (StringUtil.isBlank(serverLanguageCode)) {
            serverLanguageCode = "zh";
        }
        this.ftpClient.connect(host, port);
        if (FTPReply.isPositiveCompletion(this.ftpClient.getReplyCode())) {
            if (this.ftpClient.login(username, password)) {
                this.ftpClient.setControlEncoding(code);
                FTPClientConfig conf = new FTPClientConfig(syst);
                conf.setServerLanguageCode(serverLanguageCode);
                return true;
            }
        }
        if (this.ftpClient.isConnected()) {
            this.ftpClient.disconnect();
        }
        return false;
    }

    /**
     * 关闭连接
     * 
     * @throws IOException
     */
    public void disConnection() throws IOException {
        if (this.ftpClient.isConnected()) {
            this.ftpClient.disconnect();
        }
    }

    /**
     * 通过路径获得路径下所有文件 输出文件名
     * 
     * @param pathName
     *            路径
     * @throws IOException
     */
    public void printFileList(String pathName) throws IOException {
        String directory = pathName;
        setPath(ftpClient, directory);
        directory = repairDirEnd(directory);
        FTPFile[] files = this.ftpClient.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                System.out.println("得到文件:" + directory + files[i].getName());
            } else if (files[i].isDirectory()) {
                printFileList(directory + files[i].getName());
            }
        }
    }

    /**
     * Description: 从FTP服务器下载文件
     * 
     * @param path
     *            要下载的远程路径
     * @param localPath
     *            下载后保存到本地的路径
     * @param fileName
     *            文件名
     * @return
     */
    public boolean downloadFile(String path, String fileName, String localPath) {
        return downloadFile(ftpClient, path, fileName, localPath);
    }

    /**
     * Description: 从FTP服务器下载文件
     * 
     * @param ftp
     *            ftp工具实例
     * @param path
     *            要下载的远程路径
     * @param fileName
     *            文件名
     * @param localPath
     *            下载后保存到本地的路径
     * @return
     */
    public static boolean downloadFile(FTPClient ftp, String path,
            String fileName, String localPath) {
        boolean success = false;
        OutputStream is = null;
        try {
            File lo = new File(localPath);
            if (!lo.exists()) {
                lo.mkdirs();
            }
            setPath(ftp, path);
            localPath = repairDirEnd(localPath);
            File localFile = new File(localPath + fileName);
            is = new FileOutputStream(localFile);
            ftp.retrieveFile(zhFileName(fileName), is);
            success = true;
        } catch (IOException e) {
            logger.error(EXCEPTION, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();

                }
            }
        }
        return success;
    }

    /**
     * 编码路径为：ISO_8859_1 <br/>
     * 
     * @author jingma@iflytek.com
     * @param path
     * @return
     * @throws UnsupportedEncodingException
     */
    private static String zhFileName(String path)
            throws UnsupportedEncodingException {
        return new String(path.getBytes(), ISO_8859_1);
    }

    /**
     * Description: 从FTP服务器下载文件
     * 
     * @param ftp2
     * @param path
     *            要下载的远程路径
     * @param localPath
     *            下载后保存到本地的路径
     * @return
     */
    public boolean downloadFileList(String path, String localPath) {
        return downloadFileList(ftpClient, path, localPath);
    }

    /**
     * Description: 从FTP服务器下载文件
     * 
     * @param ftp
     *            ftp工具实例
     * @param path
     *            要下载的远程路径
     * @param localPath
     *            下载后保存到本地的路径
     * @return
     */
    public static boolean downloadFileList(FTPClient ftp, String path,
            String localPath) {
        boolean success = false;
        OutputStream is = null;
        try {
            File lo = new File(localPath);
            if (!lo.exists()) {
                lo.mkdirs();
            }
            setPath(ftp, path);
            localPath = repairDirEnd(localPath);
            path = repairDirEnd(path);
            FTPFile[] fs = ftp.listFiles();
            for (FTPFile ff : fs) {
                if (ff.isDirectory()) {
                    downloadFileList(ftp, path + ff.getName(),
                            localPath + ff.getName());
                } else {
                    File localFile = new File(localPath + ff.getName());
                    is = new FileOutputStream(localFile);
                    ftp.retrieveFile(zhFileName(ff.getName()), is);
                    is.close();
                }
            }
            success = true;
        } catch (IOException e) {
            logger.error(EXCEPTION, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();

                }
            }
        }
        return success;
    }

    public static String repairDirEnd(String path) {
        if (!path.endsWith(FILE_SEPARATOR)) {
            path += FILE_SEPARATOR;
        }
        return path;
    }

    /**
     * Description: 从FTP服务器下载文件
     * 
     * @Version1.0 Jul 27, 2008 5:32:36 PM by 崔红保（cuihongbao@d-heaven.com）创建
     * @param url
     *            FTP服务器hostname
     * @param port
     *            FTP服务器端口
     * @param username
     *            FTP登录账号
     * @param password
     *            FTP登录密码
     * @param remotePath
     *            FTP服务器上的相对路径
     * @param fileName
     *            要下载的文件名
     * @param localPath
     *            下载后保存到本地的路径
     * @return
     */
    public static boolean downloadFiles(String url, int port, String username,
            String password, String remotePath, String fileName,
            String localPath) {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;

            // 连接FTP服务器
            if (port > -1) {
                ftp.connect(url, port);
            } else {
                ftp.connect(url);
            }

            ftp.login(username, password);// 登录
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            return downloadFileList(ftp, remotePath, localPath);
        } catch (IOException e) {
            logger.error(EXCEPTION, e);
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException e) {
                    logger.error(EXCEPTION, e);
                }
            }
        }
        return success;
    }

    /**
     * 远程FTP上传文件
     * 
     * @param remotePath
     *            上传的目录
     * @param files
     *            要上传的文件列表
     * @throws Exception
     */
    public File uploadFile(String remotePath, List<File> files)
            throws Exception {
        File fileIn = null;
        FileInputStream is = null;
        try {
            for (File file : files) {
                logger.info("----进入文件上传到FTP服务器--->");
                setPath(ftpClient, remotePath);
                fileIn = file;
                is = new FileInputStream(fileIn);
                ftpClient.storeFile(zhFileName(file.getName()), is);
            }
        } catch (Exception e) {
            logger.error("上传FTP文件异常: ", e);
        } finally {
            is.close();
        }

        return fileIn;
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param path
     *            FTP服务器保存目录
     * @param localPath
     *            本地路径
     * @return 成功返回true，否则返回false
     */
    public boolean uploadFile(String path, String localPath) {
        return uploadFile(ftpClient, path, localPath);
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param path
     *            FTP服务器保存目录
     * @param filename
     *            上传到FTP服务器上的文件名
     * @param localPath
     *            本地路径
     * @return 成功返回true，否则返回false
     */
    public boolean uploadFile(String path, String filename, String localPath) {
        return uploadFile(ftpClient, path, filename, localPath);
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param ftp
     * @param path
     *            FTP服务器保存目录
     * @param localPath
     *            本地路径
     * @return 成功返回true，否则返回false
     */
    public static boolean uploadFile(FTPClient ftp, String path,
            String localPath) {
        File file = new File(localPath);
        if (file.isFile()) {
            throw new RuntimeException("没有上传文件名");
        } else if (file.isDirectory()) {
            makeDir(ftp, path);
            for (File f : file.listFiles()) {
                uploadFile(ftp, path, f.getName(), f.getAbsolutePath());
            }
            return true;
        }
        return false;
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param ftp
     * @param path
     *            FTP服务器保存目录
     * @param filename
     *            上传到FTP服务器上的文件名
     * @param localPath
     *            本地路径
     * @return 成功返回true，否则返回false
     */
    public static boolean uploadFile(FTPClient ftp, String path,
            String filename, String localPath) {
        try {
            File file = new File(localPath);
            if (file.isFile()) {
                return uploadFile(ftp, path, filename,
                        new FileInputStream(file));
            } else if (file.isDirectory()) {
                path = repairDirEnd(path);
                path = path + filename;
                makeDir(ftp, path);
                for (File f : file.listFiles()) {
                    uploadFile(ftp, path, f.getName(), f.getAbsolutePath());
                }
                return true;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param path
     *            FTP服务器保存目录
     * @param filename
     *            上传到FTP服务器上的文件名
     * @param input
     *            输入流
     * @return 成功返回true，否则返回false
     */
    public boolean uploadFile(String path, String filename, InputStream input) {
        return uploadFile(ftpClient, path, filename, input);
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param ftp
     * @param path
     *            FTP服务器保存目录
     * @param filename
     *            上传到FTP服务器上的文件名
     * @param input
     *            输入流
     * @return 成功返回true，否则返回false
     */
    public static boolean uploadFile(FTPClient ftp, String path,
            String filename, InputStream input) {
        boolean success = false;
        try {
            if(!existDir(ftp, path)){
                makeDir(ftp, path);
            }
            setPath(ftp, path);
            ftp.storeFile(zhFileName(filename), input);

            input.close();
            success = true;
        } catch (IOException e) {
            success = false;
            logger.error(EXCEPTION, e);
        }
        return success;
    }

    /**
     * 设置当前文件夹
     * 
     * @param ftp
     * @param path
     * @throws IOException
     * @throws UnsupportedEncodingException
     */
    public static void setPath(FTPClient ftp, String path) throws IOException,
            UnsupportedEncodingException {
        if (path.length() > 1 && path.endsWith(FILE_SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }
        ftp.changeWorkingDirectory(zhFileName(path));
        if (!zhFileName(path).equals(ftp.printWorkingDirectory())) {
            throw new RuntimeException(path + ",目录不存在！当前目录："
                    + ftp.printWorkingDirectory());
        }
    }

    /**
     * Description: 向FTP服务器上传文件
     * 
     * @param url
     *            FTP服务器hostname
     * @param port
     *            FTP服务器端口，如果默认端口请写-1
     * @param username
     *            FTP登录账号
     * @param password
     *            FTP登录密码
     * @param path
     *            FTP服务器保存目录
     * @param filename
     *            上传到FTP服务器上的文件名
     * @param input
     *            输入流
     * @return 成功返回true，否则返回false
     */
    public static boolean uploadFile(String url, int port, String username,
            String password, String path, String filename, InputStream input) {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;

            // 连接FTP服务器
            if (port > -1) {
                ftp.connect(url, port);
            } else {
                ftp.connect(url);
            }

            // 登录FTP
            ftp.login(username, password);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            success = uploadFile(ftp, path, filename, input);
        } catch (IOException e) {
            success = false;
            logger.error(EXCEPTION, e);
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException e) {
                    logger.error(EXCEPTION, e);
                }
            }
        }
        return success;
    }

    /**
     * 远程FTP删除目录下的所有文件包含目录本身
     * 
     * @param remotePath
     * @throws Exception
     */
    public void deleteAllFile(String remotePath) {
        try {
            setPath(ftpClient, remotePath);
            remotePath = repairDirEnd(remotePath);
            FTPFile[] ftpFiles = ftpClient.listFiles();
            for (FTPFile file : ftpFiles) {
                if (file.isDirectory()) {
                    deleteAllFile(remotePath + file.getName());
                } else {
                    ftpClient.deleteFile(zhFileName(file.getName()));
                }
            }
            ftpClient.removeDirectory(zhFileName(remotePath));

        } catch (Exception e) {
            logger.error("从FTP服务器删除文件异常：", e);
            e.printStackTrace();
        }
    }

    /**
     * <删除FTP上的文件> <远程删除FTP服务器上的录音文件>
     * 
     * @param remotePath
     *            远程文件路径
     * @param fileName
     *            待删除的文件名
     * @return
     * @see [类、类#方法、类#成员]
     */
    public boolean deleteFtpFile(String remotePath, String fileName) {
        boolean success = false;
        try {
            remotePath = repairDirEnd(remotePath);
            success = ftpClient.deleteFile(zhFileName(remotePath + fileName));
        } catch (IOException e) {
            logger.error(EXCEPTION, e);
            success = false;
        }
        return success;
    }

    /**
     * <删除FTP上的文件> <远程删除FTP服务器上的录音文件>
     * 
     * @param url
     *            FTP服务器IP地址
     * @param port
     *            FTP服务器端口
     * @param username
     *            FTP服务器登录名
     * @param password
     *            FTP服务器密码
     * @param remotePath
     *            远程文件路径
     * @param fileName
     *            待删除的文件名
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static boolean deleteFtpFile(String url, int port, String username,
            String password, String remotePath, String fileName) {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;

            // 连接FTP服务器
            if (port > -1) {
                ftp.connect(url, port);
            } else {
                ftp.connect(url);
            }

            // 登录
            ftp.login(username, password);
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            remotePath = repairDirEnd(remotePath);
            success = ftp.deleteFile(zhFileName(remotePath + fileName));
            ftp.logout();
        } catch (IOException e) {
            logger.error(EXCEPTION, e);
            success = false;
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException e) {
                    logger.error(EXCEPTION, e);
                }
            }
        }
        return success;
    }

    /**
     * 远程FTP上创建目录
     * 
     * @param dir
     */
    public boolean makeDir(String dir) {
        return makeDir(ftpClient, dir);
    }

    /**
     * 远程FTP上创建目录
     * 
     * @param ftp
     * @param dir
     */
    public static boolean makeDir(FTPClient ftp, String dir) {
        boolean flag = true;
        try {
            flag = ftp.makeDirectory(zhFileName(dir));
            if (flag) {
                System.out.println("make Directory " + dir + " succeed");
            } else {
                System.out.println("make Directory " + dir + " false");
            }
        } catch (Exception ex) {
            logger.error("远程FTP生成目录异常:", ex);
            ex.printStackTrace();
        }
        return flag;
    }
    /**
    * 判断目录是否存在 <br/>
    * @author jingma@iflytek.com
    * @param dir
    * @return
    */
    public boolean existDir(String dir){
        return existDir(ftpClient, dir);
    }
    /**
    * 判断目录是否存在 <br/>
    * @author jingma@iflytek.com
    * @param ftp
    * @param dir
    * @return
    */
    public static boolean existDir(FTPClient ftp,String dir){
        try {
            setPath(ftp, dir);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * @param fileName 要删除的文件或文件夹路径
     */
    public static void deleteDir(String fileName) {
        File file;
        file = new File(fileName);
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
            return;
        }
        for (File f1 : file.listFiles()) {
            if (f1.isDirectory()) {
                deleteDir(f1.getAbsolutePath());
            } else {
                f1.delete();
            }
        }
        file.delete();
    }

    /**
     * @return ftpClient
     */
    public FTPClient getFtpClient() {
        return ftpClient;
    }

    /**
     * @param ftpClient
     *            the ftpClient to set
     */
    public void setFtpClient(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
    }

}
