/**
* Project Name:KettleUtil
* Date:2016年7月14日下午4:13:34
* Copyright (c) 2016, jingma@iflytek.com All Rights Reserved.
*/

package com.metl.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;

import net.lingala.zip4j.exception.ZipException;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.github.junrar.Archive;
import com.github.junrar.rarfile.FileHeader;

/**
 * Rar压缩文件处理工具类 <br/>
 * date: 2016年7月14日 下午4:13:34 <br/>
 * @author jingma@iflytek.com
 * @version 
 */
public class RarUtil {
    /**
    * 是否将rar文件的名称作为对应解压文件父级目录
    */
    private static boolean isRarNameToDir = false;

    /** 
     * 使用给定密码解压指定目录下的rar压缩文件到指定目录 
     * <p> 
     * 如果指定目录不存在,可以自动创建,不合法的路径将导致异常被抛出 
     * @param rarDir 指定的rar压缩文件 
     * @param dest 解压目录 
     * @throws ZipException 压缩文件有损坏或者解压缩失败抛出 
     */  
    public static void unRarDir(String rarDir, String dest) throws ZipException {  
        File zips = new File(rarDir);
        for(File zip:zips.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if(name.endsWith(".rar")){
                    return true;
                }
                return false;
            }
        })){
            unRarFile(zip.getAbsolutePath(),dest);
        }
    }
    /** 
    * 根据原始rar路径，解压到指定文件夹下.      
    * @param srcRarPath 原始rar路径 
    * @param dstDirectoryPath 解压到的文件夹      
    */
    public static void unRarFile(String srcRarPath, String dstDirectoryPath) {
        if (!srcRarPath.toLowerCase().endsWith(".rar")) {
            System.out.println("非rar文件！");
            return;
        }
        File dstDiretory = new File(dstDirectoryPath);
        if (!dstDiretory.exists()) {// 目标目录不存在时，创建该文件夹
            dstDiretory.mkdirs();
        }
        Archive a = null;
        try {
            File srcFile = new File(srcRarPath);
            String rarName = srcFile.getName().substring(0,srcFile.getName().indexOf("."));
            a = new Archive(srcFile);
            if (a != null) {
                a.getMainHeader().print(); // 打印文件信息.
                FileHeader fh = a.nextFileHeader();
                while (fh != null) {
                    System.out.println(JSONObject.toJSON(fh));
                    if (fh.isDirectory()) { // 文件夹 
                        File fol = new File(dstDirectoryPath + File.separator
                                + fh.getFileNameString());
                        fol.mkdirs();
                    } else { // 文件
                        String fileName = fh.getFileNameW();
                        if(StringUtils.isBlank(fileName)){
                            fileName = fh.getFileNameString();
                        }
                        if(isRarNameToDir){
                            fileName = rarName + File.separator + fileName;
                        }
                        File out = new File(dstDirectoryPath + File.separator
                                + fileName);
//                        System.out.println(out.getAbsolutePath());
                        try {// 之所以这么写try，是因为万一这里面有了异常，不影响继续解压. 
                            if (!out.exists()) {
                                if (!out.getParentFile().exists()) {// 相对路径可能多级，可能需要创建父目录. 
                                    out.getParentFile().mkdirs();
                                }
                                out.createNewFile();
                            }
                            FileOutputStream os = new FileOutputStream(out);
                            a.extractFile(fh, os);
                            os.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    fh = a.nextFileHeader();
                }
                a.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * @return isRarNameToDir 
     */
    public static boolean isRarNameToDir() {
        return isRarNameToDir;
    }
    /**
     * @param isRarNameToDir the isRarNameToDir to set
     */
    public static void setRarNameToDir(boolean isRarNameToDir) {
        RarUtil.isRarNameToDir = isRarNameToDir;
    }
    public static void main(String[] args) throws ZipException {
        setRarNameToDir(true);
        unRarDir("E:\\temp", "E:\\temp\\rar");
    }
}
