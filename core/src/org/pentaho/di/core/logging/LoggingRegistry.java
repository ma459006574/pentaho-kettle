/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.EnvUtil;

public class LoggingRegistry {
    private static LoggingRegistry registry = new LoggingRegistry();
    private Map<String, LoggingObjectInterface> map;
    private Map<String, List<String>> childrenMap;
    private Date lastModificationTime;
    /**
     * 保留时限
     */
    private int blsx = 30 * 60 * 1000;
    /**
     * 清理频率
     */
    private int qlpl = 5 * 60 * 1000;
    /**
     * 清理日志管道的时间
     */
    public static Date clearLogRegTime = new Date();

    private Object syncObject = new Object();

    private LoggingRegistry() {
        this.map = new ConcurrentHashMap<String, LoggingObjectInterface>();
        this.childrenMap = new ConcurrentHashMap<String, List<String>>();

        this.lastModificationTime = new Date();
        clearLogRegTime = new Date();

        blsx = Const.toInt(
                EnvUtil.getSystemProperty("KETTLE_MAX_LOGGING_REGISTRY_BLSX"),
                30) * 60 * 1000;
        qlpl = Const.toInt(
                EnvUtil.getSystemProperty("KETTLE_MAX_LOGGING_REGISTRY_QLPL"),
                5) * 60 * 1000;
    }

    public static LoggingRegistry getInstance() {
        return registry;
    }

    public String registerLoggingSource(Object object) {
        Date start = new Date();
        StringBuffer sb = new StringBuffer(Thread.currentThread().getName()+object+"日志注册："+start+"<1>");
        synchronized (this.syncObject) {
            sb.append(new Date()+"<2>");
            LoggingObject loggingSource = new LoggingObject(object);
            sb.append(new Date()+"<3>");

            LoggingObjectInterface found = findExistingLoggingSource(loggingSource);
            sb.append(new Date()+"<4>");
            if (found != null) {
                LoggingObjectInterface foundParent = found.getParent();
                LoggingObjectInterface loggingSourceParent = loggingSource
                        .getParent();
                if (foundParent != null && loggingSourceParent != null) {
                    String foundParentLogChannelId = foundParent
                            .getLogChannelId();
                    if (foundParentLogChannelId != null
                            && foundParentLogChannelId
                                    .equals(loggingSourceParent
                                            .getLogChannelId())) {
                        Date end = new Date();
                        //延迟1秒,则输出时间线
                        if(start.getTime()<end.getTime()-1000){
                            sb.append(end+"<8>");
                            System.err.println(sb);
                        }
                        return foundParentLogChannelId;
                    }
                }
            }
            sb.append(new Date()+"<5>");

            String logChannelId = UUID.randomUUID().toString();
            loggingSource.setLogChannelId(logChannelId);

            this.map.put(logChannelId, loggingSource);
            sb.append(new Date()+"<6>");

            if (loggingSource.getParent() != null) {
                String parentLogChannelId = loggingSource.getParent()
                        .getLogChannelId();
                if (parentLogChannelId != null) {
                    List<String> parentChildren = this.childrenMap
                            .get(parentLogChannelId);
                    if (parentChildren == null) {
                        parentChildren = new ArrayList<String>();
                        this.childrenMap
                                .put(parentLogChannelId, parentChildren);
                    }
                    parentChildren.add(logChannelId);
                }
            }
            sb.append(new Date()+"<7>");

            this.lastModificationTime = new Date();
            loggingSource.setRegistrationDate(this.lastModificationTime);

            if (clearLogRegTime.getTime() < new Date().getTime() - qlpl) {
                // 排序很影响性能，改为直接移除超出时间范围的日志
                int num = 0;
                clearLogRegTime = new Date();
//                LoggingBuffer ap = KettleLogStore.getAppender();
                long t = clearLogRegTime.getTime() - blsx;
                for (Iterator<Entry<String, LoggingObjectInterface>> it = this.map
                        .entrySet().iterator(); it.hasNext();) {
                    Entry<String, LoggingObjectInterface> e = it.next();
                    // 移除超过20分钟的日志管道
                    if (e.getValue().getRegistrationDate().getTime() < t) {
//                        ap.removeChannelFromBuffer(e.getKey());
                        it.remove();
                        num++;
                    }
                }
                System.err.println(clearLogRegTime + "~" + new Date()+"--" + "移除日志管道数：" + num);
                removeOrphans();
            }
            Date end = new Date();
            //延迟1秒,则输出时间线
            if(start.getTime()<end.getTime()-1000){
                sb.append(end+"<8>");
                System.err.println(sb);
            }
            return logChannelId;
        }
    }

    public LoggingObjectInterface findExistingLoggingSource(
            LoggingObjectInterface loggingObject) {
        LoggingObjectInterface found = null;
        for (LoggingObjectInterface verify : this.map.values()) {
            if (loggingObject.equals(verify)) {
                found = verify;
                break;
            }
        }
        return found;
    }

    public LoggingObjectInterface getLoggingObject(String logChannelId) {
        return this.map.get(logChannelId);
    }

    public Map<String, LoggingObjectInterface> getMap() {
        return this.map;
    }

    /**
     * @return childrenMap
     */
    public Map<String, List<String>> getChildrenMap() {
        return childrenMap;
    }

    public List<String> getLogChannelChildren(String parentLogChannelId) {
        if (parentLogChannelId == null) {
            return null;
        }
        synchronized (this.syncObject) {
            List<String> list = getLogChannelChildren(new ArrayList<String>(),
                    parentLogChannelId);
            list.add(parentLogChannelId);
            return list;
        }
    }

    private List<String> getLogChannelChildren(List<String> children,
            String parentLogChannelId) {
        Date start = new Date();
        StringBuffer sb = new StringBuffer(Thread.currentThread().getName()+parentLogChannelId+"获取子日志："+start+"<1>");
        List<String> list = this.childrenMap.get(parentLogChannelId);
        if (list == null) {
            Date end = new Date();
            //延迟1秒,则输出时间线
            if(start.getTime()<end.getTime()-1000){
                sb.append(end+"<3>");
                System.err.println(sb);
            }
            // Don't do anything, just return the input.
            return children;
        }
        sb.append(new Date()+"<4>");

        Iterator<String> kids = list.iterator();
        while (kids.hasNext()) {
            String logChannelId = kids.next();

            // Add the children recursively
            getLogChannelChildren(children, logChannelId);

            // Also add the current parent
            children.add(logChannelId);
        }
        Date end = new Date();
        //延迟1秒,则输出时间线
        if(start.getTime()<end.getTime()-1000){
            sb.append(end+"<6>");
            System.err.println(sb);
        }

        return children;
    }

    public Date getLastModificationTime() {
        return this.lastModificationTime;
    }

    public String dump(boolean includeGeneral) {
        StringBuffer out = new StringBuffer(50000);
        for (LoggingObjectInterface o : this.map.values()) {
            if ((includeGeneral)
                    || (!o.getObjectType().equals(LoggingObjectType.GENERAL))) {
                out.append(o.getContainerObjectId());
                out.append("\t");
                out.append(o.getLogChannelId());
                out.append("\t");
                out.append(o.getObjectType().name());
                out.append("\t");
                out.append(o.getObjectName());
                out.append("\t");
                out.append(o.getParent() != null ? o.getParent()
                        .getLogChannelId() : "-");
                out.append("\t");
                out.append(o.getParent() != null ? o.getParent()
                        .getObjectType().name() : "-");
                out.append("\t");
                out.append(o.getParent() != null ? o.getParent()
                        .getObjectName() : "-");
                out.append("\n");
            }
        }
        return out.toString();
    }

    /**
     * For junit testing purposes
     * 
     * @return ro items map
     */
    Map<String, LoggingObjectInterface> dumpItems() {
        return Collections.unmodifiableMap(this.map);
    }

    /**
     * For junit testing purposes
     * 
     * @return ro parent-child relations map
     */
    Map<String, List<String>> dumpChildren() {
        return Collections.unmodifiableMap(this.childrenMap);
    }

    public void removeIncludingChildren(String logChannelId) {
        synchronized (this.map) {
            List<String> children = getLogChannelChildren(logChannelId);
            for (String child : children) {
                this.map.remove(child);
            }
            this.map.remove(logChannelId);
            removeOrphans();
        }
    }

    public void removeOrphans() {
        // Remove all orphaned children
        this.childrenMap.keySet().retainAll(this.map.keySet());
    }
}
