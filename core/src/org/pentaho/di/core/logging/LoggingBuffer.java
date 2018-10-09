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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.pentaho.di.core.Const;

/**
 * This class keeps the last N lines in a buffer
 * 
 * @author matt
 * 
 */
public class LoggingBuffer {
    private static AtomicInteger sequence = new AtomicInteger( 0 );
    
    private String name;
    /**
    * 日志管道仓库
    */
    private static LoggingRegistry lr = LoggingRegistry.getInstance();

    /**
     * <管道，日志行>
     */
    private Map<String, Map<Integer,KettleLoggingEvent>> buffer;
    
    private int lastBufferLineNr = 0;

    private int bufferSize;

    private KettleLogLayout layout;

    private List<KettleLoggingEventListener> eventListeners;

    public LoggingBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = Collections.synchronizedMap(new HashMap<String, Map<Integer,KettleLoggingEvent>>());
        layout = new KettleLogLayout(true);
        eventListeners = Collections
                .synchronizedList(new ArrayList<KettleLoggingEventListener>());
    }

    /**
     * @return the number (sequence, 1..N) of the last log line. If no records
     *         are present in the buffer, 0 is returned.
     */
    public int getLastBufferLineNr() {
       return lastBufferLineNr;
    }

    /**
     * 
     * @param channelId
     *            channel IDs to grab
     * @param includeGeneral
     *            include general log lines
     * @param from
     * @param to
     * @return
     */
    public List<KettleLoggingEvent> getLogBufferFromTo(List<String> channelId,
            boolean includeGeneral, int from, int to) {
        List<KettleLoggingEvent> lines = new ArrayList<KettleLoggingEvent>();

        synchronized (buffer) {
            for(String ci:channelId){
                Map<Integer, KettleLoggingEvent> l = buffer.get(ci);
                if(l!=null){
                    for (Entry<Integer, KettleLoggingEvent> el : l.entrySet()) {
                        if (el.getKey() > from && el.getKey() <= to) {
                            try {
                                // String string =
                                // layout.format(line.getEvent());
                                lines.add(el.getValue());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
        Collections.sort(lines, new Comparator<KettleLoggingEvent>() {
            @Override
            public int compare(KettleLoggingEvent o1, KettleLoggingEvent o2) {
                return (int) (o1.getTimeStamp()-o2.getTimeStamp());
            }
        });
        return lines;
    }

    /**
     * 
     * @param parentLogChannelId
     *            the parent log channel ID to grab
     * @param includeGeneral
     *            include general log lines
     * @param from
     * @param to
     * @return
     */
    public List<KettleLoggingEvent> getLogBufferFromTo(
            String parentLogChannelId, boolean includeGeneral, int from, int to) {

        // Typically, the log channel id is the one from the transformation or
        // job running currently.
        // However, we also want to see the details of the steps etc.
        // So we need to look at the parents all the way up if needed...
        //
        List<String> childIds = lr.getLogChannelChildren(parentLogChannelId);

        return getLogBufferFromTo(childIds, includeGeneral, from, to);
    }

    public StringBuffer getBuffer(String parentLogChannelId,
            boolean includeGeneral, int startLineNr, int endLineNr) {
        StringBuffer stringBuffer = new StringBuffer(10000);

        List<KettleLoggingEvent> events = getLogBufferFromTo(
                parentLogChannelId, includeGeneral, startLineNr, endLineNr);
        for (KettleLoggingEvent event : events) {
            stringBuffer.append(layout.format(event)).append(Const.CR);
        }

        return stringBuffer;
    }

    public StringBuffer getBuffer(String parentLogChannelId,
            boolean includeGeneral) {
        return getBuffer(parentLogChannelId, includeGeneral, 0);
    }

    public StringBuffer getBuffer(String parentLogChannelId,
            boolean includeGeneral, int startLineNr) {
        return getBuffer(parentLogChannelId, includeGeneral, startLineNr,
                getLastBufferLineNr());
    }

    public StringBuffer getBuffer() {
        return getBuffer(null, true);
    }

    public void close() {
    }

    public void doAppend(KettleLoggingEvent event) {
        synchronized (buffer) {
            Object payload = event.getMessage();
            if (payload instanceof LogMessage) {
                LogMessage message = (LogMessage) payload;
                Map<Integer, KettleLoggingEvent> cl = buffer.get(message.getLogChannelId());
                if(cl==null){
                    cl = new HashMap<Integer, KettleLoggingEvent>();
                    buffer.put(message.getLogChannelId(), cl);
                }
                else if (bufferSize > 0 && cl.size() > bufferSize) {
                    cl.clear();
                }
                lastBufferLineNr = sequence.incrementAndGet();
                cl.put(lastBufferLineNr, event);
            }else{
                System.out.println("不是日志对象："+event.getMessage());
            }
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setLayout(KettleLogLayout layout) {
        this.layout = layout;
    }

    public KettleLogLayout getLayout() {
        return layout;
    }

    public boolean requiresLayout() {
        return true;
    }

    public void clear() {
        buffer.clear();
    }

    /**
     * @return the maximum number of lines that this buffer contains, 0 or lower
     *         means: no limit
     */
    public int getMaxNrLines() {
        return bufferSize;
    }

    /**
     * @param maxNrLines
     *            the maximum number of lines that this buffer should contain, 0
     *            or lower means: no limit
     */
    public void setMaxNrLines(int maxNrLines) {
        this.bufferSize = maxNrLines;
    }

    /**
     * @return the nrLines
     */
    public int getNrLines() {
        int size = 0;
        synchronized (buffer) {
            for (Entry<String, Map<Integer, KettleLoggingEvent>> cl : buffer.entrySet()) {
                size += cl.getValue().size();
            }
        }
        return size;
    }

    /**
     * Removes all rows for the channel with the specified id
     * 
     * @param id
     *            the id of the logging channel to remove
     */
    public void removeChannelFromBuffer(String id) {
        synchronized (buffer) {
            buffer.remove(id);
        }
    }

    public int size() {
        return getNrLines();
    }

    public void removeGeneralMessages() {
        synchronized (buffer) {
            Iterator<Entry<String, Map<Integer, KettleLoggingEvent>>> iterator = buffer.entrySet().iterator();
            while (iterator.hasNext()) {
                LoggingObjectInterface loggingObject = lr.getLoggingObject(
                        iterator.next().getKey());
                if (loggingObject != null&& LoggingObjectType.GENERAL.equals(loggingObject
                                .getObjectType())) {
                    iterator.remove();
                }
            }
        }
    }

    public String dump() {
        StringBuffer buf = new StringBuffer(50000);
        synchronized (buffer) {
            for (Entry<String, Map<Integer, KettleLoggingEvent>> cl : buffer.entrySet()) {
                for(Entry<Integer, KettleLoggingEvent> cle:cl.getValue().entrySet()){
                    LogMessage message = (LogMessage) cle.getValue().getMessage();
                    buf.append(message.getLogChannelId() + "\t"
                            + message.getSubject() + "\t"
                            + message.getMessage() + "\n");
                }

            }
        }
        return buf.toString();
    }

    public void addLogggingEvent(KettleLoggingEvent loggingEvent) {
        doAppend(loggingEvent);
        synchronized (eventListeners) {
            for (KettleLoggingEventListener listener : eventListeners) {
                listener.eventAdded(loggingEvent);
            }
        }
    }

    public void addLoggingEventListener(KettleLoggingEventListener listener) {
        eventListeners.add(listener);
    }

    public void removeLoggingEventListener(KettleLoggingEventListener listener) {
        eventListeners.remove(listener);
    }
}
