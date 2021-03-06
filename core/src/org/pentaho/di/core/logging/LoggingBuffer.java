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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.pentaho.di.core.Const;

/**
 * This class keeps the last N lines in a buffer
 * 
 * @author matt
 * 
 */
public class LoggingBuffer {
    private String name;

    private List<BufferLine> buffer;

    private int bufferSize;

    private KettleLogLayout layout;

    private List<KettleLoggingEventListener> eventListeners;

    private BufferLine lastBuffer;

    public LoggingBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = Collections.synchronizedList(new ArrayList<BufferLine>(
                bufferSize));
        layout = new KettleLogLayout(true);
        eventListeners = Collections
                .synchronizedList(new ArrayList<KettleLoggingEventListener>());
    }

    /**
     * @return the number (sequence, 1..N) of the last log line. If no records
     *         are present in the buffer, 0 is returned.
     */
    public int getLastBufferLineNr() {
        return lastBuffer.getNr();
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
        Date start = new Date();
        StringBuffer sb = new StringBuffer(Thread.currentThread().getName()
                + "获取日志信息：" + start + "<1>");
        List<KettleLoggingEvent> lines = new ArrayList<KettleLoggingEvent>();
        Set<String> channelHM = null;
        if (channelId != null) {
            channelHM = new HashSet<String>(channelId);
        }

        synchronized (buffer) {
            sb.append(new Date() + "<2>");
            int fastNr = lastBuffer.getNr() - buffer.size() + 1;
            int startIdx = 0;
            if (from > fastNr) {
                startIdx = from - fastNr+1;
            }
            int endIdx = to - fastNr + 1;
            for (; startIdx < endIdx; startIdx++) {
                KettleLoggingEvent event = buffer.get(startIdx).getEvent();
                Object messageObject = event.getMessage();
                if (!(messageObject instanceof LogMessage)) {
                    System.err.println("不是LogMessage对象："
                            + messageObject.getClass());
                    continue;
                }
                LogMessage message = (LogMessage) messageObject;
                // Typically, the log channel id is the one from the
                // transformation or job running currently.
                // However, we also want to see the details of the steps etc.
                // So we need to look at the parents all the way up if needed...
                //
                if (channelHM==null||channelHM.contains(message.getLogChannelId())) {
                    lines.add(event);
                }

            }
            Date end = new Date();
            // 延迟1秒,则输出时间线
            if (start.getTime() < end.getTime() - 1000) {
                sb.append(end + "<8>");
                System.err.println(sb);
            }
        }

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
        List<String> childIds = LoggingRegistry.getInstance()
                .getLogChannelChildren(parentLogChannelId);

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
            event.gcsj.append(new Date() + "<a1>");
            if (buffer.size() > bufferSize) {
                buffer.clear();
            }
            lastBuffer = new BufferLine(event);
            buffer.add(lastBuffer);
            event.gcsj.append(new Date() + "<a2>");
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
        return buffer.size();
    }

    /**
     * Removes all rows for the channel with the specified id
     * 
     * @param id
     *            the id of the logging channel to remove
     */
    public void removeChannelFromBuffer(String id) {
        synchronized (buffer) {
            Iterator<BufferLine> iterator = buffer.iterator();
            while (iterator.hasNext()) {
                BufferLine bufferLine = iterator.next();
                Object payload = bufferLine.getEvent().getMessage();
                if (payload instanceof LogMessage) {
                    LogMessage message = (LogMessage) payload;
                    if (id.equals(message.getLogChannelId())) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    public int size() {
        return buffer.size();
    }

    public void removeGeneralMessages() {
        synchronized (buffer) {
            Iterator<BufferLine> iterator = buffer.iterator();
            while (iterator.hasNext()) {
                BufferLine bufferLine = iterator.next();
                Object payload = bufferLine.getEvent().getMessage();
                if (payload instanceof LogMessage) {
                    LogMessage message = (LogMessage) payload;
                    LoggingObjectInterface loggingObject = LoggingRegistry
                            .getInstance().getLoggingObject(
                                    message.getLogChannelId());
                    if (loggingObject != null
                            && LoggingObjectType.GENERAL.equals(loggingObject
                                    .getObjectType())) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    public Iterator<BufferLine> getBufferIterator() {
        return buffer.iterator();
    }

    public String dump() {
        StringBuffer buf = new StringBuffer(50000);
        synchronized (buffer) {
            for (BufferLine line : buffer) {
                Object payload = line.getEvent().getMessage();
                if (payload instanceof LogMessage) {
                    LogMessage message = (LogMessage) payload;
                    // LoggingObjectInterface loggingObject =
                    // LoggingRegistry.getInstance().getLoggingObject(message.getLogChannelId());
                    buf.append(message.getLogChannelId() + "\t"
                            + message.getSubject() + "\t"
                            + message.getMessage() + "\n");
                }

            }
        }
        return buf.toString();
    }

    public void removeBufferLines(List<BufferLine> linesToRemove) {
        buffer.removeAll(linesToRemove);
    }

    public List<BufferLine> getBufferLinesBefore(long minTimeBoundary) {
        List<BufferLine> linesToRemove = new ArrayList<BufferLine>();
        synchronized (buffer) {
            for (Iterator<BufferLine> i = buffer.iterator(); i.hasNext();) {
                BufferLine bufferLine = i.next();
                if (bufferLine.getEvent().timeStamp < minTimeBoundary) {
                    linesToRemove.add(bufferLine);
                } else {
                    break;
                }
            }
        }
        return linesToRemove;
    }

    public void addLogggingEvent(KettleLoggingEvent loggingEvent) {
        loggingEvent.gcsj.append(new Date() + "<1>");
        doAppend(loggingEvent);
        loggingEvent.gcsj.append(new Date() + "<2>");
        synchronized (eventListeners) {
            loggingEvent.gcsj.append(new Date() + "<3>");
            for (KettleLoggingEventListener listener : eventListeners) {
                listener.eventAdded(loggingEvent);
                loggingEvent.gcsj.append(new Date() + "<4>");
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
