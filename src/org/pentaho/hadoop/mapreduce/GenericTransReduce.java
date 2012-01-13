/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * A reducer class that just emits the sum of the input values.
 */
@SuppressWarnings("deprecation")
public class GenericTransReduce<K extends WritableComparable<?>, V extends Iterator<Writable>, K2, V2> extends PentahoMapReduceBase<K2, V2> implements
    Reducer<K, V, K2, V2> {

  public GenericTransReduce() throws KettleException {
      super();
      this.setMRType(MROperations.Reduce);
  } 
    
  public void reduce(final K key, final Iterator<V> values, final OutputCollector<K2, V2> output, final Reporter reporter) throws IOException {
    try {
      reporter.setStatus("Begin processing record");

      if (trans == null) {
        throw new RuntimeException("Error initializing transformation.  See error log."); //$NON-NLS-1$
      } else {
        // Clean up old logging
        CentralLogStore.discardLines(trans.getLogChannelId(), true);
      }

      // Create a copy of trans so we don't continue to add new TransListeners and run into a ConcurrentModificationException
      // when this reducer is reused "quickly"
      trans = MRUtil.recreateTrans(trans);
      
      //  share the variables from the PDI job.
      //  we do this here instead of in createTrans() as MRUtil.recreateTrans() will not 
      //  copy "execution" trans information.
      if (variableSpace != null) {
         setDebugStatus(reporter, "Sharing the VariableSpace from the PDI job.");
         trans.shareVariablesWith(variableSpace);
      
         if (debug) {
            
            //  list the variables
            List<String> variables = Arrays.asList(trans.listVariables());
            Collections.sort(variables);
        
            if (variables != null) {
               setDebugStatus(reporter, "Variables: ");
               for(String variable: variables) {
                  setDebugStatus(reporter, "     "+variable+" = "+trans.getVariable(variable));
               }
            }
         }
      }
      else {
         setDebugStatus(reporter, "variableSpace is null.  We are not going to share it with the trans.");
      }
      
      //  set the trans' log level if we have our's set
      if (logLevel != null) {
         setDebugStatus(reporter, "Setting the trans.logLevel to "+logLevel.toString());
         trans.setLogLevel(logLevel);
      }
      else {
         setDebugStatus(reporter, getClass().getName()+".logLevel is null.  The trans log lwevel will not be set.");
      }

      setDebugStatus(reporter, "Preparing transformation for execution");
      trans.prepareExecution(null);
      
      try {
        setDebugStatus(reporter, "Locating output step: " + reduceOutputStepName);
        StepInterface outputStep = trans.findRunThread(reduceOutputStepName);
        if (outputStep != null) {
          RowMeta injectorRowMeta = new RowMeta();
          rowCollector = new OutputCollectorRowListener<K2, V2>(output, outClassK, outClassV, reporter, debug);
          outputStep.addRowListener(rowCollector);
  
          RowProducer rowProducer = null;
          ITypeConverter inConverterK = null;
          ITypeConverter inConverterV = null;
  
          Object value = null;
  
          setDebugStatus(reporter, "Locating input step: " + reduceInputStepName);
          if (reduceInputStepName != null) {
            // Setup row injection
            rowProducer = trans.addRowProducer(reduceInputStepName, 0);
            StepInterface inputStep = rowProducer.getStepInterface();
            StepMetaInterface inputStepMeta = inputStep.getStepMeta().getStepMetaInterface();
  
            InKeyValueOrdinals inOrdinals = null;
            if (inputStepMeta instanceof BaseStepMeta) {
              setDebugStatus(reporter, "Generating converters from RowMeta for injection into the reducer transformation");
  
              // Convert to BaseStepMeta and use getFields(...) to get the row meta and therefore the expected input types
              ((BaseStepMeta) inputStepMeta).getFields(injectorRowMeta, null, null, null, null);
  
              inOrdinals = new InKeyValueOrdinals(injectorRowMeta);
              
              if(inOrdinals.getKeyOrdinal() < 0 || inOrdinals.getValueOrdinal() < 0) {
                throw new KettleException("key or value is not defined in transformation injector step");
              }
              
              
              // Get a converter for the Key
              if (injectorRowMeta.getValueMeta(inOrdinals.getKeyOrdinal()) != null) {
                Class<?> metaClass = null;
  
                // Get the concrete java class that corresponds to a given Kettle meta data type
                metaClass = MRUtil.getJavaClass(injectorRowMeta.getValueMeta(inOrdinals.getKeyOrdinal()));
  
                if (metaClass != null) {
                  // If a KettleType with a concrete conversion was found then use it
                  inConverterK = TypeConverterFactory.getInstance().getConverter(key.getClass(), metaClass);
                }
              }
  
              // Get a converter for the Value
              if (injectorRowMeta.getValueMeta(inOrdinals.getValueOrdinal()) != null) {
                Class<?> metaClass = null;
  
                // Get the concrete java class that corresponds to a given Kettle meta data type
                metaClass = MRUtil.getJavaClass(injectorRowMeta.getValueMeta(inOrdinals.getValueOrdinal()));
  
                // we need to peek into the first value to get the class (the combination of Iterator and generic makes this a pain)
                if (values.hasNext()) {
                  value = values.next();
                }
                if (metaClass != null && value != null) {
                  inConverterV = TypeConverterFactory.getInstance().getConverter(value.getClass(), metaClass);
                }
              }
            }
            
            trans.startThreads();
  
            if (rowProducer != null) {
              // Execute row injection
              // We loop through the values to do this
  
              if (value != null) {
                if(inOrdinals != null) {
                  injectValue(key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(), inConverterV, injectorRowMeta, rowProducer, reporter);
                } else {
                  injectValue(key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, reporter);
                }
              }
  
              while (values.hasNext()) {
                value = values.next();
                
                if(inOrdinals != null) {
                  injectValue(key, inOrdinals.getKeyOrdinal(), inConverterK, value, inOrdinals.getValueOrdinal(), inConverterV, injectorRowMeta, rowProducer, reporter);
                } else {
                  injectValue(key, inConverterK, value, inConverterV, injectorRowMeta, rowProducer, reporter);
                }
              }
              rowProducer.finished();
            }
  
            trans.waitUntilFinished();
            setDebugStatus(reporter, "Reducer transformation has finished");
            
          } else {
            setDebugStatus(reporter, "No input stepname was defined");
          }
  
          if (getException() != null) {
            setDebugStatus(reporter, "An exception was generated by the reducer transformation");
            // Bubble the exception from within Kettle to Hadoop
            throw getException();
          }
  
        } else {
          if (reduceOutputStepName != null) {
            setDebugStatus(reporter, "Output step [" + reduceOutputStepName + "]could not be found");
            throw new KettleException("Output step not defined in transformation");
          } else {
            setDebugStatus(reporter, "Output step name not specified");
          }
        }
      } finally {
        try {
          trans.stopAll();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
        try {
          trans.cleanup();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace(System.err);
      setDebugStatus(reporter, "An exception was generated by the reducer task");
      throw new IOException(e);
    }
    reporter.setStatus("Completed processing record");
  }

}