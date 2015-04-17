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

package plugin.template;

import java.sql.ResultSet;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import plugin.dataFilter.DataFilter;
import plugin.util.DSPContents;
import plugin.util.DSPDBUtil;

/**
 * Reads information from a database table by using freehand SQL
 *
 * @author Matt
 * @since 8-apr-2003
 */
public class DSPTableInput extends BaseStep implements StepInterface {
  private static Class<?> PKG = DSPTableInputMeta.class; // for i18n purposes, needed by Translator2!!

  private DSPTableInputMeta meta;
  private DSPTableInputData data;
  private DataFilter df;

  /**
 * jobId:TODO(所属job的id).
 * @since JDK 1.6
 */
private String jobId;

/**
 * hasRuleResult:TODO(是否需要转换).
 * @since JDK 1.6
 */
private boolean hasRuleResult;

/**
 * hasVlidateResult:TODO(是否需要验证).
 * @since JDK 1.6
 */
private boolean hasVlidateResult;
/**
 * isFilterIn:TODO(数据验证是否需要错误数据入库).
 * @since JDK 1.6
 */
private boolean isFilterIn = false;//
/**
 * isTransIn:TODO(数据转换是否需要错误数据入库).
 * @since JDK 1.6
 */
private boolean isTransIn = false;//
  

  public DSPTableInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  private RowMetaAndData readStartDate() throws KettleException {
    if ( log.isDetailed() ) {
      logDetailed( "Reading from step [" + data.infoStream.getStepname() + "]" );
    }

    RowMetaInterface parametersMeta = new RowMeta();
    Object[] parametersData = new Object[] {};

    RowSet rowSet = findInputRowSet( data.infoStream.getStepname() );
    if ( rowSet != null ) {
      Object[] rowData = getRowFrom( rowSet ); // rows are originating from "lookup_from"
      while ( rowData != null ) {
        parametersData = RowDataUtil.addRowData( parametersData, parametersMeta.size(), rowData );
        parametersMeta.addRowMeta( rowSet.getRowMeta() );

        rowData = getRowFrom( rowSet ); // take all input rows if needed!
      }

      if ( parametersMeta.size() == 0 ) {
        throw new KettleException( "Expected to read parameters from step ["
          + data.infoStream.getStepname() + "] but none were found." );
      }
    } else {
      throw new KettleException( "Unable to find rowset to read from, perhaps step ["
        + data.infoStream.getStepname() + "] doesn't exist. (or perhaps you are trying a preview?)" );
    }

    RowMetaAndData parameters = new RowMetaAndData( parametersMeta, parametersData );

    return parameters;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    if ( first ) { // we just got started

      Object[] parameters;
      RowMetaInterface parametersMeta;
      first = false;

      // Make sure we read data from source steps...
      if ( data.infoStream.getStepMeta() != null ) {
        if ( meta.isExecuteEachInputRow() ) {
          if ( log.isDetailed() ) {
            logDetailed( "Reading single row from stream [" + data.infoStream.getStepname() + "]" );
          }
          data.rowSet = findInputRowSet( data.infoStream.getStepname() );
          if ( data.rowSet == null ) {
            throw new KettleException( "Unable to find rowset to read from, perhaps step ["
              + data.infoStream.getStepname() + "] doesn't exist. (or perhaps you are trying a preview?)" );
          }
          parameters = getRowFrom( data.rowSet );
          parametersMeta = data.rowSet.getRowMeta();
        } else {
          if ( log.isDetailed() ) {
            logDetailed( "Reading query parameters from stream [" + data.infoStream.getStepname() + "]" );
          }
          RowMetaAndData rmad = readStartDate(); // Read values in lookup table (look)
          parameters = rmad.getData();
          parametersMeta = rmad.getRowMeta();
        }
        if ( parameters != null ) {
          if ( log.isDetailed() ) {
            logDetailed( "Query parameters found = " + parametersMeta.getString( parameters ) );
          }
        }
      } else {
        parameters = new Object[] {};
        parametersMeta = new RowMeta();
      }

      if ( meta.isExecuteEachInputRow() && ( parameters == null || parametersMeta.size() == 0 ) ) {
        setOutputDone(); // signal end to receiver(s)
        return false; // stop immediately, nothing to do here.
      }

      boolean success = doQuery( parametersMeta, parameters );
      if ( !success ) {
        return false;
      }
    } else {
      if ( data.thisrow != null ) { // We can expect more rows

        data.nextrow = data.db.getRow( data.rs, meta.isLazyConversionActive() );
        if ( data.nextrow != null ) {
          incrementLinesInput();
        }
      }
    }

    if ( data.thisrow == null ) { // Finished reading?

      boolean done = false;
      if ( meta.isExecuteEachInputRow() ) // Try to get another row from the input stream
      {
        Object[] nextRow = getRowFrom( data.rowSet );
        if ( nextRow == null ) { // Nothing more to get!

          done = true;
        } else {
          // First close the previous query, otherwise we run out of cursors!
          closePreviousQuery();

          boolean success = doQuery( data.rowSet.getRowMeta(), nextRow ); // OK, perform a new query
          if ( !success ) {
            return false;
          }

          if ( data.thisrow != null ) {
            putRow( data.rowMeta, data.thisrow ); // fill the rowset(s). (wait for empty)
            data.thisrow = data.nextrow;

            if ( checkFeedback( getLinesInput() ) ) {
              if ( log.isBasic() ) {
                logBasic( "linenr " + getLinesInput() );
              }
            }
          }
        }
      } else {
        done = true;
      }

      if ( done ) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }
    } else {
        // 调用数据过滤方法进行过滤
        boolean filterFlag = true;//验证结果
    	boolean transFlag = true;//转换结果
    	Object[] objs = data.thisrow;
    	if(StringUtils.isNotEmpty(jobId)){
    		if(hasRuleResult){//如果有过滤规则，则进行过滤
    			//通过jobid和节点类型查询该节点的错误数据是否入库
    			if(!df.doFilter(jobId,objs,meta.getSQL())){
    				filterFlag = false;
    			}
    		}
    		if(filterFlag && hasVlidateResult){//如果有转换规则，并且验证是正确的
    			Object[] rets = df.doTrans(jobId,objs,meta.getSQL());
    			if(rets == null){
    				transFlag = false;
    			}else{
    				objs = rets;
    			}
    		}
    	}
      //如果验证结果是错误，则根据验证的前台选择的是否进行错误数据是否入库(验证错误不需要再进行转换)
      if(!filterFlag&&isFilterIn){
    	  putRow( data.rowMeta, objs);
      //如果验证结果是正确的，则根据转换的前台选择转换是否进行错误数据入库和转换结果决定是否入库
      }else if(isTransIn || transFlag){
    	  putRow( data.rowMeta, objs);
      }

      data.thisrow = data.nextrow;
      if ( checkFeedback( getLinesInput() ) ) {
        if ( log.isBasic() ) {
          logBasic( "linenr " + getLinesInput() );
        }
      }
    }

    return true;
  }

  private void closePreviousQuery() throws KettleDatabaseException {
    if ( data.db != null ) {
      data.db.closeQuery( data.rs );
     // df.db.closeQuery(data.rs );
    }
  }

  private boolean doQuery( RowMetaInterface parametersMeta, Object[] parameters ) throws KettleDatabaseException {
    boolean success = true;
    // Open the query with the optional parameters received from the source steps.
    String sql = null;
    if ( meta.isVariableReplacementActive() ) {
      sql = environmentSubstitute( meta.getSQL() );
    } else {
      sql = meta.getSQL();
    }

    if ( log.isDetailed() ) {
      logDetailed( "SQL query : " + sql );
    }
    if ( parametersMeta.isEmpty() ) {
      data.rs = data.db.openQuery( sql, null, null, ResultSet.FETCH_FORWARD, meta.isLazyConversionActive() );
    } else {
      data.rs =
        data.db.openQuery( sql, parametersMeta, parameters, ResultSet.FETCH_FORWARD, meta
          .isLazyConversionActive() );
    }
    if ( data.rs == null ) {
      logError( "Couldn't open Query [" + sql + "]" );
      setErrors( 1 );
      stopAll();
      success = false;
    } else {
      // Keep the metadata
      data.rowMeta = data.db.getReturnRowMeta();

      // Set the origin on the row metadata...
      if ( data.rowMeta != null ) {
        for ( ValueMetaInterface valueMeta : data.rowMeta.getValueMetaList() ) {
//        	if("DATE".equals(valueMeta.getOriginalColumnTypeName().toUpperCase())){
//        		String comments = valueMeta.getComments();
//        		String name = valueMeta.getName();
//        		valueMeta = new ValueMetaString();
//        		valueMeta.setOriginalColumnTypeName("CHAR");
//        		valueMeta.setComments(comments);
//        		valueMeta.setName(name);
//        	}
        	
          valueMeta.setOrigin( getStepname() );
        }
      }

      // Get the first row...
      data.thisrow = data.db.getRow( data.rs );
      if ( data.thisrow != null ) {
        incrementLinesInput();
        data.nextrow = data.db.getRow( data.rs );
        if ( data.nextrow != null ) {
          incrementLinesInput();
        }
      }
    }
    return success;
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( log.isBasic() ) {
      logBasic( "Finished reading query, closing connection." );
    }
    try {
      closePreviousQuery();
    } catch ( KettleException e ) {
      logError( "Unexpected error closing query : " + e.toString() );
      setErrors( 1 );
      stopAll();
    } finally {
      if ( data.db != null ) {
        data.db.disconnect();
        DataFilter.db.disconnect();
      }
    }

    super.dispose( smi, sdi );
  }

  /** Stop the running query */
  public void stopRunning( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (DSPTableInputMeta) smi;
    data = (DSPTableInputData) sdi;

    setStopped( true );

    if ( data.db != null && !data.isCanceled ) {
      synchronized ( data.db ) {
        data.db.cancelQuery();
      }
      data.isCanceled = true;
    }
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (DSPTableInputMeta) smi;
    data = (DSPTableInputData) sdi;
    df =  DataFilter.getInstance();
    if ( super.init( smi, sdi ) ) {
      // Verify some basic things first...
      //
      boolean passed = true;
      if ( Const.isEmpty( meta.getSQL() ) ) {
        logError( BaseMessages.getString( PKG, "TableInput.Exception.SQLIsNeeded" ) );
        passed = false;
      }

      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "TableInput.Exception.DatabaseConnectionsIsNeeded" ) );
        passed = false;
      }
      if ( !passed ) {
        return false;
      }

      data.infoStream = meta.getStepIOMeta().getInfoStreams().get( 0 );
      if ( meta.getDatabaseMeta() == null ) {
        logError( BaseMessages.getString( PKG, "TableInput.Init.ConnectionMissing", getStepname() ) );
        return false;
      }
      data.db = new Database( this, meta.getDatabaseMeta() );
      data.db.shareVariablesWith( this );

      DataFilter.db  = DSPDBUtil.getConn();
      DataFilter.db.shareVariablesWith( this );
      
      data.db.setQueryLimit( Const.toInt( environmentSubstitute( meta.getRowLimit() ), 0 ) );
      
      DataFilter.db.setQueryLimit( Const.toInt( environmentSubstitute( meta.getRowLimit() ), 0 ) );
      
      try {
        if ( getTransMeta().isUsingUniqueConnections() ) {
          synchronized ( getTrans() ) {
            data.db.connect( getTrans().getTransactionId(), getPartitionID() );
            DataFilter.db.connect( getTrans().getTransactionId(), "dsp" );
          }
        } else {
          data.db.connect( getPartitionID() );
          
          DataFilter.db.connect( getTrans().getTransactionId(), null );
        }

        if ( meta.getDatabaseMeta().isRequiringTransactionsOnQueries() ) {
          data.db.setCommit( 100 ); // needed for PGSQL it seems...
          DataFilter.db.setCommit( 100 ); 
        }
        if ( log.isDetailed() ) {
          logDetailed( "Connected to database..." );
        }
        
    	jobId = "";
//    	SpoonPerspectiveManager manager = SpoonPerspectiveManager.getInstance();
//        if ( manager != null && manager.getActivePerspective() != null ) {
//        	EngineMetaInterface met1 = manager.getActivePerspective().getActiveMeta();
//            if ( met1 instanceof JobMeta ) {
//            	jobId = String.valueOf(((JobMeta) met1).getObjectId());
//            }
//      }
		//add by llchen 用后台进程运行时，jobid得不到，这里通过获取转换，再获取转换的作业id
		jobId = getTrans().getParentJob().getObjectId().toString();
        log.logBasic("jobId:"+jobId);
    	if(StringUtils.isNotEmpty(jobId)){
    		hasRuleResult = df.isValidate(jobId,DSPContents.NodeType.FILTER_TYPE);
    		hasVlidateResult = df.isValidate(jobId,DSPContents.NodeType.TRANS_TYPE);
    		isFilterIn = df.validateIsWrongIn(jobId,DSPContents.NodeType.FILTER_TYPE);
    		isTransIn = df.transIsWrongIn(jobId,DSPContents.NodeType.TRANS_TYPE);
    	}
    	
    	df.init(meta.getSQL(),jobId);
    	
        return true;
      } catch ( KettleException e ) {
        logError( "An error occurred, processing will be stopped: " + e.getMessage() );
        setErrors( 1 );
        stopAll();
      }
    }

    return false;
  }

  public boolean isWaitingForData() {
    return true;
  }


}
