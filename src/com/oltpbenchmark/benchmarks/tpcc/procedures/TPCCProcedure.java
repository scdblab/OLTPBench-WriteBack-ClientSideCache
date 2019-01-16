/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import com.oltpbenchmark.api.Procedure;
import com.usc.dblab.cafe.Config;
import com.usc.dblab.cafe.NgCache;
//import com.oltpbenchmark.benchmarks.tpcc.cache.CacheTPCCWorker;

public abstract class TPCCProcedure extends Procedure {
    
    public static PrintWriter out = null;
    
    static {
        if (com.oltpbenchmark.benchmarks.Config.DEBUG) {
            try {
                out = new PrintWriter("/home/hieun/Desktop/verify.txt");
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    protected void sleepRetry() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public abstract ResultSet run(
            Connection conn,
            Random gen,
            int terminalWarehouseID, 
            int numWarehouses,
            int terminalDistrictLowerID,
            int terminalDistrictUpperID,
            Map<String, Object> tres) throws SQLException;
    
    public abstract ResultSet run(
            Connection conn, 
            Random gen,
            int terminalWarehouseID, 
            int numWarehouses,
            int terminalDistrictLowerID,
            int terminalDistrictUpperID, 
            NgCache cafe, Map<String, Object> tres) throws SQLException;
}
