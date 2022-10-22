/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.slf4j.Logger;

public class LogUtils {
    public static void printLog(Logger logger, Object... objects) {
        //        Method caller = new Object(){}.getClass().getEnclosingMethod();
        StackTraceElement callerElement = new Exception().getStackTrace()[1];

        printLog(
                String.format(
                        "%s.%s:%d",
                        callerElement.getClassName(),
                        callerElement.getMethodName(),
                        callerElement.getLineNumber()),
                logger,
                objects);
    }

    private static void printLog(Object caller, Logger logger, Object... objects) {
        String[] strings = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] != null) {
                strings[i] = objects[i].toString();
            } else {
                strings[i] = null;
            }
        }

        logger.info(String.format("%s %s", caller, String.join(" ", strings)));
    }
}
