/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.training.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MyProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @Test
    public void testProcessorIsValidWhenMandatoryPropertyIsSpecified() {
        testRunner.setProperty("shift-by", "2");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorIsInvalidWhenMissingMandatoryProperties() {
        testRunner.assertNotValid();
    }

    @Test
    public void testProcessorTransfersSuccesWithCorrectInput() {
        testRunner.enqueue(createMockFlowFile("Hello World"));
        testRunner.setProperty("shift-by", "2");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred("success");
        testRunner.assertTransferCount("success", 1);
    }

    @Test
    public void testProcessorTransfersFailureWithNullInput() {
        testRunner.enqueue(createMockFlowFile(null));
        testRunner.setProperty("shift-by", "2");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred("failure");
        testRunner.assertTransferCount("failure", 1);
    }

    @Test
    public void testNothingIsTransferredWhenNoFlowFileInQueue() {
        testRunner.setProperty("shift-by", "2");
        testRunner.run();

        testRunner.assertTransferCount("failure", 0);
        testRunner.assertTransferCount("success", 0);
    }

    private FlowFile createMockFlowFile(String inputVal) {
        MockFlowFile flowFile = new MockFlowFile(1L);

        Map<String, String> flowFileAttrs = new HashMap<>();
        flowFileAttrs.put("charshift", inputVal);

        flowFile.putAttributes(flowFileAttrs);
        return flowFile;
    }

}
