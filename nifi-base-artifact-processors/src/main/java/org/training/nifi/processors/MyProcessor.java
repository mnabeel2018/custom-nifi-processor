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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso()
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    private final Logger LOGGER = LoggerFactory.getLogger(MyProcessor.class);

    private static final PropertyDescriptor SHIFT_LETTERS_BY_N = new PropertyDescriptor
            .Builder().name("shift-by")
            .displayName("Shift by")
            .description("The attribute 'charshift' in the input flowfile will have it's value shifted by the number of" +
                    " places specified this property. Value must be a number.")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor OPTIONAL_PROP = new PropertyDescriptor
            .Builder().name("optional_prop")
            .displayName("optional")
            .description("Unused property to demonstrate it doesn't need a value")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("On success")
            .build();

    private static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("On failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    /**
     * Initialise processor context, providing the list of properties it can take as well as the relationships it can have with other processors
     * @param context processor context
     */
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SHIFT_LETTERS_BY_N);
        descriptors.add(OPTIONAL_PROP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * char shifts the contents of the property "bitshift" in the input flowfile, by the number of places specified in the charshift property
     * @param context processor context
     * @param session session
     * @throws ProcessException
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        LOGGER.info("Triggering custom processor");
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            LOGGER.error("No flowfile provided");
            return;
        }

        String oldValue = flowFile.getAttribute("charshift");

        if (oldValue == null) {
            LOGGER.warn("charshift attribute is empty");
            session.transfer(flowFile, FAILURE_RELATIONSHIP);
            return;
        }

        int shift = Integer.parseInt(context.getProperty("shift-by").getValue());

        LOGGER.info("shifting the chars of: {} by {} places", oldValue, shift);
        String newValue = charShift(oldValue, shift);
        LOGGER.info("Shifted string {}",newValue);
        session.putAttribute(flowFile, "charshift", newValue);

        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }

    private String charShift(String input, int shift) {
        StringBuilder result = new StringBuilder(input);
        for (int i=0; i<input.length(); i++) {
            char newChar = (char) (input.charAt(i) + shift);
            result.setCharAt(i, newChar);
        }
        return result.toString();
    }
}
