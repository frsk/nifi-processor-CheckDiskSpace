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
package no.xxd.processors.CheckDiskSpace;

import java.util.concurrent.TimeUnit;
import java.io.File;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"disk"})
@CapabilityDescription("Stop processing flow files if a given path has free space below a given limit (in %)")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CheckDiskSpace extends AbstractProcessor {

    private static long last_check = 0L;
    private static boolean flowfile_space_available = true;

    public static final PropertyDescriptor INTERVAL = new PropertyDescriptor.Builder()
	.name("Interval")
        .description("Number of seconds between filesystem usage checks")
        .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
        .required(true)
        .defaultValue("30 secs")
        .build();

    public static final PropertyDescriptor PROP_PATH = new PropertyDescriptor
            .Builder().name("Path")
            .displayName("Path")
            .description("Path to check for available disk space")
            .required(true)
			.defaultValue("/")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .build();

    public static final PropertyDescriptor PROP_LIMIT = new PropertyDescriptor
            .Builder().name("Limit")
            .displayName("Limit")
            .description("Limit (% of free space)")
            .required(true)
			.defaultValue("10")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Flows that are successfully processed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_PATH);
        descriptors.add(PROP_LIMIT);
        descriptors.add(INTERVAL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    public static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            /* do nothing */
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final Integer interval = context.getProperty(INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        Integer limit = context.getProperty(PROP_LIMIT).evaluateAttributeExpressions(flowFile).asInteger();
        String path = context.getProperty(PROP_PATH).evaluateAttributeExpressions(flowFile).getValue();
        long current_epoch = System.currentTimeMillis();
        if ((current_epoch - last_check) >= interval) {
            last_check = current_epoch;
            int freespace = getFreeSpace(path);
            if (freespace < limit) {
                if (flowfile_space_available) {
                    getLogger().warn("Path " + path + " has less than " + limit + "% available.");
                }
                flowfile_space_available = false;
            } else {
                flowfile_space_available = true;
            }
        }
        if (flowfile_space_available) {
            session.transfer(flowFile, REL_SUCCESS);
        } else {
            session.rollback(false);
            CheckDiskSpace.sleepQuietly(5000L);
        }
    }
    public int getFreeSpace(String path) {
        File fs;
        fs = new File(path);
        Float total;
        Float free;
        int freepct;

        total = new Float(fs.getTotalSpace());
        free = new Float(fs.getUsableSpace());
        freepct = (int)((free/total)*100);

        return freepct;
    }

}
