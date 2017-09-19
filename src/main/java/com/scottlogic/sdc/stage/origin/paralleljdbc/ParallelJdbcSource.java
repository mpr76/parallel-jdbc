/**
 * Copyright 2017 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scottlogic.sdc.stage.origin.paralleljdbc;

import com.scottlogic.sdc.stage.lib.sample.Errors;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This source is an example and does not actually read from anywhere.
 * It does however, generate generate a simple record with one field.
 */
public abstract class ParallelJdbcSource extends BasePushSource {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelJdbcSource.class);

    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    private Runnable createRunnable(final int id) {
        return () -> {
            BatchContext batchContext = getContext().startBatch();

        };
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    /** {@inheritDoc} */
    @Override
    public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
        ExecutorService executor = Executors.newFixedThreadPool(getNumberOfThreads());
        List<Future<Runnable>> futures = new ArrayList<>(getNumberOfThreads());

// Start the threads
        for (int i = 0; i < getNumberOfThreads(); i++) {
            Future future = executor.submit(createRunnable(i));
            futures.add(future);
        }

// Wait for execution end
        for (Future<Runnable> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Record generation threads have been interrupted", e.getMessage());
            }
        }
    }

}
