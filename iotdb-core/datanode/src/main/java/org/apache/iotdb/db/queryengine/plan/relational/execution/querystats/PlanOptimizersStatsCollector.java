/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.execution.querystats;

import org.apache.iotdb.db.queryengine.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PlanOptimizersStatsCollector {
  private final Map<Class<?>, QueryPlanOptimizerStats> stats = new ConcurrentHashMap<>();
  private final int queryReportedRuleStatsLimit;

  public PlanOptimizersStatsCollector(int queryReportedRuleStatsLimit) {
    this.queryReportedRuleStatsLimit = queryReportedRuleStatsLimit;
  }

  public void recordRule(Rule<?> rule, boolean invoked, boolean applied, long elapsedNanos) {
    if (invoked) {
      statsForClass(rule.getClass()).record(elapsedNanos, applied);
    }
  }

  public void recordOptimizer(PlanOptimizer planOptimizer, long duration) {
    statsForClass(planOptimizer.getClass()).record(duration, true);
  }

  public void recordFailure(Rule<?> rule) {
    statsForClass(rule.getClass()).recordFailure();
  }

  public void recordFailure(PlanOptimizer rule) {
    statsForClass(rule.getClass()).recordFailure();
  }

  public List<QueryPlanOptimizerStatistics> getTopRuleStats() {
    return getTopRuleStats(queryReportedRuleStatsLimit);
  }

  public List<QueryPlanOptimizerStatistics> getTopRuleStats(int limit) {
    return stats.values().stream()
        .map(QueryPlanOptimizerStats::snapshot)
        .sorted(Comparator.comparing(QueryPlanOptimizerStatistics::totalTime).reversed())
        .limit(limit)
        .collect(toImmutableList());
  }

  public void add(PlanOptimizersStatsCollector other) {
    other.stats.forEach((key, value) -> statsForClass(key).merge(value));
  }

  private QueryPlanOptimizerStats statsForClass(Class<?> clazz) {
    return stats.computeIfAbsent(clazz, key -> new QueryPlanOptimizerStats(key.getCanonicalName()));
  }

  public static PlanOptimizersStatsCollector createPlanOptimizersStatsCollector() {
    return new PlanOptimizersStatsCollector(Integer.MAX_VALUE);
  }
}
