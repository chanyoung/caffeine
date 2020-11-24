/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache.simulator.policy.irr;

import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.KeyOnlyPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public final class WorkloadAnalyzer implements KeyOnlyPolicy {
  private final PolicyStats policyStats;
  private final List<Long> ops;
  private final Long2ObjectMap<Node> data;
  private final Map<Integer, Integer> irr;

  public WorkloadAnalyzer(Config config) {
    this.policyStats = new PolicyStats("irr.WorkloadAnalyzer");
    this.data = new Long2ObjectOpenHashMap<>();
    this.ops = new ArrayList<>();
    this.irr = new HashMap<>();
  }

  public static Set<Policy> policies(Config config) {
    return ImmutableSet.of(new WorkloadAnalyzer(config));
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  private final Map<Long, Integer> keyMap = new HashMap<>();

  @Override
  public void record(long key) {
    ops.add(key);

    Node node = data.get(key);
    if (node == null) {
      node = new Node(key, policyStats.operationCount());
      data.put(key, node);
    } else {
      node.addOps(policyStats.operationCount());
    }

    policyStats.recordOperation();
    if (policyStats.operationCount() >= Integer.MAX_VALUE) {
      throw new IllegalStateException("analyzer limit");
    }
  }

  @Override
  public void finished() {
    checkState(ops.size() == (int) policyStats.operationCount());

    System.out.println("Total requests: " + ops.size());
    System.out.println("Unique pages: " + data.size());
    analyzeRequestPerPages();
    analyzeIrr();
  }

  private void analyzeIrr() {
    data.values().forEach(this::analyzeIrr);
    System.out.println("[ irr distance ]");
    irr.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
      System.out.println(e.getKey() + ": " + e.getValue());
    });
  }

  private void analyzeIrr(Node node) {
    if (node.count == 1) {
      return;
    }
    long from = node.ops.get(0);
    for (long to : node.ops) {
      int irr = calcIrrDistance(from, to);
      if (irr > 0) {
        int count = this.irr.getOrDefault(irr, 0);
        count++;
        this.irr.put(irr, count);
      }
      from = to;
    }
  }

  private int calcIrrDistance(long from, long to) {
    if (from >= to) {
      return 0;
    }
    checkState(ops.get((int) from).equals(ops.get((int) to)));
    Map<Long, Integer> uniquePages = new HashMap<>();
    for (int i = (int) from + 1; i < to; i++) {
      long key = ops.get(i);
      uniquePages.put(key, 1);
    }
    return uniquePages.size();
  }

  private void analyzeRequestPerPages() {
    Map<Integer, Integer> counter = new HashMap<>();
    data.long2ObjectEntrySet().stream().iterator().forEachRemaining(e -> {
      int count = counter.getOrDefault(e.getValue().count, 0);
      count++;
      counter.put(e.getValue().count, count);
    });
    System.out.println("[ Request per pages ]");
    counter.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
      System.out.println(e.getKey() + ": " + e.getValue());
    });
  }

  final class Node {
    private long key;
    private int count;
    private List<Long> ops = new LinkedList<>();

    public Node(long key, long ops) {
      this.key = key;
      this.count = 1;
      this.ops.add(ops);
    }

    public void addOps(long ops) {
      this.ops.add(ops);
      this.count++;
    }
  }
}
