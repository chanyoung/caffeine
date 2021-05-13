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

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.KeyOnlyPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * The ClockPro algorithm. This algorithm differs from LIRS by replacing the LRU stacks with Clock
 * (Second Chance) policy. This allows cache hits to be performed concurrently at the cost of a
 * global lock on a miss and a worst case O(n) eviction when the queue is scanned.
 * <p>
 * ClockPro uses three hands that scan the queue. The hot hand points to the largest recency, the
 * cold hand to the cold entry furthest from the hot hand, and the test hand to the last cold entry
 * in the test period. This policy is adaptive by adjusting the percentage of hot and cold entries
 * that may reside in the cache. It uses non-resident (ghost) entries to retain additional history,
 * which are removed during the test hand's scan. The algorithm is explained by the authors in
 * <a href="http://www.ece.eng.wayne.edu/~sjiang/pubs/papers/jiang05_CLOCK-Pro.pdf">CLOCK-Pro: An
 * Effective Improvement of the CLOCK Replacement</a> and
 * <a href="http://www.slideshare.net/huliang64/clockpro">Clock-Pro: An Effective Replacement in OS
 * Kernel</a>.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @author park910113@gmail.com (Chanyoung Park)
 */
@PolicySpec(name = "irr.ClockProR")
public final class ClockProRPolicy implements KeyOnlyPolicy {
  private final Long2ObjectMap<Node> data;
  private final PolicyStats policyStats;

  private final Node headHot;
  private final Node headCold;
  private final Node headNonResident;

  // Maximum number of resident pages (hot + resident cold)
  private final int maxSize;

  private int sizeHot;
  private int sizeCold;
  private int sizeNR;

  // Target number of resident cold pages (adaptive):
  //  - increases when test page gets a hit
  //  - decreases when test page is removed
  private int coldTarget;

  // Enable to print out the internal state
  static final boolean debug = false;

  public ClockProRPolicy(Config config) {
    BasicSettings settings = new BasicSettings(config);
    this.maxSize = Ints.checkedCast(settings.maximumSize());
    this.policyStats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();
    this.sizeHot = this.sizeCold = this.sizeNR = 0;
    this.coldTarget = 0;
    this.headHot = new Node(Long.MIN_VALUE, -1);
    this.headCold = new Node(Long.MIN_VALUE, -1);
    this.headNonResident = new Node(Long.MIN_VALUE, -1);
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void finished() {
    if (debug) {
      printClock();
    }
    int cold = (int) data.values().stream()
      .filter(node -> node.status == Status.COLD)
      .count();
    int hot = (int) data.values().stream()
      .filter(node -> node.status == Status.HOT)
      .count();
    int nonResident = (int) data.values().stream()
      .filter(node -> node.status == Status.NR)
      .count();

    checkState(cold == sizeCold,
      "Active: expected %s but was %s", sizeCold, cold);
    checkState(hot == sizeHot,
      "Inactive: expected %s but was %s", sizeHot, hot);
    checkState(nonResident == sizeNR,
      "NonResident: expected %s but was %s", sizeNR, nonResident);
    checkState(data.size() == (cold + hot + nonResident));
    checkState(cold + hot <= maxSize);
    checkState(nonResident <= maxSize);
  }

  private long currentAge() {
    return policyStats.missCount();
  }

  @Override
  public void record(long key) {
    policyStats.recordOperation();
    Node node = data.get(key);
    if (node == null) {
      onMiss(key);
    } else if (node.status == Status.HOT || node.status == Status.COLD) {
      onHit(node);
    } else if (node.status == Status.NR) {
      onNonResidentMiss(node);
    } else {
      throw new IllegalStateException();
    }
  }

  private void onHit(Node node) {
    policyStats.recordHit();
    node.marked = true;
  }

  private void onMiss(long key) {
    policyStats.recordMiss();
    Node node = new Node(key, currentAge());
    node.status = Status.COLD;
    node.link(headCold);
    data.put(key, node);
    sizeCold++;
    evict();
  }

  private void prune() {
    while (sizeNR > 0 && !inTestPeriod(headNonResident.prev)) {
      runHandNR();
    }
  }

  private void onNonResidentMiss(Node node) {
    policyStats.recordMiss();
    node.unlink();
    sizeNR--;
    if (canPromote(node)) {
      node.status = Status.HOT;
      node.link(headHot);
      sizeHot++;
    } else {
      node.status = Status.COLD;
      node.link(headCold);
      sizeCold++;
    }
    node.age = currentAge();
    evict();
  }

  private void evict() {
    policyStats.recordEviction();
    while (maxSize < sizeCold + sizeHot) {
      if (sizeCold > 0) {
        runHandCold();
      } else {
        runHandHot(currentAge());
      }
    }
    prune();
  }

  private boolean canPromote(Node candidate) {
    if (!inTestPeriod(candidate)) {
      return false;
    }
    adjustColdTarget(+1);
    while (sizeHot > 0 && sizeHot >= maxSize - coldTarget) {
      // Failed to demote a hot node. Reject the promotion.
      if (!runHandHot(candidate.age)) {
        return false;
      }
    }
    return inTestPeriod(candidate);
  }

  private void runHandCold() {
    Node victim = headCold.prev;
    victim.unlink();
    if (victim.marked) {
      victim.marked = false;
      if (canPromote(victim)) {
        victim.age = currentAge();
        victim.status = Status.HOT;
        victim.link(headHot);
        sizeCold--;
        sizeHot++;
      } else {
        victim.age = currentAge();
        victim.link(headCold);
      }
    } else {
      sizeCold--;
      if (inTestPeriod(victim)) {
        victim.status = Status.NR;
        victim.link(headNonResident);
        sizeNR++;
      } else {
        data.remove(victim.key);
      }
      while (sizeNR > maxSize) {
        runHandNR();
      }
    }
  }

  private boolean runHandHot(long age) {
    while (sizeHot > 0) {
      Node victim = headHot.prev;
      if (victim.age > age) {
        break;
      }
      victim.unlink();
      if (victim.marked) {
        victim.marked = false;
        victim.age = currentAge();
        victim.link(headHot);
      } else {
        victim.status = Status.COLD;
        victim.link(headCold);
        sizeHot--;
        sizeCold++;
        return true;
      }
    }
    return false;
  }

  private void runHandNR() {
    Node victim = headNonResident.prev;
    victim.unlink();
    data.remove(victim.key);
    adjustColdTarget(-1);
    sizeNR--;
  }

  private void adjustColdTarget(int n) {
    coldTarget += n;
    if (coldTarget < 0) {
      coldTarget = 0;
    } else if (coldTarget > maxSize) {
      coldTarget = maxSize;
    }
  }

  private boolean inTestPeriod(Node node) {
    return sizeHot == 0 || node.age > headHot.prev.age;
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    if (sizeCold > 0) {
      System.out.println("** CLOCK-Pro list COLD HEAD (small recency) **");
      for (Node n = headCold.next; n != headCold; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list COLD TAIL (large recency) **");
      System.out.println("");
    }
    if (sizeHot > 0) {
      System.out.println("** CLOCK-Pro list HOT HEAD (small recency) **");
      for (Node n = headHot.next; n != headHot; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list HOT TAIL (large recency) **");
      System.out.println("");
    }
    if (sizeNR > 0) {
      System.out.println("** CLOCK-Pro list NR HEAD (small recency) **");
      for (Node n = headNonResident.next; n != headNonResident; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list NR TAIL (large recency) **");
    }
  }

  enum Status {
    HOT, COLD, NR,
  }

  final class Node {
    final long key;
    long age;

    Status status;
    Node prev;
    Node next;

    boolean marked;

    public Node(long key, long age) {
      this.key = key;
      prev = next = this;
      this.age = age;
      this.status = Status.COLD;
    }

    public void unlink() {
      prev.next = next;
      next.prev = prev;
      prev = next = this;
    }

    public void link(Node node) {
      prev = node;
      next = node.next;
      prev.next = this;
      next.prev = this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("marked", marked)
          .add("type", status)
          .add("age", age)
          .toString();
    }
  }
}
