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
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.KeyOnlyPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Set;

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
public final class ClockProNewPolicy implements KeyOnlyPolicy {
  private final Long2ObjectMap<Node> data;
  private final PolicyStats policyStats;

  // We place all the accessed pages, either hot or cold, into one single list in the order of their
  // accesses. In the list, the pages with small recency are at the list head, and the pages with
  // large recency are at the list tail.
  private Node listHead;

  private Node hand;

  // Maximum number of resident pages (hot + resident cold)
  private final int maxSize;
  private final int maxNonResSize;

  private int sizeHot;
  private int sizeResCold;
  private int sizeNonResCold;
  private int sizeShortIrr;
  private int sizeFree;

  // Target number of resident cold pages (adaptive):
  //  - increases when test page gets a hit
  //  - decreases when test page is removed
  private int coldTarget;
  // {min,max}ResColdSize are boundary of coldTarget.
  private int minResColdSize;
  private int maxResColdSize;

  // Enable to print out the internal state
  static final boolean debug = false;

  public ClockProNewPolicy(Config config) {
    ClockProSettings settings = new ClockProSettings(config);
    this.maxSize = Ints.checkedCast(settings.maximumSize());
    this.maxNonResSize = (int) (maxSize * settings.nonResidentMultiplier());
    this.minResColdSize = (int) (maxSize * settings.percentMinCold());
    if (minResColdSize < settings.lowerBoundCold()) {
      minResColdSize = settings.lowerBoundCold();
    }
    this.maxResColdSize = (int) (maxSize * settings.percentMaxCold());
    if (maxResColdSize > maxSize - minResColdSize) {
      maxResColdSize = maxSize - minResColdSize;
    }
    this.policyStats = new PolicyStats("irr.ClockProNew");
    this.data = new Long2ObjectOpenHashMap<>();
    this.coldTarget = minResColdSize;
    this.listHead = this.hand = null;
    this.sizeFree = maxSize;
    checkState(minResColdSize <= maxResColdSize);
  }

  /**
   * Returns all variations of this policy based on the configuration parameters.
   */
  public static Set<Policy> policies(Config config) {
    return ImmutableSet.of(new ClockProNewPolicy(config));
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
  }

  @Override
  public void record(long key) {
    policyStats.recordOperation();
    Node node = data.get(key);
    if (node == null) {
      node = new Node(key);
      data.put(key, node);
      onMiss(node);
    } else if (node.isResident()) {
      onHit(node);
    } else {
      onMiss(node);
    }
  }

  private void onHit(Node node) {
    policyStats.recordHit();
    node.marked = true;
  }

  private void onMiss(Node node) {
    policyStats.recordMiss();
    if (sizeFree > minResColdSize) {
      onHotWarmupMiss(node);
    } else if (sizeFree > 0) {
      onColdWarmupMiss(node);
    } else {
      onFullMiss(node);
    }
  }

  /** Records a miss when the hot set is not full. */
  private void onHotWarmupMiss(Node node) {
    node.moveToHead(Status.HOT_SHORT_IRR);
  }

  /** Records a miss when the cold set is not full. */
  private void onColdWarmupMiss(Node node) {
    node.moveToHead(Status.COLD_RES_SHORT_IRR);
  }

  /** Records a miss when the hot and cold set are full. */
  private void onFullMiss(Node node) {
    if (node.status == Status.COLD_NON_RES) {
      onNonResidentFullMiss(node);
    } else if (node.status == Status.OUT_OF_CLOCK) {
      onOutOfClockFullMiss(node);
    } else {
      throw new IllegalStateException();
    }
  }

  private void onOutOfClockFullMiss(Node node) {
    evict();
    node.moveToHead(Status.COLD_RES_SHORT_IRR);
  }

  private void onNonResidentFullMiss(Node node) {
    evict();
    node.moveToHead(Status.COLD_RES_SHORT_IRR);
  }

  private void evict() {
    policyStats.recordEviction();
    checkState(sizeFree == 0);
    Node node = listHead.prev;
    node.removeFromClock();
    data.remove(node.key);
    checkState(sizeFree == 1);
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    System.out.println("** CLOCK-Pro list HEAD (small recency) **");
    System.out.println(listHead.toString());
    for (Node n = listHead.next; n != listHead; n = n.next) {
      System.out.println(n.toString());
    }
    System.out.println("** CLOCK-Pro list TAIL (large recency) **");
  }

  enum Status {
    HOT_LONG_IRR, HOT_SHORT_IRR,
    COLD_RES_LONG_IRR, COLD_RES_SHORT_IRR,
    COLD_NON_RES, OUT_OF_CLOCK,
  }

  final class Node {
    final long key;

    Status status;
    Node prev;
    Node next;

    boolean marked;

    public Node(long key) {
      this.key = key;
      prev = next = this;
      status = Status.OUT_OF_CLOCK;
    }

    public void moveToHead(Status status) {
      if (isInClock()) {
        removeFromClock();
      }
      if (listHead == null) {
        next = prev = this;
      } else {
        next = listHead;
        prev = listHead.prev;
        listHead.prev.next = this;
        listHead.prev = this;
      }
      setStatus(status);
      listHead = this;
    }

    public void removeFromClock() {
      if (this == listHead) {
        listHead = listHead.next;
      }
      if (this == hand) {
        hand = hand.prev;
      }
      prev.next = next;
      next.prev = prev;
      prev = next = this;
      setStatus(Status.OUT_OF_CLOCK);
      marked = false;
    }

    public void setStatus(Status status) {
      if (this.isResident()) { sizeFree++; }
      if (this.isShortIrr()) { sizeShortIrr--; }
      if (this.isResidentCold()) { sizeResCold--; }
      if (this.status == Status.COLD_NON_RES) { sizeNonResCold--; }
      if (this.isHot()) { sizeHot--; }
      this.status = status;
      if (this.isResident()) { sizeFree--; }
      if (this.isShortIrr()) { sizeShortIrr++; }
      if (this.isResidentCold()) { sizeResCold++; }
      if (this.status == Status.COLD_NON_RES) { sizeNonResCold++; }
      if (this.isHot()) { sizeHot++; }
    }

    boolean isShortIrr() {
      return status == Status.COLD_RES_SHORT_IRR || status == Status.HOT_SHORT_IRR;
    }
    boolean isResident() {
      return isResidentCold() || isHot();
    }
    boolean isResidentCold() {
      return status == Status.COLD_RES_LONG_IRR || status == Status.COLD_RES_SHORT_IRR;
    }
    boolean isCold() {
      return isResidentCold() || status == Status.COLD_NON_RES;
    }
    boolean isHot() {
      return status == Status.HOT_LONG_IRR || status == Status.HOT_SHORT_IRR;
    }
    boolean isInClock() {
      return status != Status.OUT_OF_CLOCK;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("marked", marked)
          .add("type", status)
          .toString());
      if (this == hand) {
        sb.append(" <--[ HAND ]");
      }
      return sb.toString();
    }
  }

  static final class ClockProSettings extends BasicSettings {
    public ClockProSettings(Config config) {
      super(config);
    }
    public int lowerBoundCold() {
      return config().getInt("clockpro.lower-bound-resident-cold");
    }
    public double percentMinCold() {
      return config().getDouble("clockpro.percent-min-resident-cold");
    }
    public double percentMaxCold() {
      return config().getDouble("clockpro.percent-max-resident-cold");
    }
    public double nonResidentMultiplier() {
      return config().getDouble("clockpro.non-resident-multiplier");
    }
  }
}
