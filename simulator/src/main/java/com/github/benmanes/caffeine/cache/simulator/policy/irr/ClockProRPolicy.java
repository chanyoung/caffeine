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

  private Node handHot;
  private Node handCold;
  private Node handNR;

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
    this.handHot = this.handCold = this.handNR = null;
    this.sizeHot = this.sizeCold = this.sizeNR = 0;
    this.coldTarget = 0;
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

  private long currentVirtualTime() {
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
    Node node = new Node(key, currentVirtualTime());
    data.put(key, node);
    appendToCold(node);
    sizeCold++;
    evict();
  }

  private void prune() {
    while (handNR != null && !inTestPeriod(handNR)) {
      runHandNR();
    }
  }

  private void onNonResidentMiss(Node node) {
    policyStats.recordMiss();
    if (node == handNR) {
      if (handNR.next == handNR) {
        handNR = null;
      } else {
        handNR = handNR.next;
      }
    }
    sizeNR--;
    if (canPromote(node)) {
      appendToHot(node);
      sizeHot++;
    } else {
      appendToCold(node);
      sizeCold++;
    }
    node.age = currentVirtualTime();
    evict();
  }

  private void appendToCold(Node node) {
    node.removeFromClock();
    if (handCold != null) {
      node.link(handCold);
    } else {
      handCold = node;
    }
    node.status = Status.COLD;
  }

  private void appendToHot(Node node) {
    node.removeFromClock();
    if (handHot != null) {
      node.link(handHot);
    } else {
      handHot = node;
    }
    node.status = Status.HOT;
  }

  private void appendToNR(Node node) {
    node.removeFromClock();
    if (handNR != null) {
      node.link(handNR);
    } else {
      handNR = node;
    }
    node.status = Status.NR;
  }

  private void evict() {
    policyStats.recordEviction();
    while (maxSize < sizeCold + sizeHot) {
      if (sizeCold > 0) {
        runHandCold();
      } else {
        runHandHot(currentVirtualTime());
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
    checkState(handCold.status == Status.COLD);
    if (handCold.marked) {
      if (inTestPeriod(handCold)) {
        if (canPromote(handCold)) {
          Node node = handCold;
          if (handCold.next == handCold) {
            handCold = null;
          } else {
            handCold = handCold.next;
          }
          node.age = currentVirtualTime();
          appendToHot(node);
          sizeCold--;
          sizeHot++;
        } else {
          handCold.age = currentVirtualTime();
          handCold.marked = false;
          handCold = handCold.next;
        }
      } else {
        handCold.age = currentVirtualTime();
        handCold.marked = false;
        handCold = handCold.next;
      }
    } else {
      Node node = handCold;
      if (handCold.next == handCold) {
        handCold = null;
      } else {
        handCold = handCold.next;
      }
      sizeCold--;
      if (inTestPeriod(node)) {
        appendToNR(node);
        sizeNR++;
      } else {
        node.removeFromClock();
        data.remove(node.key);
      }
      while (sizeNR > maxSize) {
        runHandNR();
      }
    }
  }

  private boolean runHandHot(long age) {
    checkState(handHot.status == Status.HOT);
    while (handHot.age <= age) {
      if (handHot.marked) {
        handHot.marked = false;
        handHot.age = currentVirtualTime();
        handHot = handHot.next;
      } else {
        Node node = handHot;
        if (handHot.next == handHot) {
          handHot = null;
        } else {
          handHot = handHot.next;
        }
        appendToCold(node);
        sizeHot--;
        sizeCold++;
        return true;
      }
    }
    return false;
  }

  private void runHandNR() {
    Node node = handNR;
    if (handNR.next == handNR) {
      handNR = null;
    } else {
      handNR = handNR.next;
    }
    node.removeFromClock();
    data.remove(node.key);
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
    return handHot == null || node.age > handHot.age;
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    if (handCold != null) {
      System.out.println("** CLOCK-Pro list COLD HEAD (large recency) **");
      System.out.println(handCold.toString());
      for (Node n = handCold.next; n != handCold; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list COLD TAIL (small recency) **");
      System.out.println("");
    }
    if (handHot != null) {
      System.out.println("** CLOCK-Pro list HOT HEAD (large recency) **");
      System.out.println(handHot.toString());
      for (Node n = handHot.next; n != handHot; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list HOT TAIL (small recency) **");
      System.out.println("");
    }
    if (handNR != null) {
      System.out.println("** CLOCK-Pro list NR HEAD (large recency) **");
      System.out.println(handNR.toString());
      for (Node n = handNR.next; n != handNR; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list NR TAIL (small recency) **");
    }
  }

  // +----- Status ------+- Resident -+- In Test -+
  // |               HOT |       TRUE |     FALSE |
  // |              COLD |       TRUE |     FALSE |
  // |      COLD_IN_TEST |       TRUE |      TRUE |
  // |                NR |      FALSE |      TRUE |
  // +-------------------+------------+-----------+
  enum Status {
    HOT, COLD, NR, OUT_OF_CLOCK,
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
      this.status = Status.OUT_OF_CLOCK;
    }

    public void unlink() {
      prev.next = next;
      next.prev = prev;
      prev = next = this;
    }

    public void link(Node node) {
      next = node;
      prev = node.prev;
      prev.next = this;
      next.prev = this;
    }

    public void removeFromClock() {
      checkState(handCold != this);
      checkState(handHot != this);
      checkState(handNR != this);
      this.unlink();
      marked = false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("marked", marked)
          .add("type", status)
          .add("age", age)
          .toString());
      if (this == handHot) {
        sb.append(" <--[ HAND_HOT ]");
      }
      if (this == handCold) {
        sb.append(" <--[ HAND_COLD ]");
      }
      if (this == handNR) {
        sb.append(" <--[ HAND_NR ]");
      }
      return sb.toString();
    }
  }
}
