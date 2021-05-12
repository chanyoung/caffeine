/*
 * Copyright 2020 Ben Manes. All Rights Reserved.
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
 * @author park910113@gmail.com (Chanyoung Park)
 */
@PolicySpec(name = "irr.ClockProPebble")
public final class ClockProPebblePolicy implements KeyOnlyPolicy {
  final Long2ObjectMap<Node> data;
  final PolicyStats policyStats;
  final int maximumSize;

  Node handHot;
  Node handCold;
  Node handTest;

  int sizeHot;
  int sizeCold;
  int sizeTest;
  int coldTarget;

  public ClockProPebblePolicy(Config config) {
    BasicSettings settings = new BasicSettings(config);
    this.policyStats = new PolicyStats(name());
    this.maximumSize = Ints.checkedCast(settings.maximumSize());
    this.data = new Long2ObjectOpenHashMap<>();
    this.handHot = this.handCold = this.handTest = null;
    this.coldTarget = maximumSize;
  }

  @Override
  public void record(long key) {
    policyStats.recordOperation();

    Node node = data.get(key);
    if (node == null) {
      onMiss(key);
    } else if (node.status == Status.NON_RESIDENT) {
      onNonResidentHit(node);
    } else if (node.status.isResidentCold() || node.status == Status.HOT) {
      onResidentHit(node);
    } else {
      throw new IllegalStateException();
    }
  }

  private void onMiss(long key) {
    // When a page is accessed for the first time, it is added to the head of the inactive list,
    // slides every existing inactive page towards the tail by one slot, and pushes the current
    // tail page out of memory.
    Node node = new Node(key, Status.COLD_IN_TEST);
    moveToHead(node);
    policyStats.recordMiss();
    data.put(key, node);
    sizeCold++;
    evict();
  }

  private void onResidentHit(Node node) {
    // When a page is accessed for the second time, it is promoted to the active list, shrinking the
    // inactive list by one slot.  This also slides all inactive pages that were faulted into the
    // cache more recently than the activated page towards the tail of the inactive list.
    policyStats.recordHit();
    node.marked = true;
  }

  private void onNonResidentHit(Node node) {
    // So when a refault distance of (R - E) is observed and there are at least (R - E) active
    // pages, the refaulting page is activated optimistically in the hope that (R - E) active pages
    // are actually used less frequently than the refaulting page - or even not used at all anymore.
    adjustColdTarget(+1);

    sizeTest--;
    sizeHot++;
    node.status = Status.HOT;
    moveToHead(node);

    policyStats.recordMiss();
    evict();
  }

  private void moveToHead(Node node) {
    if (handHot == node) {
      handHot = handHot.next;
      if (handHot == node) {
        handHot = null;
      }
    }
    if (handCold == node) {
      handCold = handCold.next;
      if (handCold == node) {
        handCold = null;
      }
    }
    if (handTest == node) {
      handTest = handTest.next;
      if (handTest == node) {
        handTest = null;
      }
    }

    node.prev.next = node.next;
    node.next.prev = node.prev;
    node.next = node.prev = node;

    if (node.status == Status.HOT) {
      if (handHot != null) {
        node.next = handHot;
        node.prev = handHot.prev;
        node.next.prev = node;
        node.prev.next = node;
      } else {
        handHot = node;
      }
    } else if (node.status.isResidentCold()) {
      if (handCold != null) {
        node.next = handCold;
        node.prev = handCold.prev;
        node.next.prev = node;
        node.prev.next = node;
      } else {
        handCold = node;
      }
    } else if (node.status == Status.NON_RESIDENT) {
      if (handTest != null) {
        node.next = handTest;
        node.prev = handTest.prev;
        node.next.prev = node;
        node.prev.next = node;
      } else {
        handTest = node;
      }
    } else {
      throw new IllegalStateException("invalid status");
    }
  }

  private void evict() {
    // When a page is finally evicted from memory, the number of inactive pages accessed while the
    // page was in cache is at least the number of page slots on the inactive list.
    while (maximumSize < sizeCold + sizeHot) {
      if (coldTarget < sizeCold) {
        runHandCold();
      } else {
        runHandHot();
      }
    }
    prune();
  }

  private void prune() {
    // Approximate a reasonable limit for the nodes containing shadow entries. We don't need to keep
    // more shadow entries than possible pages on the active list, since refault distances bigger
    // than that are dismissed.
//    while (maximumSize - coldTarget < sizeTest) {
    while (maximumSize - sizeCold < sizeTest) {
      runHandTest();
    }
  }

  private void runHandCold() {
    checkState(sizeCold > 0 && handCold != null);
    checkState(handCold.status.isResidentCold());

    Node victim = handCold;
    handCold = handCold.next;
    if (victim.marked) {
      victim.marked = false;
      // If its bit is set and it is in its test period, we turn the cold page into a hot page,
      // and ask HAND for its actions, because an access during the test period indicates a
      // competitively small reuse distance. If its bit is set but it is not in its test period,
      // there are no status change as well as HAND actions. Its reference bit is reset, and we
      // move it to the list head.
      if (victim.status.isInTest()) {
        sizeCold--;
        sizeHot++;
        victim.status = Status.HOT;
        moveToHead(victim);
      } else {
        victim.status = Status.COLD_IN_TEST;
      }
    } else {
      // If the reference bit of the cold page currently pointed to by handCold is unset, we replace
      // the cold page for a free space. If the replaced cold page is in its test period, then it
      // will remain in the list as a non-resident cold page until it runs out of its test period.
      // If the replaced cold page is not in its test period, we move it out of the clock.
      if (victim.status.isInTest()) {
        sizeCold--;
        sizeTest++;
        victim.status = Status.NON_RESIDENT;
        moveToHead(victim);
      } else {
        sizeCold--;
        data.remove(victim.key);
        victim.remove();
      }
    }

    if (sizeCold == 0) {
      handCold = null;
    }
  }

  private void runHandHot() {
    checkState(sizeHot > 0 && handHot != null);
    checkState(handHot.status == Status.HOT);

    Node victim = handHot;
    handHot = handHot.next;
    if (victim.marked) {
      victim.marked = false;
    } else {
      sizeHot--;
      sizeCold++;
      victim.status = Status.COLD;
      moveToHead(victim);
    }

    if (sizeHot == 0) {
      handHot = null;
    }
  }

  private void runHandTest() {
    checkState(sizeTest > 0 && handTest != null);
    checkState(handTest.status == Status.NON_RESIDENT);

    Node victim = handTest;
    handTest = handTest.next;
    sizeTest--;
    data.remove(victim.key);
    victim.remove();

    adjustColdTarget(-1);

    if (sizeTest == 0) {
      handTest = null;
    }
  }

  private void adjustColdTarget(int n) {
    coldTarget += n;
    if (coldTarget < 0) {
      coldTarget = 0;
    } else if (coldTarget > maximumSize) {
      coldTarget = maximumSize;
    }
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void finished() {
//    printClock();
    int hot = (int) data.values().stream()
        .filter(node -> node.status == Status.HOT)
        .count();
    int residentCold = (int) data.values().stream()
        .filter(node -> node.status.isResidentCold())
        .count();
    int nonResident = (int) data.values().stream()
        .filter(node -> node.status == Status.NON_RESIDENT)
        .count();

    checkState(hot == sizeHot,
        "Active: expected %s but was %s", sizeHot, hot);
    checkState(residentCold == sizeCold,
        "Inactive: expected %s but was %s", sizeCold, residentCold);
    checkState(nonResident <= maximumSize,
        "NonResident: expected %s less than %s", nonResident, maximumSize);
    checkState((sizeCold + sizeHot + sizeTest) == data.size(),
            "sizeCold: %s, sizeHot %s, sizeTest %s, data.size %s", sizeCold, sizeHot, sizeTest, data.size());
    checkState((sizeCold + sizeHot) <= maximumSize);
  }

  enum Status {
    HOT,
    COLD,
    COLD_IN_TEST,
    NON_RESIDENT;

    public boolean isResidentCold() {
      return this == COLD || this == COLD_IN_TEST;
    }
    public boolean isInTest() {
      return this == COLD_IN_TEST || this == NON_RESIDENT;
    }
  }

  static final class Node {
    final long key;

    Status status;
    Node prev;
    Node next;
    boolean marked;

    public Node(long key, Status status) {
      this.status = status;
      this.key = key;
      this.prev = this.next = this;
    }

    /** Removes the node from the list. */
    public void remove() {
      prev.next = next;
      next.prev = prev;
      prev = next = this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("status", status)
          .add("marked", marked)
          .toString();
    }
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    if (handHot != null) {
      System.out.println("** handHot **");
      System.out.println(handHot.toString());
      for (Node n = handHot.next; n != handHot; n = n.next) {
        System.out.println(n.toString());
      }
    }
    if (handCold != null) {
      System.out.println("** handCold **");
      System.out.println(handCold.toString());
      for (Node n = handCold.next; n != handCold; n = n.next) {
        System.out.println(n.toString());
      }
    }
    if (handTest != null) {
      System.out.println("** handTest **");
      System.out.println(handTest.toString());
      for (Node n = handTest.next; n != handTest; n = n.next) {
        System.out.println(n.toString());
      }
    }
  }
}
