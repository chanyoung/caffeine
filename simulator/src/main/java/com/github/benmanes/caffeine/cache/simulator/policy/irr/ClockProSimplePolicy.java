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
 * This implementation works exactly like ClockPro, but pursues the simplicity of the code.
 * It divides a single list of ClockPro into three lists: hot, cold, and non-resident.
 * For maintaining a test period of each entries, it uses epoch.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @author park910113@gmail.com (Chanyoung Park)
 */
@PolicySpec(name = "irr.ClockProSimple")
public final class ClockProSimplePolicy implements KeyOnlyPolicy {
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
  private int reaccessedCount;

  // Target number of resident cold pages (adaptive):
  //  - increases when test page gets a hit
  //  - decreases when test page is removed
  private int coldTarget;

  // Enable to print out the internal state
  static final boolean debug = false;

  public ClockProSimplePolicy(Config config) {
    BasicSettings settings = new BasicSettings(config);
    this.maxSize = Ints.checkedCast(settings.maximumSize());
    this.policyStats = new PolicyStats(name());
    this.data = new Long2ObjectOpenHashMap<>();
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
      "Cold: expected %s but was %s", sizeCold, cold);
    checkState(hot == sizeHot,
      "Hot: expected %s but was %s", sizeHot, hot);
    checkState(nonResident == sizeNR,
      "NonResident: expected %s but was %s", sizeNR, nonResident);
    checkState(data.size() == (cold + hot + nonResident));
    checkState(cold + hot <= maxSize);
    checkState(nonResident <= maxSize);
  }

  // To know the order of entries, epoch is used. The epoch is incremented by 1 when a new entry is inserted,
  // or when an existing entry is re-accessed and moved to the head. The epoch is used to determine whether
  // an entry's test period has expired or not.
  private long epoch() {
    return policyStats.missCount() + reaccessedCount;
  }

  @Override
  public void record(long key) {
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
    policyStats.recordOperation();
    policyStats.recordHit();
    node.marked = true;
  }

  private void onMiss(long key) {
    policyStats.recordOperation();
    policyStats.recordMiss();
    Node node = new Node(key, epoch());
    node.status = Status.COLD;
    node.link(headCold);
    data.put(key, node);
    sizeCold++;
    evict();
  }

  // Prune removes all non-resident entries whose test period has expired.
  private void prune() {
    while (sizeNR > 0 && !inTestPeriod(headNonResident.prev)) {
      scanNonResident();
    }
  }

  private void onNonResidentMiss(Node node) {
    policyStats.recordOperation();
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
    node.epoch = epoch();
    evict();
  }

  private void evict() {
    policyStats.recordEviction();
    while (maxSize < sizeCold + sizeHot) {
      if (sizeCold > 0) {
        scanCold();
      } else {
        scanHot(epoch());
      }
    }
    prune();
  }

  private boolean canPromote(Node candidate) {
    // Only entries in its test period can be considered to promote.
    if (!inTestPeriod(candidate)) {
      return false;
    }
    // The candidate cold entry is re-accessed during its test period, so we increment coldTarget.
    adjustColdTarget(+1);
    while (sizeHot > 0 && sizeHot >= maxSize - coldTarget) {
      // Failed to demote a hot node. Reject the promotion.
      if (!scanHot(candidate.epoch)) {
        return false;
      }
    }
    // Candidate's test period should not be expired while scanning hot entries.
    return inTestPeriod(candidate);
  }

  private void scanCold() {
    policyStats.recordOperation();
    Node victim = headCold.prev;
    victim.unlink();
    if (victim.marked) {
      // If its bit is set and it is in its test period, we consider this entry as a candidate for promotion to hot,
      // because an access during the test period indicates a competitively small reuse distance. We scans hot entries
      // for finding a hot entry with a longer reuse distance than the candidate cold entry. If we failed to find a
      // hot entry with a longer reuse distance than the candidate, or the candidate test period is expired, we reset
      // its reference bit and move it to the list head, and grant a new test period by renewing the epoch.
      victim.marked = false;
      if (canPromote(victim)) {
        victim.status = Status.HOT;
        victim.link(headHot);
        sizeCold--;
        sizeHot++;
      } else {
        victim.link(headCold);
      }
      reaccessedCount++;
      victim.epoch = epoch();
    } else {
      // If the reference bit of the cold entry is unset, we replace the cold entry for a free space. If the replaced
      // cold entry is in its test period, then it will remain in the list as a non-resident cold entry until it runs
      // out of its test period. If the replaced cold entry is not in its test period, we move it out of the clock.
      sizeCold--;
      if (inTestPeriod(victim)) {
        victim.status = Status.NR;
        victim.link(headNonResident);
        sizeNR++;
      } else {
        data.remove(victim.key);
      }
      // We keep track the number of non-resident cold entries. Once the number exceeds the limit, we terminate the
      // test period of the oldest non-resident entry.
      while (sizeNR > maxSize) {
        scanNonResident();
      }
    }
  }

  // ScanHot demotes a hot entry between the oldest hot entry's epoch and the given epoch.
  // If the demotion was successful it returns true, otherwise it returns false.
  private boolean scanHot(long epoch) {
    policyStats.recordOperation();
    while (sizeHot > 0) {
      Node victim = headHot.prev;
      if (victim.epoch > epoch) {
        break;
      }
      victim.unlink();
      // If the reference bit of the hot entry is unset, we can simply change its status and link to the head of cold
      // list. However, if the bit is set, which indicates the entry has been re-accessed, we spare this entry, reset
      // its reference bit and keep it as a hot entry. This is because the actual access time of the hot entry could be
      // earlier than the cold entry. Then we move the hand forward and do the same on the hot entries with their bits
      // set until the hand encounters a hot entry with a reference bit of zero. Then the hot entry turns into a cold
      // page.
      if (victim.marked) {
        victim.marked = false;
        victim.link(headHot);
        reaccessedCount++;
        victim.epoch = epoch();
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

  private void scanNonResident() {
    policyStats.recordOperation();
    // We terminate the test period of the non-resident cold page, and also remove it from the clock. Because the cold
    // page has used up its test period without a re-access and has no chance to turn into a hot page with its next
    // access.
    Node victim = headNonResident.prev;
    victim.unlink();
    data.remove(victim.key);
    sizeNR--;
    // If a cold page passes its test period without a re-access, we decrement coldTarget.
    adjustColdTarget(-1);
  }

  private void adjustColdTarget(int n) {
    coldTarget += n;
    if (coldTarget < 0) {
      coldTarget = 0;
    } else if (coldTarget > maxSize) {
      coldTarget = maxSize;
    }
  }

  // Test period should be set as the largest recency of the hot entry. If an entry is
  // older than the largest recency of the hot entry, the test period has expired.
  private boolean inTestPeriod(Node node) {
    return sizeHot == 0 || node.epoch > headHot.prev.epoch;
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    if (sizeCold > 0) {
      System.out.println("** CLOCK-Pro list COLD HEAD (small recency) **");
      for (Node n = headCold.next; n != headCold; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list COLD TAIL (large recency) **");
    }
    if (sizeHot > 0) {
      System.out.println("** CLOCK-Pro list HOT HEAD (small recency) **");
      for (Node n = headHot.next; n != headHot; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro list HOT TAIL (large recency) **");
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
    long epoch;

    Status status;
    Node prev;
    Node next;

    boolean marked;

    public Node(long key, long epoch) {
      this.key = key;
      prev = next = this;
      this.epoch = epoch;
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
          .add("epoch", epoch)
          .toString();
    }
  }
}
