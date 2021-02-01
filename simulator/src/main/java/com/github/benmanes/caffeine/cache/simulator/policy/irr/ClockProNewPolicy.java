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
import com.github.benmanes.caffeine.cache.simulator.membership.Membership;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.KeyOnlyPolicy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
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
  private final int maximumSize;

  private Node shortIrrListHead;
  private Node longIrrListHead;

  private int sizeHot;
  private int sizeCold;
  private int sizeFree() { return maximumSize - (sizeHot + sizeCold); }

  private SimpleDecayBloomFilter filter;
  private int decayTime;
  private int decayMaxLife;
  private int[] lives = new int[Byte.SIZE + 1];

  // Enable to print out the internal state
  static final boolean debug = false;

  public ClockProNewPolicy(Config config) {
    ClockProSettings settings = new ClockProSettings(config);
    this.maximumSize = Ints.checkedCast(settings.maximumSize());
    this.policyStats = new PolicyStats("irr.ClockProNew");
    this.data = new Long2ObjectOpenHashMap<>();

    this.decayIrrThreshold = 8;
    this.decayMaxLife = 8;
    filter = new SimpleDecayBloomFilter(maximumSize * decayMaxLife, 0.01, decayMaxLife);
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
    System.out.println(data.size());
    Arrays.stream(lives).forEach(System.out::println);
  }

  @Override
  public void record(long key) {
    policyStats.recordOperation();
    Node node = data.get(key);
    if (node == null) {
      node = new Node(key);
      data.put(key, node);
      onMiss(node);
    } else {
      onHit(node);
    }
  }

  private void onHit(Node node) {
    policyStats.recordHit();
    node.marked = true;
  }

  private void onMiss(Node node) {
    policyStats.recordMiss();
    if (sizeFree() > 0) {
      onHotWarmupMiss(node);
    } else {
      onFullMiss(node);
    }
  }

  private void recordNR(Node node) {
    filter.put(node.key);
  }

  private void remove(Node node) {
    if (node == shortIrrListHead) {
      if (node.next == node) {
        shortIrrListHead = null;
      } else {
        shortIrrListHead = shortIrrListHead.next;
      }
    } else if (node == longIrrListHead) {
      if (node.next == node) {
        longIrrListHead = null;
      } else {
        longIrrListHead = longIrrListHead.next;
      }
    }
    node.unlink();
    if (node.status != Status.DECAY) {
      data.remove(node.key);
    }
  }

  private void moveToShortIrrHead(Node node) {
    if (node == shortIrrListHead) {
      return;
    } else if (node == longIrrListHead) {
      if (node.next == node) {
        longIrrListHead = null;
      } else {
        longIrrListHead = longIrrListHead.next;
      }
    }
    node.unlink();
    if (shortIrrListHead != null) {
      node.linkTo(shortIrrListHead);
    }
    shortIrrListHead = node;
  }

  private void moveToLongIrrHead(Node node) {
    if (node == longIrrListHead) {
      return;
    } else if (node == shortIrrListHead) {
      if (node.next == node) {
        shortIrrListHead = null;
      } else {
        shortIrrListHead = shortIrrListHead.next;
      }
    }
    node.unlink();
    if (longIrrListHead != null) {
      node.linkTo(longIrrListHead);
    }
    longIrrListHead = node;
  }

  /** Records a miss when the hot set is not full. */
  private void onHotWarmupMiss(Node node) {
    moveToShortIrrHead(node);
    node.setStatus(Status.HOT_LONG_IRR);
  }

  /** Records a miss when the cold set is not full. */
  private void onColdWarmupMiss(Node node) {
    moveToShortIrrHead(node);
    node.setStatus(Status.COLD_SHORT_IRR);
  }

  int decayIrrThreshold;
  int scanSpeedCredits;

  /** Records a miss when the hot and cold set are full. */
  private void onFullMiss(Node node) {
    checkState(node.status == Status.OUT_OF_CLOCK);

    int val = filter.mightContain2(node.key);
    int freshness = 0; // [Not exist] 0 - [Old] 1 - 2 ... - 7 - 8 [Fresh]
    while (val != 0) {
      freshness++;
      val >>>= 1;
    }
    lives[freshness]++;

    if (freshness == 0) {
      onOutOfClockFullMiss(node);
      return;
    }

    if (freshness >= decayMaxLife - 1) {
      scanSpeedCredits++;
    } else {
      scanSpeedCredits--;
    }

    if (freshness >= decayIrrThreshold + (decayIrrThreshold != decayMaxLife ? 1 : 0)) {
      onNonResidentFullMiss(node);
    } else {
      onOutOfClockFullMiss(node);
    }
  }

  private void onOutOfClockFullMiss(Node node) {
    evict();
    moveToShortIrrHead(node);
    node.setStatus(Status.COLD_SHORT_IRR);
  }

  private void onNonResidentFullMiss(Node node) {
    findVictimFromLongIrr();
    evict();
    moveToShortIrrHead(node);
    node.setStatus(Status.HOT_LONG_IRR);
  }

  int scanLengthLimit = 1;

  private void findVictimFromLongIrr() {
    for (int scanLength = 0; scanLength < scanLengthLimit; scanLength++) {
      if (longIrrListHead == null) {
        return;
      }
      Node longIrrListTail = longIrrListHead.prev;
      if (longIrrListTail.status == Status.DECAY) {
        decayIrrThreshold++;
        System.out.println("Increase decayIrrThreshold to " + decayIrrThreshold);
        remove(longIrrListTail);
        scanLength--;
        continue;
      }
      if (longIrrListTail.marked) {
        scanSpeedCredits--;
        moveToShortIrrHead(longIrrListTail);
        longIrrListTail.setStatus(Status.HOT_LONG_IRR);
      } else {
        moveToShortIrrHead(longIrrListTail);
        longIrrListTail.setStatus(Status.COLD_LONG_IRR);
      }
    }
  }

  private void findVictimFromShortIrr() {
    while (shortIrrListHead == null) {
      findVictimFromLongIrr();
    }
    while (shortIrrListHead != null) {
      decayTime++;
      if (decayTime > maximumSize) {
        filter.decay();
        decayTime = 0;

        Node decay = new Node(0);
        moveToLongIrrHead(decay);
        decay.status = Status.DECAY;
        decayIrrThreshold--;
        System.out.println("Decrease decayIrrThreshold to " + decayIrrThreshold);

        adjustScanLength();
      }

      Node shortIrrListTail = shortIrrListHead.prev;
      if (shortIrrListTail.marked) {
        if (shortIrrListTail.isShortIrr()) {
          scanSpeedCredits++;
        }
        if (shortIrrListTail.status == Status.HOT_LONG_IRR) {
          moveToShortIrrHead(shortIrrListTail);
          shortIrrListTail.setStatus(Status.HOT_SHORT_IRR);
        } else if (shortIrrListTail.status == Status.HOT_SHORT_IRR) {
          moveToShortIrrHead(shortIrrListTail);
          shortIrrListTail.setStatus(Status.HOT_SHORT_IRR);
        } else if (shortIrrListTail.status == Status.COLD_LONG_IRR) {
          moveToShortIrrHead(shortIrrListTail);
          shortIrrListTail.setStatus(Status.COLD_SHORT_IRR);
        } else if (shortIrrListTail.status == Status.COLD_SHORT_IRR) {
          findVictimFromLongIrr();
          moveToShortIrrHead(shortIrrListTail);
          shortIrrListTail.setStatus(Status.HOT_SHORT_IRR);
        }
      } else {
        if (shortIrrListTail.status == Status.HOT_LONG_IRR) {
          moveToLongIrrHead(shortIrrListTail);
          shortIrrListTail.setStatus(Status.HOT_LONG_IRR);
        } else if (shortIrrListTail.status == Status.HOT_SHORT_IRR) {
          moveToLongIrrHead(shortIrrListTail);
          shortIrrListTail.setStatus(Status.HOT_LONG_IRR);
        } else if (shortIrrListTail.status == Status.COLD_LONG_IRR) {
          remove(shortIrrListTail);
          break;
        } else if (shortIrrListTail.status == Status.COLD_SHORT_IRR) {
          recordNR(shortIrrListTail);
          remove(shortIrrListTail);
          break;
        }
      }
    }
  }

  private void adjustScanLength() {
    if (scanSpeedCredits > 0) {
      scanLengthLimit++;
    } else if (scanSpeedCredits < 0) {
      scanLengthLimit--;
    }
    if (scanLengthLimit < 1) {
      scanLengthLimit = 1;
    } else if (scanLengthLimit > 3) {
      scanLengthLimit = 3;
    }
    System.out.println("Scan length limit has changed to : " + scanLengthLimit);
    scanSpeedCredits /= 10;
    System.out.println("ScanSpeedCredits: " + scanSpeedCredits);
  }

  private void evict() {
    policyStats.recordEviction();
    while (sizeFree() == 0) {
      if (sizeCold == 0) {
        findVictimFromLongIrr();
      }
      findVictimFromShortIrr();
    }
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    if (shortIrrListHead != null) {
      System.out.println("** CLOCK-Pro short irr list HEAD (small recency) **");
      System.out.println(shortIrrListHead.toString());
      for (Node n = shortIrrListHead.next; n != shortIrrListHead; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro short irr list TAIL (large recency) **");
    }
    if (longIrrListHead != null) {
      System.out.println("===================================================");
      System.out.println("** CLOCK-Pro long irr list HEAD (small recency) **");
      System.out.println(longIrrListHead.toString());
      for (Node n = longIrrListHead.next; n != longIrrListHead; n = n.next) {
        System.out.println(n.toString());
      }
      System.out.println("** CLOCK-Pro long irr list TAIL (large recency) **");
    }
  }

  enum Status {
    HOT_LONG_IRR, HOT_SHORT_IRR, COLD_LONG_IRR, COLD_SHORT_IRR, OUT_OF_CLOCK,
    DECAY,
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

    public void linkTo(Node node) {
      next = node;
      prev = node.prev;
      node.prev.next = this;
      node.prev = this;
    }

    public void unlink() {
      prev.next = next;
      next.prev = prev;
      prev = next = this;
      marked = false;
      setStatus(Status.OUT_OF_CLOCK);
    }

    public void setStatus(Status status) {
      if (this.isHot()) { sizeHot--; }
      if (this.isCold()) {sizeCold--; }
      this.status = status;
      if (this.isHot()) { sizeHot++; }
      if (this.isCold()) {sizeCold++; }
    }

    boolean isShortIrr() {
      return status == Status.COLD_SHORT_IRR || status == Status.HOT_SHORT_IRR;
    }
    boolean isCold() {
      return status == Status.COLD_LONG_IRR || status == Status.COLD_SHORT_IRR;
    }
    boolean isHot() {
      return status == Status.HOT_LONG_IRR || status == Status.HOT_SHORT_IRR;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("marked", marked)
          .add("type", status)
          .toString());
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

  /**
   * Based on com/github/benmanes/caffeine/cache/simulator/membership/bloom/BloomFilter.java
   */
  static final class SimpleDecayBloomFilter implements Membership {

    static final long[] SEED = { // A mixture of seeds from FNV-1a, CityHash, and Murmur3
            0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L, 0x9ae16a3b2f90404fL, 0xcbf29ce484222325L};

    int tableSize;
    byte[] table;
    byte fullLifeTime;

    /**
     * Creates a membership sketch based on the expected number of insertions and the false positive
     * probability.
     */
    public SimpleDecayBloomFilter(long expectedInsertions, double fpp, int lifetime) {
      checkArgument(lifetime > 0 && lifetime <= Byte.SIZE);
      tableSize = optimalSize(expectedInsertions, fpp);
      table = new byte[tableSize];
      fullLifeTime = (byte) (1 << (lifetime - 1));
    }

    /**
     * Calc optimal size.
     *
     * @param expectedInsertions the number of expected insertions
     * @param fpp the false positive probability, where 0.0 > fpp < 1.0
     */
    int optimalSize(long expectedInsertions, double fpp) {
      checkArgument(expectedInsertions >= 0);
      checkArgument(fpp > 0 && fpp < 1);

      double optimalBitsFactor = -Math.log(fpp) / (Math.log(2) * Math.log(2));
      return (int) (expectedInsertions * optimalBitsFactor);
    }

    @Override
    public boolean mightContain(long e) {
      int item = spread(Long.hashCode(e));
      for (int i = 0; i < 4; i++) {
        int hash = seeded(item, i);
        int index = Math.abs(hash % tableSize);
        if ((table[index]) == 0) {
          return false;
        }
      }
      return true;
    }

    public int mightContain2(long e) {
      int life = Integer.MAX_VALUE;
      int item = spread(Long.hashCode(e));
      for (int i = 0; i < 4; i++) {
        int hash = seeded(item, i);
        int index = Math.abs(hash % tableSize);
        if ((table[index]) == 0) {
          return 0;
        }
        int uintVal = table[index] & 0xFF;
        if (life > uintVal) {
          life = uintVal;
        }
      }
      return life;
    }

    @Override
    public void clear() {
      byte zero = (byte) 0;
      Arrays.fill(table, zero);
    }

    @Override
    @SuppressWarnings("ShortCircuitBoolean")
    public boolean put(long e) {
      int item = spread(Long.hashCode(e));
      return setAt(item, 0) | setAt(item, 1) | setAt(item, 2) | setAt(item, 3);
    }

    public void decay() {
      for (int i = 0; i < tableSize; i++) {
        table[i] = (byte) ((table[i] & 0xFF) >>> 1);
      }
    }

    /**
     * Sets the membership flag for the computed bit location.
     *
     * @param item the element's hash
     * @param seedIndex the hash seed index
     * @return if the membership changed as a result of this operation
     */
    @SuppressWarnings("PMD.LinguisticNaming")
    boolean setAt(int item, int seedIndex) {
      int hash = seeded(item, seedIndex);
      int index = Math.abs(hash % tableSize);
      if (table[index] == fullLifeTime) {
        return true;
      }
      table[index] = fullLifeTime;
      return false;
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which defends against poor quality
     * hash functions.
     */
    int spread(int x) {
      x = ((x >>> 16) ^ x) * 0x45d9f3b;
      x = ((x >>> 16) ^ x) * 0x45d9f3b;
      return (x >>> 16) ^ x;
    }

    /**
     * Applies the independent hash function for the given seed index.
     *
     * @param item the element's hash
     * @param i the hash seed index
     * @return the table index
     */
    static int seeded(int item, int i) {
      long hash = (item + SEED[i]) * SEED[i];
      hash += hash >>> 32;
      return (int) hash;
    }
  }
}
