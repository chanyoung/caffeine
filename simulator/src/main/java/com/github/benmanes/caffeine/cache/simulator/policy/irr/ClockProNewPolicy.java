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

  private Node head;
  private Node hand;

  private int sizeHot;
  private int sizeCold;
  private int sizeFree() { return maximumSize - (sizeHot + sizeCold); }

  // +---------------------------------------------+
  // |   ColdTarget Adaption Algorithm Summary     |
  // +------+----------+---------------------------+
  // | When | increase | access in short irr pages |
  // |      | decrease |  access in long irr pages |
  // +------+----------+---------------------------+
  // | Size | increase |                        +1 |
  // |      | decrease |                        -1 |
  // +------+----------+---------------------------+
  private int coldTarget;
  private int hotTarget() { return maximumSize - coldTarget; }

  // {min,max}ResColdSize are boundary of coldTarget.
  private int minResColdSize;
  private int maxResColdSize;

  private SimpleDecayBloomFilter filter;
  private int decayThreshold;
  private int decayTime;
  private int decayMaxLife;
  private int[] lives = new int[Byte.SIZE + 1];

  // Enable to print out the internal state
  static final boolean debug = false;

  public ClockProNewPolicy(Config config) {
    ClockProSettings settings = new ClockProSettings(config);
    this.maximumSize = Ints.checkedCast(settings.maximumSize());
    this.minResColdSize = (int) (maximumSize * settings.percentMinCold());
    if (minResColdSize < settings.lowerBoundCold()) {
      minResColdSize = settings.lowerBoundCold();
    }
    this.maxResColdSize = (int) (maximumSize * settings.percentMaxCold());
    if (maxResColdSize > maximumSize - minResColdSize) {
      maxResColdSize = maximumSize - minResColdSize;
    }
    this.policyStats = new PolicyStats("irr.ClockProNew");
    this.data = new Long2ObjectOpenHashMap<>();
    this.coldTarget = minResColdSize;
    this.head = this.hand = null;
    checkState(minResColdSize <= maxResColdSize);

    this.decayThreshold = maximumSize;
    this.decayIrrThreshold = 7;
    this.decayMaxLife = 7;
    filter = new SimpleDecayBloomFilter(maximumSize * 5, 0.001, decayMaxLife);

    lifelimit = maximumSize;
  }

  private void calcLifeLimitInitVal() {
    lifelimitInitVal = (sizeHot / 2) / Math.max(1, (decayMaxLife - decayIrrThreshold + 1));
//    System.out.println("life limit: " + lifelimitInitVal);
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
//    printClock();
    Arrays.stream(lives).forEach(System.out::println);
    System.out.println("IN : " + in);
    System.out.println("OUT: " + out);
    System.out.println(data.size());
    System.out.println(decayIrrThreshold);
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
    if (sizeFree() > minResColdSize) {
      onHotWarmupMiss(node);
    } else if (sizeFree() > 0) {
      onColdWarmupMiss(node);
    } else {
      onFullMiss(node);
    }
  }

  private void recordNR(Node node) {
    filter.put(node.key);

    decayTime++;
    if (decayTime > decayThreshold && decayIrrThreshold > 1) {
      filter.decay();
      decayTime = 0;

      Node decay = new Node(0);
      decay.status = Status.DECAY;
      decay.linkTo(hand.next);
      decayIrrThreshold--;
//      System.out.println("Decrease decayIrrThreshold to " + decayIrrThreshold);

      int prevScanLength = scanLength;
      calcScanLength();
//      System.out.println("Scan length has changed from " + prevScanLength + " to " + scanLength);
    }
  }

  private void remove(Node node) {
    if (node == hand) {
      nextHand();
    }
    node.unlink();
    if (node.status != Status.DECAY) {
      data.remove(node.key);
    }
  }

  private void moveToHead(Node node) {
    if (node == hand) {
      nextHand();
    }
    node.unlink();
    if (head != null) {
      node.linkTo(head);
    }
    head = node;
  }

  private void nextHand() {
    checkState(hand != null);
    checkState(hand != head);
    hand = hand.prev;
  }

  /** Records a miss when the hot set is not full. */
  private void onHotWarmupMiss(Node node) {
    moveToHead(node);
    node.setStatus(Status.HOT_SHORT_IRR);
  }

  /** Records a miss when the cold set is not full. */
  private void onColdWarmupMiss(Node node) {
    moveToHead(node);
    node.setStatus(Status.COLD_SHORT_IRR);
    if (hand == null) {
      hand = node;
    }
  }

  int decayIrrThreshold;

  int out = 1;
  int in = 1;

  int scanLength = 1;
  int maxScanLength = 10;
  private void calcScanLength() {
//    System.out.println("in: " + in + ", out: " + out);
    if (in + out < maximumSize) {
//      System.out.println("Sample is too small");
      return;
    }

    int delta = Math.abs(in - out) / Math.min(in, out);
    if (in > out) {
      scanLength = 2 + delta;
      scanLength = Math.min(maxScanLength, scanLength);
    } else {
      scanLength = 2 - delta;
      scanLength = Math.max(1, scanLength);
    }

//    scanLength = in / out;
//    if (scanLength == 0) {
//      scanLength = 1;
//    } else if (scanLength > maxScanLength) {
//      scanLength = maxScanLength;
//    }
    in = out = 1;
  }

  int lifelimitInitVal;
  int lifelimit;

  /** Records a miss when the hot and cold set are full. */
  private void onFullMiss(Node node) {
    checkState(node.status == Status.OUT_OF_CLOCK);

    int life = filter.mightContain2(node.key);
    int index = 0;
    while (life != 0) {
      index++;
      life = life >>> 1;
    }
    lives[index]++;

    if (index > 0) {
      if (index == decayMaxLife) {
        in++;
        coldTargetAdjust(+1);
      } else {
        out++;
        coldTargetAdjust(-1);
      }

      // Loop problem. ..
      // Fix decayIrrThreshold -> decayIrrThreshold + 1
      if (index >= decayIrrThreshold) {
//        in++;
//        coldTargetAdjust(+1);
        if (index == decayIrrThreshold) {
          if (lifelimit > 0) {
            onNonResidentFullMiss(node);
            lifelimit--;
          } else {
            onOutOfClockFullMiss(node);
          }
        } else {
          onNonResidentFullMiss(node);
        }
      } else {
//        out++;
//        coldTargetAdjust(-1);
        onOutOfClockFullMiss(node);
      }
    } else {
      onOutOfClockFullMiss(node);
    }

//    if (filter.mightContain(node.key)) {
//      onNonResidentFullMiss(node);
//      throw new IllegalStateException();
//    } else {
//      onOutOfClockFullMiss(node);
//    }
  }

  private void onOutOfClockFullMiss(Node node) {
    evict();
    moveToHead(node);
    node.setStatus(Status.COLD_SHORT_IRR);
  }

  private void onNonResidentFullMiss(Node node) {
    evict();
    if (sizeHot < maximumSize - coldTarget) {
      moveToHead(node);
      node.setStatus(Status.HOT_SHORT_IRR);
    } else {
      if (demoteHot()) {
        moveToHead(node);
        node.setStatus(Status.HOT_SHORT_IRR);
      } else {
        moveToHead(node);
        node.setStatus(Status.COLD_SHORT_IRR);
      }
    }
//    if (sizeHot >= maxSize - coldTarget) {
//      demoteHot();
//    }
//    if (sizeHot < maxSize - coldTarget) {
//      moveToHead(node);
//      node.setStatus(Status.HOT_SHORT_IRR);
//    } else {
//      moveToHead(node);
//      node.setStatus(Status.COLD_SHORT_IRR);
//    }

//    evict();
//    if (canPromote()) {
//      int life = filter.mightContain2(node.key);
//      int index = 0;
//      while (life != 0) {
//        index++;
//        life = life >>> 1;
//      }
//      lives[index]++;
//
//      if (index >= decayIrrThreshold) {
//        moveToHead(node);
//        node.setStatus(Status.HOT_SHORT_IRR);
//      } else {
//        moveToHead(node);
//        node.setStatus(Status.COLD_SHORT_IRR);
//      }
//    } else {
//      moveToHead(node);
//      node.setStatus(Status.COLD_SHORT_IRR);
//    }
  }

  private void evict() {
    policyStats.recordEviction();
    while (sizeFree() == 0) {
      runHand();
    }
  }

  private void coldTargetAdjust(int n) {
    coldTarget += n;
    if (coldTarget < minResColdSize) {
      coldTarget = minResColdSize;
    } else if (coldTarget > maxResColdSize) {
      coldTarget = maxResColdSize;
    }
  }

  private void runHand() {
    if (hand == head) {
      printClock();
      throw new IllegalStateException();
    }
    checkState(hand != head);
    Node node = hand;

    if (node.isCold()) {
      if (node.marked) {
        if (node.isShortIrr()) {
          coldTargetAdjust(+1);
          in++;
        } else {
          out++;
        }

        if (canPromote()) {
          moveToHead(node);
          node.setStatus(Status.HOT_SHORT_IRR);
        } else {
          moveToHead(node);
          node.setStatus(Status.COLD_SHORT_IRR);
        }
      } else {
        if (node.isShortIrr()) {
          recordNR(node);
        }
        remove(node);
      }
    } else {
      if (node.marked) {
        if (node.isShortIrr()) {
          in++;
        } else {
//          out++;
        }
      }
      nextHand();
    }
  }

  private boolean canPromote() {
    int targetHot = maximumSize - coldTarget;
    if (sizeHot >= targetHot) {
      demoteHot();
    }
    return sizeHot < targetHot;
  }

  private boolean demoteHot() {
    boolean demote = false;
//    int scanLimit = 1;
    int scanLimit = scanLength;
    for (Node node = head.prev; node != hand; node = node.prev) {
      checkState(node.isHot() || node.status == Status.DECAY);

      if (node.status == Status.DECAY) {
        decayIrrThreshold++;
//        System.out.println("Increase decayIrrThreshold to " + decayIrrThreshold);
        remove(node);
        node = head;
        calcLifeLimitInitVal();
        lifelimit = lifelimitInitVal;
        continue;
      }

      if (node.marked) {
//        coldTargetAdjust(-1);
        out++;
        moveToHead(node);
        node.setStatus(Status.HOT_LONG_IRR);
      } else {
        moveToHead(node);
        node.setStatus(Status.COLD_LONG_IRR);

        demote = true;
        if (sizeHot < maximumSize - coldTarget) {
          break;
        }
      }

      scanLimit--;
      if (scanLimit == 0) {
        break;
      }
    }
//    return false;
    return demote;
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    System.out.println("** CLOCK-Pro list HEAD (small recency) **");
    System.out.println(head.toString());
    for (Node n = head.next; n != head; n = n.next) {
      System.out.println(n.toString());
    }
    System.out.println("** CLOCK-Pro list TAIL (large recency) **");
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
    boolean isInClock() {
      return status != Status.OUT_OF_CLOCK && status != Status.DECAY;
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
        if (life > table[index]) {
          life = table[index];
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
        table[i] = (byte) (table[i] >>> 1);
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
