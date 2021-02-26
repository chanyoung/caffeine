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
@Policy.PolicySpec(name = "irr.ClockProNew")
public final class ClockProNewPolicy implements KeyOnlyPolicy {
  private final Long2ObjectMap<Node> data;
  private final PolicyStats policyStats;

  private final int maximumSize;
  private int maximumHotSize;

  private Node clockHead;
  private Node clockHand;
  // HandHot은 별도로 구현할 필요 없다. tail 이 결국 handHot이라고 볼 수 있다.
  // 하지만 이 경우 다음과 같은 의문을 가질 수 있다.
  // 1. handCold 가 handHot을 지나갈 수 있는가?
  // - 지나갈 수 없다. handCold가 listTail로부터 출발하여 한바퀴 돌아 다시 listTail에 온다면 sizeCold == 0 인 상황이다.
  // sizeCold가 0이면 handCold는 움직이지 않는다. 따라서 handCold는 listTail을 지나갈 수 없다.
  // 추후 coldEntry가 생성이 되면 listTail로부터 다시 출발한다는 개념이 맞다.
  // sizeCold가 0인 경우, handCold 는 null을 가르킨다.
  //
  // 2. handTest의 구현.
  // - handTest는 handHot을 만나면 흡수되는 형식으로 구현되는 것이 맞다.
  // handTest가 지나가면서 resident cold entry를 만날일은 없다.
  // 따라서 handTest의 역할은 오로지 NR이 limit을 넘어가면 삭제하는 것, 그 이상도 이하도 아니다.
  // NR이 limit을 넘어가면 handHot으로부터 출발하여, NR을 목표만큼 삭제하고 머무르면 된다. 그러다가 handHot이 뒤에서 쫓아와서
  // 만나게 되면, null로 만들면 된다. handHot은 handTest의 역할도 함께 수행한다.
  //
  // 3. handHot의 구현.
  // - handHot을 움직이는 시계바늘로 구현할라 하면 골치가 아파지지만, 포인터 이동 연산이 더 저렴하긴 하니까.
  // cold가 0이 되는 경우, handCold를 null로 만들었다가 cold 엔트리가 생기면 추가해준다.
  // hot이 0이 되는 경우, handHot을 null로 만들고, 리스트헤드는 handCold가 대체한다.
  // hot이 새로 생기는 경우, handHot을 할당하고 handHot이 다시 리스트헤드를 갖는다.
  //
  // - handHot을 고정된 리스트 테일로 구현한다면.
  // cold가 0이 되는 경우, handCold를 null로 만들었다가 cold 엔트리가 생기면 추가해준다.
  // hot이 0이 되는 경우, irr을 비교할 hot entry가 없으므로, NR도 필요없다? clock 알고리즘과 동일하게 동작한다.
  // 하지만 NR이 아예 없으면, ARC 어댑션을 적용한 상태에서 hotTarget을 늘릴 방도가 없다.
  private Node clockTail() { return clockHead == null ? null : clockHead.prev; }

  private int sizeHot;
  private int sizeCold;
  private int sizeDemoted;
  private int sizeFree() { return maximumSize - (sizeHot + sizeCold); }

  private SimpleDecayBloomFilter filter;
  private int decayThreshold;
  private int decayTime;
  private int decayMaxLife;
  private int nonResidentIrrThreshold;

  private int scanLengthLimit = 2;
  private int vHotTarget;

  // Enable to print out the internal state
  static final boolean debug = false;

  public ClockProNewPolicy(Config config) {
    BasicSettings settings = new BasicSettings(config);
    this.policyStats = new PolicyStats("irr.ClockProNew");
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = Ints.checkedCast(settings.maximumSize());
    this.maximumHotSize = (int) (maximumSize * 0.99);
    this.clockHead = this.clockHand = null;
    this.decayThreshold = maximumHotSize / 4;
    this.nonResidentIrrThreshold = 8;
    this.decayMaxLife = 8;
    filter = new SimpleDecayBloomFilter(maximumSize * decayMaxLife, 0.001, decayMaxLife);

    this.vHotTarget = maximumSize - maximumHotSize;
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
    System.out.println(nonResidentIrrThreshold);
  }

  @Override
  public void record(long key) {
    if (policyStats.operationCount() % 100000 == 0) {
      System.out.println((maximumSize - vHotTarget) * 100.0 / maximumSize);
    }
    policyStats.recordOperation();
    Node node = data.get(key);
    if (node == null) {
      node = new Node(key);
      data.put(key, node);
      onMiss(node);
    } else {
      onHit(node);
    }
//    if (policyStats.operationCount() % 10000 == 0) {
//      System.out.println(vHotTarget * 100.0 / maximumSize);
//    }
  }

  private void onHit(Node node) {
    policyStats.recordHit();
    node.marked = true;
  }

  private void onMiss(Node node) {
    // cold를 늘릴 수 있는 길이마다 decayThreshold를 조절해나가면 어떨까?
    // 예를 들어서, cold entry가 1% 일때는, max의 98% 까지 늘릴 수 있으니까, decayThreshold를 max의 98%로 지정하는거지.
    // 이러면 유틸리티 어댑션 안해도 되지 않을까?
    //
    // 보통의 케이스에서는 안해도 되는데, DS1에서 안하니까, hotTarget이 낮아져 버려서, scanLength가 길어져서 의미가 없어지네.
    // scanLength는 vHotTarget과 별개로 제어해야하나?
//    decayThreshold = maximumHotSize - sizeCold;

    policyStats.recordMiss();
    if (sizeFree() > maximumSize) {
      onHotWarmupMiss(node);
    } else if (sizeFree() > 0) {
      onColdWarmupMiss(node);
    } else {
      onFullMiss(node);
    }

    if (policyStats.operationCount() - prevOps > maximumSize / 10) {
      updateHotTarget();
      prevOps = policyStats.operationCount();
    }
  }

  long prevOps;

  /** Records a miss when the hot set is not full. */
  private void onHotWarmupMiss(Node node) {
    moveToHead(node);
    node.setStatus(Status.HOT);
  }

  /** Records a miss when the cold set is not full. */
  private void onColdWarmupMiss(Node node) {
    moveToHead(node);
    node.setStatus(Status.COLD_SHORT_IRR);
    if (clockHand == null) {
      clockHand = node;
    }
  }

  private void decayNonResidents() {
    filter.decay();
    Node decayNode = new Node(0);
    decayNode.status = Status.DECAY;
    decayNode.linkTo(clockHand.next);
    nonResidentIrrThreshold--;
  }

  private void recordHistory(long key) {
    filter.put(key);

    decayTime++;
    if (decayTime > decayThreshold && nonResidentIrrThreshold > 1) {
      decayNonResidents();
      decayTime = 0;
//      System.out.println(vHotTarget);
//      System.out.println(scanLengthLimit);
    }
  }

  private void remove(Node node) {
    if (node == clockHand) {
      nextHand();
    }
    node.unlink();
    if (node.status != Status.DECAY) {
      data.remove(node.key);
    }
  }

  private void moveToHead(Node node) {
    if (node == clockHand) {
      nextHand();
    }
    node.unlink();
    if (clockHead != null) {
      node.linkTo(clockHead);
    }
    clockHead = node;
  }

  private void nextHand() {
    checkState(clockHand != null);
    checkState(clockHand != clockHead);
    clockHand = clockHand.prev;
  }

  private double prevHitRate;
  private int nextStep = 1;

  private void updateHotTarget() {
    double hitRate = policyStats.hitRate();
    if (hitRate > prevHitRate) {
      nextStep *= 2;
    } else {
      nextStep /= 2;
      if (nextStep == 0) {
        nextStep = 10;
      }
      nextStep = -nextStep;
    }
    vHotTarget += nextStep;
    if (vHotTarget > maximumHotSize) {
      vHotTarget = maximumHotSize;
    } else if (vHotTarget < maximumSize - maximumHotSize) {
      vHotTarget = maximumSize - maximumHotSize;
    }
    prevHitRate = hitRate;
  }

  /** Records a miss when the hot and cold set are full. */
  private void onFullMiss(Node node) {
    checkState(node.status == Status.OUT_OF_CLOCK);

    int val = filter.mightContain2(node.key);
    int freshness = 0; // [Not exist] 0 - [Old] 1 - 2 ... - 7 - 8 [Fresh]
    while (val != 0) {
      freshness++;
      val >>>= 1;
    }

    if (freshness == 0) {
      onOutOfClockFullMiss(node);
      return;
    }

    if (freshness == decayMaxLife || freshness > nonResidentIrrThreshold) {
      // 굉장히 애매하다. Irr 1 이상인 고스트의 경우 hotTarget을 줄이는게 맞나?
      // Irr이 1 이상이면 어차피 cold 에서 담아내지 못한다. 그렇다면 HotTarget을 줄이지 않는게 맞다.
      // 하지만 HotTarget을 줄이지 않으면? scanhot이 안일어나는게 문제네, 일단 무조건 1번은 일어나야한다.
      // 근데 그렇게 구현 안했었는데, 생각해보니 오리지날 CP에서도 scanhot을 하긴하네.
//      if (vHotTarget > (maximumSize - maximumHotSize)) {
//        vHotTarget--;
//      }
//      if (freshness >= decayMaxLife - 1) {
//        if (vHotTarget > (maximumSize - maximumHotSize)) {
//          vHotTarget--;
//        }
//      } else {
//        if (vHotTarget < maximumHotSize) {
//          vHotTarget++;
//        }
//      }
      onNonResidentFullMiss(node);
    } else {
      onOutOfClockFullMiss(node);
    }
  }

  private void onOutOfClockFullMiss(Node node) {
    evict();
    moveToHead(node);
    node.setStatus(Status.COLD_SHORT_IRR);
  }

  private void onNonResidentFullMiss(Node node) {
    evict();
    if (sizeHot >= vHotTarget) {
      scanHot();
    }
    if (sizeHot < maximumHotSize) {
      moveToHead(node);
      node.setStatus(Status.HOT);
    } else {
      moveToHead(node);
      node.setStatus(Status.COLD_SHORT_IRR);
    }
  }

  private void evict() {
    policyStats.recordEviction();
    while (sizeFree() == 0) {
      runHand();
    }
  }

  private void runHand() {
    Node node = clockHand;
    if (node.isCold()) {
      if (node.marked) {
        if (node.isShortIrr()) {
          if (sizeHot >= vHotTarget) {
            scanHot();
          }
          if (sizeHot < vHotTarget) {
//          if (sizeHot < maximumHotSize) {
            moveToHead(node);
            node.setStatus(Status.HOT);
          } else {
            moveToHead(node);
            node.setStatus(Status.COLD_SHORT_IRR);
          }
        } else {
          // 유틸리티 어댑션이 과연 효율적인가?
          // 유틸리티 어댑션의 장점은 무엇이라고 생각하나?
          // 나는 일단 NR을 지울때 hotTarget을 늘린다는 개념 자체가 틀렸다고 본다.
          // 현재 CP에서 지워지는 NR의 경우 irr이 hot 엔트리 중 가장 irr이 긴 녀석보다 긴 엔트리들이다.
          // 이 케이스가 많은것이 왜 hotTarget을 늘려야 하는 이유가 되는것인가?
          //
          // irr이 제일 긴 hot보다 더 긴 NR의 개수가 늘어나면 hotTarget을 늘려야 한다는 것은 미래에 대한 예측이며 간접적인 이유이다.
          // 그에 반해 내가 irr이 긴 hot 엔트리들을 쫓아냈더니, 잠시후에 그 엔트리에 접근이 된 경우는 직전 행동에 대한 결과이며 직접적인 이유이다.
          // 따라서 결과에 의한 보상으로 움직이는 adaptive 모델이 더 적합하다.
          //
          // 현재 모델에서 2번 이상 접근된 엔트리는 hot 리스트에 올라가게 된다. 따라서 cold 리스트의 크기는
          // 단지 2번째 접근이 이루어지기까지의 irr을 담고있을만한 길이를 최대한 맞춰줄 수 있으면 된다.
          //
          // 위 생각을 바탕으로 보면 NR에서 hit되는 경우가 아닌, resident cold가 hit되는 경우에 콜드 비중을 늘리는 것도
          // 크게 좋은 방법은 아니게 된다. 왜냐하면 이미 현재 확보된 콜드 엔트리의 길이로 두번째 hit를 담아냈기 때문에,
          // 콜드 엔트리의 길이를 더 늘릴 필요가 없기 떄문이다.
          //
          // 그렇다면, 결과적으로 hot target을 늘릴때는 hot 엔트리에서 쫓아낸 것이 다시 hit가 되었을 때 hot 엔트리를 늘려갈 필요가 있는거고
          // cold 엔트리에서 쫓겨난 것이 다시 hit가 되었을 때 cold 엔트리를 늘리는 것이 적합한 adaptive 모델이 된다.
          // 그리고 이 모델은 ARC의 모델이다.
          //
          // CP+는 위 모델을 아주 훌륭하게 CP에 입혔다. CP는 hot에서 demotion되는 경우 바로 축출하지 않고 test 기간이 지난 cold 엔트리로
          // 보내는데, 이는 ARC의 B2와 동일한 역할을 한다.
          //
          // NR에 hit되는 경우 hotTarget을 줄인다고 했을 때.
          // sketch 알고리즘의 경우 어느 경우까지 허용을 해 주어야 하는 것일까?
          // 결과적으로 NR 개수가 maximum size 보다 멀리있는 경우는 hotTarget을 줄일 필요가 없다.
          // 왜냐하면 그 정도 길이의 irr로 접근되었다는 것은 캐시에서 콜드 엔트리 비중이 99%라고 해도 어차피 담아낼 수 없기 떄문이다.
          // 하지만 irr이 현재 hot 엔트리보다 안에 있으므로 hot엔트리로의 진급은 시켜주면 된다.
          //
          // demoted 엔트리의 유틸리티 어댑션 케이스.
          // 이는 다소 불공정게임이라 할 수 있다. 왜냐하면 demoted 엔트리는 handCold에 의해 소멸되는데, 이 속도가 매우 빠를 가능성이 크기 때문이다.
          // 그리고 CP는 ARC와 달리 demoted 엔트리가 삭제될 경우 irr 규칙에 따라 히스토리를 생성하지 않기 때문에, hotTarget이 상승할 확률이
          // 다소 부족하다고 볼 수 있다. 따라서 CP+는 유틸리티 어댑션을 보완하여 성공을 거뒀다.
          //
          // (반대의견) 유틸리티 어댑션이 필요한가? CP의 경우 cold가 줄어들면 NR은 늘어나고, cold가 늘어날 확률은 자연스레 커지고
          // cold가 늘어나면 NR이 줄고 demoted 된 애들이 많아 hot으로 갈 확률이 알아서 커질텐데?
          //
          // 유틸리티 어댑션을 어떻게 해야하는가가 의문이네.
          if (vHotTarget < maximumHotSize) {
//            int max = maximumSize * (decayMaxLife - nonResidentIrrThreshold + 1);
//            vHotTarget += max / (sizeHot / 4);
//            if (vHotTarget > maximumHotSize) {
//              vHotTarget = maximumHotSize;
//            }
//            vHotTarget++;

//            vHotTarget += Math.max(1, maximumSize / 2 / (sizeDemoted + 1));
//            if (vHotTarget > maximumHotSize) {
//              vHotTarget = maximumHotSize;
//            }
          }
          moveToHead(node);
          node.setStatus(Status.COLD_SHORT_IRR);
        }
      } else {
        if (node.isShortIrr()) {
          recordHistory(node.key);
        }
        remove(node);
      }
    } else {
      if (node.marked) {
        moveToHead(node);
        node.setStatus(Status.HOT);
      } else {
        nextHand();
      }
    }
  }

  private void scanHot() {
    while (sizeHot >= vHotTarget && clockTail() != clockHand) {
      for (Node node = clockTail(); node != clockHand; node = node.prev) {
        checkState(node.isHot() || node.status == Status.DECAY);

        if (node.status == Status.DECAY) {
          nonResidentIrrThreshold++;
//        System.out.println("Increase decayIrrThreshold to " + decayIrrThreshold);
          remove(node);
          node = clockHead;
          continue;
        }

        if (node.marked) {
          moveToHead(node);
          node.setStatus(Status.HOT);
        } else {
          moveToHead(node);
          node.setStatus(Status.COLD_LONG_IRR);
        }
      }
    }
  }

  /** Prints out the internal state of the policy. */
  private void printClock() {
    System.out.println("** CLOCK-Pro list HEAD (small recency) **");
    System.out.println(clockHead.toString());
    for (Node n = clockHead.next; n != clockHead; n = n.next) {
      System.out.println(n.toString());
    }
    System.out.println("** CLOCK-Pro list TAIL (large recency) **");
  }

  enum Status {
    HOT, COLD_LONG_IRR, COLD_SHORT_IRR,
    DECAY, OUT_OF_CLOCK,
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
      if (this.status == Status.COLD_LONG_IRR) { sizeDemoted--; }
      this.status = status;
      if (this.isHot()) { sizeHot++; }
      if (this.isCold()) {sizeCold++; }
      if (this.status == Status.COLD_LONG_IRR) { sizeDemoted++; }
    }

    boolean isShortIrr() {
      return status == Status.COLD_SHORT_IRR;
    }
    boolean isCold() {
      return status == Status.COLD_LONG_IRR || status == Status.COLD_SHORT_IRR;
    }
    boolean isHot() {
      return status == Status.HOT;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("marked", marked)
        .add("type", status)
        .toString());
      if (this == clockHand) {
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