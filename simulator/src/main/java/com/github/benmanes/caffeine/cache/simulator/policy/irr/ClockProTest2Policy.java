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
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

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
 * <p>
 * This implementation is based on
 * <a href="https://bitbucket.org/SamiLehtinen/pyclockpro">PyClockPro</a> by Sami Lehtinen,
 * available under the MIT license.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class ClockProTest2Policy implements KeyOnlyPolicy {
  private final Long2ObjectMap<Node> data;
  private final PolicyStats policyStats;

  /* the percentage band within which the number of cold blocks is adjusted */
  private double MIN_COLD_ALLOC_RATE;
  private double MAX_COLD_ALLOC_RATE;
  private int MIN_COLD_ALLOC;
  private int MAX_COLD_ALLOC;
  /* however, the above band should be no less than this  */
  private int LOWEST_COLD_ALLOC;
  private int MAX_NON_RES_PAGE;

  private int freeMemSize;
  private int memSize;
  private int clockSize;
  private int numHotPages;
  private int numNonResPages;
  private int numColdPages() { return memSize - numHotPages; }
  private int coldIncreaseCredits;

  private Node clockHead;

  // Points to the hot page with the largest recency. The position of this hand actually serves as a
  // threshold of being a hot page. Any hot pages swept by the hand turn into cold ones. For the
  // convenience of the presentation, we call the page pointed to by HAND as the tail of the list,
  // and the page immediately after the tail page in the clockwise direction as the head of the
  // list.
  private Node handHot;

  // Points to the last resident cold page (i.e., the farthest one to the list head). Because we
  // always select this cold page for replacement, this is the position where we start to look for a
  // victim page, equivalent to the hand in CLOCK.
  private Node handCold;

  // Points to the last cold page in the test period. This hand is used to terminate the test period
  // of cold pages. The non-resident cold pages swept over by this hand will leave the circular
  // list.
  private Node handTest;

  public ClockProTest2Policy(Config config) {
    ClockProSettings settings = new ClockProSettings(config);
    memSize = Ints.checkedCast(settings.maximumSize());
    policyStats = new PolicyStats("irr.ClockProTest2");
    data = new Long2ObjectOpenHashMap<>();

    clockSize = numHotPages = numNonResPages = coldIncreaseCredits = 0;
    freeMemSize = memSize;
    clockHead = handHot = handCold = handTest = null;

    MIN_COLD_ALLOC_RATE = settings.percentMinCold();
    MAX_COLD_ALLOC_RATE = settings.percentMaxCold();
    LOWEST_COLD_ALLOC = settings.lowerBoundCold();

    /* verify the band in which cold allocation adapts */
    MIN_COLD_ALLOC = (int) (MIN_COLD_ALLOC_RATE * memSize);
    if (MIN_COLD_ALLOC < LOWEST_COLD_ALLOC) {
      MIN_COLD_ALLOC = LOWEST_COLD_ALLOC;
    }
    if (MAX_COLD_ALLOC_RATE < MIN_COLD_ALLOC_RATE) {
      MAX_COLD_ALLOC = MIN_COLD_ALLOC;
    } else {
      MAX_COLD_ALLOC = (int) (MAX_COLD_ALLOC_RATE * memSize);
      if (MAX_COLD_ALLOC < LOWEST_COLD_ALLOC) {
        MAX_COLD_ALLOC = LOWEST_COLD_ALLOC;
      }
    }

    MAX_NON_RES_PAGE = (int) (memSize * settings.nonResidentMultiplier());
  }

  /**
   * Returns all variations of this policy based on the configuration parameters.
   */
  public static Set<Policy> policies(Config config) {
    return ImmutableSet.of(new ClockProTest2Policy(config));
  }

//  private void viewClock(Node n) {
//    System.out.printf("\n No. %d: to access %d (%s)...\n", 0, n.key, n.isResident? "hit":"miss");
//    System.out.printf("free_mem_size = %d,  num_hot_pages = %d  clock_size = %d ColdIncreaseCredits = %d\n",
//            freeMemSize, numHotPages, clockSize, coldIncreaseCredits);
//
//    Node p = clockHead;
//    if (p == null) {
//      return;
//    }
//
//    do {
//      if (p.isColdPage) {
//        if (p.isInTest)
//          System.out.printf("{");
//        else
//          System.out.printf("(");
//      } else {
//        System.out.printf("[");
//      }
//      System.out.printf("%d", p.key);
//      if (p == handHot)
//        System.out.printf("^");
//      if (p == handTest)
//        System.out.printf("@");
//      if (p == handCold)
//        System.out.printf("*");
//      if (p.refBit)
//        System.out.printf(".");
//      if (p.isResident)
//        System.out.printf("=");
//      System.out.printf(" ");
//      p = p.next;
//      checkState(p.prev.next == p);
//    } while (p != clockHead);
//    System.out.printf("\n");
//  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void finished() {
    statCheck();
  }

  @Override
  public void record(long key) {
    Node n = data.get(key);
    if (n == null) {
      n = new Node(key);
      data.put(key, n);
    }
//    viewClock(n);
    if (n.isResident) {
      policyStats.recordOperation();
      policyStats.recordHit();
      /* BLOCK HIT */
      n.refBit = true;
      // This maybe just for a stat.
//        break nextRefNoRefReset;
    } else {
      policyStats.recordOperation();
      policyStats.recordMiss();
      /* BLOCK MISS */

      /* INITIALIZATION */
      /* fill the clock with (mem_size - MIN_cold_alloc) hot pages at first,
       * then MIN_cold_alloc cold pages.
       */

      /* a default hot page */
      if (freeMemSize > MIN_COLD_ALLOC) {
        n.isResident = true;
        freeMemSize--;

        n.isColdPage = false;
        numHotPages++;

        /* first page in the clock */
        if (handHot == null) {
          handHot = n;
          handTest = handHot;
          handHot.next = handHot;
          handHot.prev = handHot;
          clockHead = handHot;

          n.isInClock = true;
          clockSize++;
        } else {
          insertToClockHead(n);
        }

        // goto next_ref:
        // Maybe just set ref_bit = 0;
        n.refBit = false;
        return;

      } else if (freeMemSize > 0) {
        /* a default resident cold page */
        n.isResident = true;
        freeMemSize--;
        n.isInTest = true;

        if (handCold == null) {
          handCold = n;
        }

        insertToClockHead(n);
        // goto next_ref:
        // Maybe just set ref_bit = 0;
        n.refBit = false;
        return;
      }

      checkState(n.isColdPage);

      /* for out of clock cold page */
      /* outOfClock */
//      outOfClock:
      while (true) {
        if (n.isInClock == false) {
          /* if a page is originally identified as in-clock and
           * ends up being out-of-clock by run_HAND_hot,
           * its free_mem_size is 1.
           */
          if (freeMemSize == 0) {
            runHandCold();
          }
          checkState(freeMemSize == 1);

          n.isResident = true;
          freeMemSize--;

          insertToClockHead(n);
          n.isInTest = true;
        } else {
          /* for in clock non-resident cold page */
          checkState(n.isInTest);
          /* get a free page */
          runHandCold();
          checkState(freeMemSize == 1);
          if (n.isInClock == false) {
//          break outOfClock;
            continue;
          }
          checkState(n.isInTest);

          int isMatched = runHandHot(n);
          if (isMatched == 1) {
//          break outOfClock;
            continue;
          }

          /* be promoted */
          n.isResident = true;
          freeMemSize--;

          n.isColdPage = false;
          numHotPages++;

          coldIncreaseCredits++;

          if (isMatched == 2) {
            checkState(n.next == null);
          } else {
            if (n == handTest) {
              handTest = handTest.prev;
            }

            removeFromClock(n, false);
            numNonResPages--;
          }

          insertToClockHead(n);
        }

        // next_ref:
        n.refBit = false;
        break;
      }
    }
  }

  /* this function searches for a hot page with its bit of 0 to replace.
   * return value:
   *           2: HAND_hot meets 'cold_ptr' and a hot page is demoted into a cold one
   *
   *           1: Otherwise, HAND_hot meets 'cold_ptr';
   *           0: Otherwise.
   */
  private int runHandHot(Node coldPtr) {
    boolean isDone = false;
    boolean isMatched = false;
    boolean isDemote = false;
    Node tmp;

    /* 'cold_ptr' is the cold page that triggers the movement of HAND_hot */
    checkState(coldPtr.isColdPage);
    checkState(coldPtr.isInClock);
    checkState(coldPtr.isInTest);

    while (isDone == false || handHot.isColdPage) {
      /* see a hot page */
      if (handHot.isColdPage == false) {
        if (handHot.refBit) {
          handHot.refBit = false;
        } else {
          handHot.isColdPage = true;
          isDone = true;
          numHotPages--;
          isDemote = true;
        }

        tmp = handHot;
        if (handTest == handHot)
          handTest = handTest.prev;
        handHot = handHot.prev;

        removeFromClock(tmp, false);
        insertToClockHead(tmp);
      } else {
        /* see a cold page */
        if (handHot == coldPtr) {
          isDone = true;
          isMatched = true;
          checkState(handHot == handTest);
        }

        handHot = handHot.prev;
      }
      if (handHot.next == handTest) {
        runHandTest(true);
      }
    }

    if (isMatched && isDemote) return 2;
    if (isMatched) return 1;
    return 0;
  }

  private void runHandTest(boolean mustMoveOneStep) {
    if (!mustMoveOneStep) {
      nonResLimitEnforce();
      return;
    }

    if (handTest.isColdPage) {
      if (!handTest.isResident) {
        checkState(handTest.isInTest);
        coldIncreaseCredits--;
      }

      handTest.isInTest = false;
      handTest.refBit = false;

      if (handTest.isResident == false) {
        handTest = handTest.prev;
        removeFromClock(handTest.next, true);
        numNonResPages--;
        checkState(numNonResPages <= MAX_NON_RES_PAGE);
      } else {
        handTest = handTest.prev;
      }
    } else {
      handTest = handTest.prev;
    }

    nonResLimitEnforce();
  }

  private void nonResLimitEnforce() {
    if (numNonResPages > MAX_NON_RES_PAGE) {
      while (handTest.isResident) {
        if (handTest.isColdPage) {
          handTest.isInTest = false;
          // What? not same in the paper.
          handTest.refBit = false;
        }
        handTest = handTest.prev;
      }

      coldIncreaseCredits--;

      handTest = handTest.prev;
      removeFromClock(handTest.next, true);
      numNonResPages--;
      checkState(numNonResPages <= MAX_NON_RES_PAGE);
    }
  }

  private void runHandCold() {
    Node tmp;
    int isMatched;

    checkState(handCold.isColdPage && handCold.isResident);

//    evictablePage:
    while (true) {
      /* non-accessed page */
      if (handCold.refBit == false || handCold.isInTest == false) {
        handCold.isResident = false;
        freeMemSize++;
        handCold = handCold.prev;

        /* inTest --> stay in the clock */
        if (handCold.next.isInTest)
          numNonResPages++;
        else
          removeFromClock(handCold.next, true);

        while (handCold.isColdPage == false)
          handCold = handCold.prev;

        checkState(handCold.isResident);

        runHandTest(false);

        /* evictablepage */
        return;
      } else {
        /* accessed page */

        /* filter the coldIncreaseCredits */
        checkAdaptiveBound();

        /* promote the cold page w/o running HAND_hot */
        if (coldIncreaseCredits < 0) {
          tmp = handCold;
          handCold = handCold.prev;
          removeFromClock(tmp, false);
          tmp.isColdPage = false;
          numHotPages++;
          tmp.refBit = false;
          insertToClockHead(tmp);
          coldIncreaseCredits++;
        } else if (coldIncreaseCredits > 0) {
          /* demote a hot page w/o promoting the cold page
           * to compensate the accessed page, move it to top w/o resetting its bit
           */
          isMatched = runHandHot(handCold);
          if (isMatched == 0) {
            tmp = handCold;
            handCold = handCold.prev;
            removeFromClock(tmp, false);
            handCold.refBit = true; /* due to resetting at remove_from_clock */
            tmp.isInTest = true;
            insertToClockHead(tmp);
            coldIncreaseCredits--;
          } else {
            /* this is a fake accessed page */
            handCold.refBit = false;
            if (isMatched == 2) {
              /* matched but also have demotion */
              coldIncreaseCredits--;
            }
            /* evictablepage */
//          break evictablePage;
            continue;
          }
        } else {
          /* normal case: one promtion + one demotion */
          isMatched = runHandHot(handCold);

          if (isMatched == 0 || isMatched == 2) {
            tmp = handCold;
            handCold = handCold.prev;
            removeFromClock(tmp, false);
            insertToClockHead(tmp);
            tmp.isColdPage = false;
            numHotPages++;
          }
        }

        /* search for next cold page for consideration */
        while (handCold.isColdPage == false) {
          handCold = handCold.prev;
        }

        /* evictablepage */
//        break evictablePage;
        continue;
      }
    }
  }

  private void statCheck() {
    int hots = 0;
    int residents = 0;
    int nonResidents = 0;
    int clockSize = 0;

    Node n = clockHead;

    if (n == null) {
      return;
    }

    do {
      if (!n.isColdPage) hots++;
      if (n.isResident) residents++;
      if (n.isInClock) clockSize++;
      if (!n.isResident) {
        checkState(n.isColdPage);
        checkState(n.isInTest);
        nonResidents++;
      }
      n = n.next;
    } while (n != clockHead);

    checkState(residents + freeMemSize == memSize);
    checkState(hots == numHotPages);
    checkState(freeMemSize + numColdPages() >= LOWEST_COLD_ALLOC);
    checkState(numColdPages() - freeMemSize <= memSize - LOWEST_COLD_ALLOC);
    checkState(clockSize == this.clockSize);
    checkState(nonResidents == numNonResPages);
    checkState(nonResidents <= MAX_NON_RES_PAGE);
    checkState(nonResidents + residents == this.clockSize);
  }

//  /* Why is this need? */
//  private void initOnePage(Node n) {
//    n.isResident = false;
//    n.isColdPage = true;
//
//    n.next = null;
//    n.prev = null;
//
//    n.isInClock = false;
//    n.isInTest = false;
//    n.refBit = false;
//
//    return;
//  }

  private void removeFromClock(Node n, boolean isPermanent) {
    checkState(n != handCold);
    checkState(n != handHot);
    checkState(n != handTest);
    checkState(n != clockHead);

    n.prev.next = n.next;
    n.next.prev = n.prev;

    n.next = null;
    n.prev = null;
    n.isInClock = false;
    clockSize--;
    n.isInTest = false;

    n.refBit = false;
    if (isPermanent) {
      n.isResident = false;
      n.isColdPage = true;
    }
  }

  private void insertToClockHead(Node n) {
    checkState(freeMemSize > 0 || clockHead != handHot);
    checkState(freeMemSize > 0 || clockHead != handTest);

    n.next = clockHead;
    n.prev = clockHead.prev;
    clockHead.prev.next = n;
    clockHead.prev = n;
    clockHead = n;

    clockHead.isInClock = true;
    clockSize++;

    return;
  }

  private void checkAdaptiveBound() {
    if (coldIncreaseCredits > 0 && numColdPages() >= MAX_COLD_ALLOC) {
      coldIncreaseCredits = 0;
    }
    if (coldIncreaseCredits < 0 && numColdPages() <= MIN_COLD_ALLOC) {
      coldIncreaseCredits = 0;
    }
  }

  private static final class Node {
    final long key;

    Node prev;
    Node next;

    boolean isResident;
    boolean isColdPage;
    boolean isInClock;
    boolean isInTest;
    boolean refBit;

    public Node(long key) {
      this.isColdPage = true;
      this.key = key;
    }

    /** Removes the node from the list. */
    public void remove() {
      prev.next = next;
      next.prev = prev;
      prev = next = null;
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
