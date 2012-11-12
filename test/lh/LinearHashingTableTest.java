package lh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrey Lomakin
 * @since 24.07.12
 */
public class LinearHashingTableTest {
  private static final int KEYS_COUNT = 100000;
  public static final int MAX_SEED = 1000;

  @Test
  @Ignore // not uniformly distributed data is not allowed while statistic was not implemented
  public void testKeyPut() {
    LinearHashingTable linearHashingTable = new LinearHashingTable();

    for (int i = 0; i < KEYS_COUNT; i++){
      Assert.assertTrue("i " + i, linearHashingTable.put(i));
      Assert.assertTrue("i " + i, linearHashingTable.contains(i));
    }

    for (int i = 0; i < KEYS_COUNT; i++){
      Assert.assertTrue(i + " key is absent", linearHashingTable.contains(i));
    }

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++){
      Assert.assertFalse(linearHashingTable.contains(i));
    }
  }

  @Test
  public void testKeyPutRandom() {
    LinearHashingTable linearHashingTable;
    Random random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;//Long.MIN_VALUE;

    int stackCnt = 0;
    int groupCnt = 0;
    while (i < MAX_SEED) {
      try {
        linearHashingTable = new LinearHashingTable();
        random = new Random(i);
        keys.clear();

        while (keys.size() < KEYS_COUNT) {
          long key = random.nextLong();
//          if (key < 0)
//            key = -key;

          if (linearHashingTable.put(key)) {
            keys.add(key);
            Assert.assertTrue("key " + key, linearHashingTable.contains(key));
          }
        }

        for (long key : keys)
          Assert.assertTrue("" + key, linearHashingTable.contains(key));


      } catch (GroupOverflowException e) {
        groupCnt++;//Do nothing. This exception should occurs on non uniform distributed data.
      } catch (StackOverflowError e) {
        stackCnt++;
        //Do nothing. This exception should occurs on non uniform distributed data.
      } catch (Throwable e) {
        e.printStackTrace();
        break;
      } finally {
        i++;
        System.out.println(
            "i = " + i + " % " + ((100.0 * (stackCnt + groupCnt)) / i) + "(" + stackCnt + "+" + groupCnt + "/" + i + ")"
        );
      }
    }

  }

  @Test
  @Ignore // not uniformly distributed data is not allowed while statistic was not implemented
  public void testKeyDelete() {
    LinearHashingTable linearHashingTable = new LinearHashingTable();

    for (int i = 0; i < KEYS_COUNT; i++)
      linearHashingTable.put(i);

    for (int i = 0; i < KEYS_COUNT; i++){
      if (i % 3 == 0)
        Assert.assertTrue("i " + i, linearHashingTable.delete(i));
    }

    for (int i = 0; i < KEYS_COUNT; i++){
      if (i % 3 == 0)
        Assert.assertFalse(linearHashingTable.contains(i));
      else
        Assert.assertTrue(linearHashingTable.contains(i));
    }
  }

  @Test
  public void testKeyDeleteRandom() {
    int seed = 0;
    while (seed < MAX_SEED) {
      try {

        LinearHashingTable linearHashingTable = new LinearHashingTable();
        HashSet<Long> longs = new HashSet<Long>();
        final Random random = new Random(seed);

        for (int i = 0; i < KEYS_COUNT; i++){
          long key = random.nextLong();
//      if (key < 0)
//        key = -key;


          if (linearHashingTable.put(key)) {
            longs.add(key);
          }
        }

        for (long key : longs){
          if (key % 3 == 0) {
            Assert.assertTrue("key = " + key, linearHashingTable.delete(key));
          }
        }

        for (long key : longs){
          if (key % 3 == 0)
            Assert.assertFalse(linearHashingTable.contains(key));
          else
            Assert.assertTrue(linearHashingTable.contains(key));
        }
      } finally {
        seed++;
        System.out.println(seed + "/" + seed + " = " + (100.0 * seed) / seed + "%");
      }
    }
  }

  @Test
  @Ignore // not uniformly distributed data is not allowed while statistic was not implemented
  public void testKeyAddDelete() {
    LinearHashingTable linearHashingTable = new LinearHashingTable();

    for (int i = 0; i < KEYS_COUNT; i++)
      Assert.assertTrue(linearHashingTable.put(i));

    for (int i = 0; i < KEYS_COUNT; i++){
      if (i % 3 == 0)
        Assert.assertTrue(linearHashingTable.delete(i));

      if (i % 2 == 0)
        Assert.assertTrue(linearHashingTable.put(KEYS_COUNT + i));
    }

    for (int i = 0; i < KEYS_COUNT; i++){
      if (i % 3 == 0)
        Assert.assertFalse(linearHashingTable.contains(i));
      else
        Assert.assertTrue(linearHashingTable.contains(i));

      if (i % 2 == 0)
        Assert.assertTrue("i " + (KEYS_COUNT + i), linearHashingTable.contains(KEYS_COUNT + i));
    }
  }


  private List<Long> getUniqueRandomValuesArray(int seed, int size) {
    Random random = new Random(seed);
    long data[] = new long[size];
    for (int i = 0, dataLength = data.length; i < dataLength; i++){
      data[i] = (long) (Long.MIN_VALUE + (i * Math.pow(2, 64) / size));
    }

    int max = data.length - 1;

    List<Long> list = new ArrayList<Long>(size);
    while (max > 0) {

      swap(data, max, Math.abs(random.nextInt(max)));
      list.add(data[max--]);
    }
    return list;
  }

  @Test
  public void testKeyAddDeleteRandom() {
    LinearHashingTable linearHashingTable;
    int seed = 0;
    int out = 0;
    while (seed < MAX_SEED) {
      try {
        linearHashingTable = new LinearHashingTable();
        List<Long> longs = getUniqueRandomValuesArray(seed, 2 * KEYS_COUNT);


        //add
        for (int i = 0; i < KEYS_COUNT; i++){
//      System.out.println(i + ", " + longs.get(i));
          linearHashingTable.put(longs.get(i));
        }

        //remove+add
        for (int i = 0; i < KEYS_COUNT; i++){
          if (i % 3 == 0) {
            Assert.assertTrue(linearHashingTable.delete(longs.get(i)));
          }

          if (i % 2 == 0) {
            Assert.assertTrue(linearHashingTable.put(longs.get(i + KEYS_COUNT)));
            Assert.assertTrue("i = " + i, linearHashingTable.contains(longs.get(KEYS_COUNT + i)));
          }
        }

        //check removed ok
        for (int i = 0; i < KEYS_COUNT; i++){
          if (i % 3 == 0)
            Assert.assertFalse(linearHashingTable.contains(longs.get(i)));
          else
            Assert.assertTrue(linearHashingTable.contains(longs.get(i)));

          if (i % 2 == 0)
            Assert.assertTrue(linearHashingTable.contains(longs.get(KEYS_COUNT + i)));
        }
      } catch (IndexOutOfBoundsException e) {
        out++;
      } finally {
        seed++;
        System.out.println(out + "/" + seed + " = " + (100.0 * out) / seed + "%");
      }
    }
  }

  @Test
  public void testNextHaveRightOrder() throws Exception {
    LinearHashingTable linearHashingTable;
    Random random;
    List<Long> keys = new ArrayList<Long>();
    long i = 0;//Long.MIN_VALUE;

    int stackCnt = 0;
    int groupCnt = 0;
    while (i < MAX_SEED) {
      try {
        linearHashingTable = new LinearHashingTable();
        random = new Random(i);
        keys.clear();

        while (keys.size() < KEYS_COUNT) {
          long key = random.nextLong();
//          if (key < 0)
//            key = -key;

          if (linearHashingTable.put(key)) {
            keys.add(key);
            Assert.assertTrue("key " + key, linearHashingTable.contains(key));
          }
        }

        Collections.sort(keys);


        Iterator<Long> iterator = linearHashingTable.iterator();
        for (Long key : keys){
          Assert.assertTrue("" + key, iterator.hasNext());
          Long lhKey = iterator.next();
          Assert.assertEquals("" + key, key, lhKey);
        }

        iterator = linearHashingTable.positiveIterator();
        for (Long key : keys){
          if (key < 0)
            continue;
          Assert.assertTrue("" + key, iterator.hasNext());
          Long lhKey = iterator.next();
          Assert.assertEquals("" + key, key, lhKey);
        }

        iterator = linearHashingTable.negativeIterator();
        for (Long key : keys){
          if (key >= 0) {
            Assert.assertFalse("" + key, iterator.hasNext());
          } else {
            Assert.assertTrue("" + key, iterator.hasNext());
            Long lhKey = iterator.next();
            Assert.assertEquals("" + key, key, lhKey);
          }
        }
      } catch (GroupOverflowException e) {
        groupCnt++;//Do nothing. This exception should occurs on non uniform distributed data.
      } catch (StackOverflowError e) {
        stackCnt++;
        //Do nothing. This exception should occurs on non uniform distributed data.
      } catch (Throwable e) {
        e.printStackTrace();
        break;
      } finally {
        i++;
        System.out.println(
            "i = " + i + " % " + ((100.0 * (stackCnt + groupCnt)) / i) + "(" + stackCnt + "+" + groupCnt + "/" + i + ")"
        );
      }
    }

  }

  private void swap(long[] data, int firstIndex, int secondIndex) {
    long tmp = data[firstIndex];
    data[firstIndex] = data[secondIndex];
    data[secondIndex] = tmp;
  }

}