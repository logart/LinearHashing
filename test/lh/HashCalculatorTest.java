package lh;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class HashCalculatorTest {

  public static final int N = 10000000;
  private Random random = new Random();
  private long[] counter = new long[10];

  @Test
  public void testCalculateHash() throws Exception {
    List<Long> array = new ArrayList<Long>(N);

    for (int i1 = 0, counterLength = counter.length; i1 < counterLength; i1++){
      counter[i1] = 0;
    }


    for (long i = 0; i < N; ++i){
      int hash = HashCalculator.calculateNaturalOrderedHash(i, 2);
//      System.out.println(hash + " " + HashCalculator.calculateHashIn01Range(i, -1));
      counter[hash]++;
    }

    for (long aCounter : counter){
      System.out.println(aCounter);
    }

    for (int i = 0; i < N; ++i){
      array.add(random.nextLong());
    }

    Collections.sort(array);
    for (int i1 = 0, counterLength = counter.length; i1 < counterLength; i1++){
      counter[i1] = 0;
    }

    for (int i = 0; i < N; ++i){
      int hash = HashCalculator.calculateNaturalOrderedHash(array.get(i), 2);
//      System.out.println(hash + " " + HashCalculator.calculateHashIn01Range(array.get(i), -1));
      counter[hash]++;
    }
    System.out.println("new!");
    for (long aCounter : counter){
      System.out.println(aCounter);
    }

    for (int i1 = 0, counterLength = counter.length; i1 < counterLength; i1++){
      counter[i1] = 0;
    }

    for (int i = 0; i < N; ++i){
//      double hash = HashCalculator.calculateHashIn01Range(array.get(i), 2);
//      System.out.println(hash + " " + HashCalculator.calculateHashIn01Range(array.get(i), -1));
//      counter[(int)Math.floor(hash*10)]++;
    }
    System.out.println("new!");
    for (long aCounter : counter){
      System.out.println(aCounter);
    }

  }
}
