package lh;

import java.util.Random;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com
 *         Date: 8/28/12
 *         Time: 12:24 PM
 */
public class LinearHashingTableHelper {
  private static Random random = new Random();

  public static int calculateSignature(long key) {

    if (key == -1) {
      return 255;
    }

    random.setSeed(key);

    return random.nextInt()& 0xFF;
  }
}
