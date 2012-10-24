package lh;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class HashCalculator {
  private static final double ELEMENTS_COUNT = Math.pow(2, 64);

  public static double calculateHashIn01Range(long key, int level) {

    double result = (key + (-(double) Long.MIN_VALUE)) / ELEMENTS_COUNT;

    assert result >= 0;
    assert result < 1;
    return result;
  }

  public static int calculateNaturalOrderedHash(long key, int level) {
    return (int) Math.floor(Math.pow(2, level) * calculateHashIn01Range(key, level));
  }

  public static int calculateBucketNumber(int hash, int level) {
    final int result;

    if (level == 0 && hash == 0)
      return 0;

    if ((hash % 2 == 0) && (level > 0))
      return calculateBucketNumber( hash / 2, level - 1);
    else
      result = (hash - 1) / 2 + (int) Math.pow(2, level - 1);

    assert result >= 0;
    return result;
  }

}
