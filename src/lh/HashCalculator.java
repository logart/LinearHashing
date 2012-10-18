package lh;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class HashCalculator {

  public static double calculateHashIn01Range(long key, int level) {
    double result = 1 - 1.0 / (Math.abs(key) + 1);
    assert result >=0;
    assert result < 1;
    return result;
  }

  public static int calculateHash(long key, int level) {
    return a(level, (int) Math.floor(Math.pow(2, level) * calculateHashIn01Range(key, level)));
  }

  private static int a(int level, int hash) {
    final int result;

    if ((hash % 2 == 0) && (level > 0))
      return a(level - 1, hash / 2);
    else
      result = (hash - 1) / 2 + (int) Math.pow(2, level - 1);

    return result;
  }

}
