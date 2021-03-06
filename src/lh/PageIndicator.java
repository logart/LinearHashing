package lh;

import java.util.BitSet;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class PageIndicator {
  BitSet indicator = new BitSet();

  public boolean get(int i) {
    return indicator.get(i);
  }

  public int getRealPosInSecondaryIndex(int page) {
    if (page < 0) {
      throw new RuntimeException("page number in page indicator should be positive");
    }
    if (!indicator.get(page)) {
      throw new RuntimeException("page which will be removed should be used before");
    }
    int index = 0;
    for (int i = 0; i < page; ++i){
      if (indicator.get(i)) {
        index++;
      }
    }
    return index;
  }

  public void set(int pageToUse) {
    assert !indicator.get(pageToUse);
    indicator.set(pageToUse);
  }

  public int getFirstEmptyPage(int group, int size) {
    for (int i = group; i < group + size; i++){
      if (!indicator.get(i))
        return i;
    }
    return -1;
  }

  public void unset(int pageToUse) {
    assert indicator.get(pageToUse);
    indicator.set(pageToUse, false);
  }

  @Override
  public String toString() {
    return indicator.toString();
  }
}
