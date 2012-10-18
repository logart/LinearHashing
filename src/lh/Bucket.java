package lh;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Andrey Lomakin
 * @since 23.07.12
 */
public class Bucket {
  public static final int BUCKET_MAX_SIZE = 64;

  long[] keys = new long[BUCKET_MAX_SIZE];
  int size;

  public boolean searchForItem(long key) {
    for (int i = 0; i < size; i++){
      if (keys[i] == key) {
        return true;
      }
    }
    return false;
  }

  public List<Long> getLargestRecords(final byte iSignature) {
    int signature = iSignature & 0xFF;
    List<Long> result = new ArrayList<Long>(size / 10);
    for (int i = 0; i < size; ){
      if (LinearHashingTableHelper.calculateSignature(keys[i]) == signature) {
        result.add(keys[i]);
        --size;
        keys[i] = keys[size];
        keys[size] = 0;
      } else {
        ++i;
      }
    }

    assert !result.isEmpty();

    return result;
  }

  public Collection<? extends Long> getContent() {
    List<Long> list = new ArrayList<Long>(keys.length);
    for (int i = 0; i < size; i++){
      list.add(keys[i]);
    }
    return list;
  }

  public void emptyBucket() {
    size = 0;
  }

  public int deleteKey(long key) {
    for (int i = 0; i < size; i++){
      if (keys[i] == key) {
        keys[i] = keys[size - 1];
        size--;
        return i;
      }
    }
    return -1;
  }

  public List<Long> getSmallestRecords(int iMaxSizeOfRecordsArray) {
    int signature = 255;
    List<Long> result = new ArrayList<Long>(size / 10);
    for (int i = 0; i < size; ++i){
      int keySignature = LinearHashingTableHelper.calculateSignature(keys[i]);
      if (keySignature < signature) {
        signature = keySignature;
        result.clear();
        result.add(keys[i]);
      } else if (keySignature == signature) {
        result.add(keys[i]);
      }
    }

    assert !result.isEmpty();

    if (result.size() > iMaxSizeOfRecordsArray) {
      return new ArrayList<Long>();
    } else {
      return result;
    }
  }

  public void add(List<Long> smallestRecords) {
    if (smallestRecords.size() > (BUCKET_MAX_SIZE - size)) {
      throw new IllegalArgumentException("array size should be less than existing free space in bucket");
    } else {
      for (Long smallestRecord : smallestRecords){
        keys[size] = smallestRecord;
        size++;
      }

    }
  }
}