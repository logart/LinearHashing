package lh;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Andrey Lomakin
 * @since 23.07.12
 */
public class LinearHashingTable {

  private static final int FILE_SIZE = 4096;

  private Index primaryIndex;
  private Index secondaryIndex;
  private final int CHAIN_NUMBER;
  private int level;
  private int next;
  private double maxCapacity;
  private double minCapacity;
  private GroupOverflowTable groupOverflowTable;
  private List<Bucket> file;
  private PageIndicator pageIndicator;
  private List<Long> recordPool = new ArrayList<Long>(100);
  private List<Long> chainPool = new ArrayList<Long>(100);
  private int size;
  private boolean isSplitting;
  private static final int MAX_GROUP_SIZE = 128;


  public LinearHashingTable() {
    CHAIN_NUMBER = 1;
    size = 0;
    level = 0;
    next = 0;
    maxCapacity = 0.8;
    minCapacity = 0.4;
    primaryIndex = new Index();
    secondaryIndex = new Index();
    groupOverflowTable = new GroupOverflowTable();
    pageIndicator = new PageIndicator();

    file = new ArrayList<Bucket>(FILE_SIZE);
    for (int i = 0; i < FILE_SIZE; ++i){
      file.add(null);
    }

    file.set(0, new Bucket());

    primaryIndex.addNewPosition();

  }

  public boolean put(long key) {
    if (contains(key)) {
      return false;
    }
    int bucketNumber = calculateHash(key);

    final boolean result = tryInsertIntoChain(bucketNumber, key);
    if (result) {
      size++;
    }
    splitBucketsIfNeeded();

    return result;
  }

  private int currentLevel(int naturalOrderedKey) {
    if (naturalOrderedKey < next || (naturalOrderedKey == next && isSplitting)) {
      return level + 1;
    } else {
      return level;
    }
  }

  private int calculateHash(long key) {
    int internalHash = HashCalculator.calculateNaturalOrderedHash(key, level);

    final int bucketNumber;
    if (internalHash < next || (internalHash == next && isSplitting)) {
      bucketNumber = calculateNextHash(key);
    } else {
      bucketNumber = HashCalculator.calculateBucketNumber(internalHash, level);
    }


    return bucketNumber;
  }

  private int calculateNextHash(long key) {
    int keyHash = HashCalculator.calculateNaturalOrderedHash(key, level + 1);
    return HashCalculator.calculateBucketNumber(keyHash, level + 1);

  }

  private boolean tryInsertIntoChain(final int bucketNumber, long key) {
    int chainDisplacement = primaryIndex.getChainDisplacement(bucketNumber);

    //try to store record in main bucket
    int pageToStore;
    final int keySignature;

    if (chainDisplacement > 253) {
//      System.out.println("chainDisplacement " + chainDisplacement);
      return storeRecordInMainBucket(bucketNumber, key);
    } else {
      int chainSignature = primaryIndex.getChainSignature(bucketNumber);
      keySignature = LinearHashingTableHelper.calculateSignature(key);
      if (keySignature < chainSignature) {
        moveLargestRecordToRecordPool(bucketNumber, (byte) chainSignature);
        final boolean result = storeRecordInMainBucket(bucketNumber, key);
        storeRecordFromRecordPool();
        return result;
      } else if (keySignature == chainSignature) {
        recordPool.add(key);
        size--;
        moveLargestRecordToRecordPool(bucketNumber, (byte) chainSignature);
        Bucket bucket = file.get(bucketNumber);

        primaryIndex.updateSignature(bucketNumber, bucket.keys, bucket.size);

        storeRecordFromRecordPool();
        return true;
      } else {
        if (chainDisplacement == 253) {
          //allocate new page

          return allocateNewPageAndStore(bucketNumber, bucketNumber, key, true);

        } else {
          pageToStore = findNextPageInChain(bucketNumber, HashCalculator.calculateNaturalOrderedHash(key, level), chainDisplacement);
//                    System.out.println("pageToStore " + pageToStore + " chainNumber " + chainNumber + "/" + primaryIndex.bucketCount() + " chainDisplacement " + chainDisplacement);
//                    System.out.println();
        }
      }
    }

    //try to store in overflow bucket chain
    while (true) {
//            System.out.println("pts " + pageToStore + " size " + file.get(chainNumber).size);
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToStore);
//            System.out.println("rpisi " + realPosInSecondaryIndex + " " + pageIndicator);
      chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
//            System.out.println("cd " + realPosInSecondaryIndex + " cnt " + cnt);


      if (chainDisplacement > 253) {
        return storeRecordInOverflowBucket(pageToStore, key);
      } else {
        int chainSignature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
        if (keySignature < chainSignature) {
          moveLargestRecordToRecordPool(pageToStore, (byte) chainSignature);
          final boolean result = storeRecordInOverflowBucket(pageToStore, key);
          storeRecordFromRecordPool();
          return result;
        } else if (keySignature == chainSignature) {
          recordPool.add(key);
          size--;
          moveLargestRecordToRecordPool(pageToStore, (byte) chainSignature);
          Bucket bucket = file.get(pageToStore);

          secondaryIndex.updateSignature(realPosInSecondaryIndex, bucket.keys, bucket.size);

          storeRecordFromRecordPool();
          return true;
        } else {
          if (chainDisplacement == 253) {
            //allocate new page
            return allocateNewPageAndStore(bucketNumber, pageToStore, key, false);
          } else {
            pageToStore = findNextPageInChain(bucketNumber, HashCalculator.calculateNaturalOrderedHash(key, level), chainDisplacement);
          }
        }
      }
    }
  }

  private void splitBucketsIfNeeded() {
    //calculate load factor
    double capacity = ((double) size) / (primaryIndex.bucketCount() * Bucket.BUCKET_MAX_SIZE);
    if (capacity > maxCapacity) {
      int bucketNumberToSplit = HashCalculator.calculateBucketNumber(next, level);
      //TODO make this durable by inventing cool record pool
      loadChainInPool(bucketNumberToSplit, next);
      int pageToStore = next + (int) Math.pow(2, level);
      int groupSize = calculateGroupSize(level + 1);
      boolean needMove = false;
      //TODO use group size from group overflow to prevent collisions
      for (int i = pageToStore; i < pageToStore + groupSize; ++i){
        if (pageIndicator.get(i)) {
          needMove = true;
          break;
        }
      }

      if (needMove) {
//                System.out.println("next " + next);
//                System.out.println("pageToStore " + pageToStore);
//                System.out.println(groupOverflowTable);
        moveOverflowGroupToNewPosition(pageToStore);
//                System.out.println(groupOverflowTable);
      }

      //TODO review
      primaryIndex.addNewPosition(pageToStore);
      file.set(pageToStore, new Bucket());
      groupOverflowTable.moveDummyGroupIfNeeded(pageToStore, groupSize);

      isSplitting = true;
      storeRecordFromChainPool();
      isSplitting = false;

      next++;
      if (next == (CHAIN_NUMBER * Math.pow(2, level))) {
        next = 0;
        level++;
      }
    }
  }

  private void mergeBucketIfNeeded() {
    //calculate load factor
    double capacity = ((double) size) / (primaryIndex.bucketCount() * Bucket.BUCKET_MAX_SIZE);
    if (capacity < minCapacity) {
      System.out.println("in");
      //TODO make this durable by inventing cool record pool
      int naturalOrderKey = primaryIndex.bucketCount() - 1;
      int bucketNumberToMerge = HashCalculator.calculateBucketNumber(next, level);

      loadChainInPool(bucketNumberToMerge, naturalOrderKey);
      primaryIndex.remove(bucketNumberToMerge);
      //todo remove secondary index info

      //todo remove pageIndicator info
      file.set(bucketNumberToMerge, null);

      isSplitting = true;
      storeRecordFromChainPool();
      isSplitting = false;

      next--;
      if (next < 0) {
        level--;
        next = (int) (CHAIN_NUMBER * Math.pow(2, level) - 1);
      }
    }
  }

  private void storeRecordFromChainPool() {
    while (!chainPool.isEmpty()) {
      Long key = chainPool.get(0);
      chainPool.remove(key);

      int bucketNumber = calculateNextHash(key);

      assert bucketNumber == HashCalculator.calculateBucketNumber(next, level) || bucketNumber == next + Math.pow(2, level);

      final boolean result = tryInsertIntoChain(bucketNumber, key);
      if (result) {
        size++;
      } else {
        throw new RuntimeException("error while saving records from pool");
      }
    }
  }

  private void moveOverflowGroupToNewPosition(int page) {
    List<GroupOverflowInfo> groupsToMove = groupOverflowTable.getOverflowGroupsInfoToMove(page);

    for (GroupOverflowInfo groupOverflowInfo : groupsToMove){
      int oldPage = groupOverflowInfo.startingPage;
      int groupSize = groupOverflowTable.getSizeForGroup(groupOverflowInfo.group);
      int newPage = groupOverflowTable.move(groupOverflowInfo.group, groupSize);

      moveGroupToNewPosition(oldPage, newPage, groupSize);

    }
  }

  private void moveGroupToNewPosition(int oldPage, int newPage, int groupSize) {
    for (int i = oldPage; i < oldPage + groupSize; ++i){
      if (pageIndicator.get(i)) {
        Bucket bucket = file.get(i);
        file.set(i - oldPage + newPage, bucket);
//                    System.out.println("null page " + i + " but size is " + primaryIndex.bucketCount());
        file.set(i, null);
        //move resords in secondary index
        int oldPositionInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(i);
        pageIndicator.set(i - oldPage + newPage);
        pageIndicator.unset(i);
        int newPositionInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(i - oldPage + newPage);

        secondaryIndex.moveRecord(oldPositionInSecondaryIndex, newPositionInSecondaryIndex);
      }
    }
  }

  private void loadChainInPool(final int bucketNumber, final int naturalOrderedKey) {
    Collection<? extends Long> content = file.get(bucketNumber).getContent();
    size -= content.size();
    chainPool.addAll(content);
    file.get(bucketNumber).emptyBucket();

    int displacement = primaryIndex.getChainDisplacement(bucketNumber);
    int pageToUse;

    while (displacement < 253) {
      pageToUse = findNextPageInChain(bucketNumber, naturalOrderedKey, displacement);

      content = file.get(pageToUse).getContent();
      size -= content.size();
      chainPool.addAll(content);
      file.get(pageToUse).emptyBucket();


      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
      displacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);

      //free index
      pageIndicator.unset(pageToUse);
      secondaryIndex.remove(realPosInSecondaryIndex);
    }

    groupOverflowTable.removeUnusedGroups(pageIndicator);
    primaryIndex.clearChainInfo(bucketNumber);
  }

  private void storeRecordFromRecordPool() {
    while (!recordPool.isEmpty()) {
      Long key = recordPool.get(0);
      recordPool.remove(key);

      //TODO be sure that key successfully stored
      if (!put(key)) {
        throw new RuntimeException("error while saving records from pool");
      }
    }
  }

  private void moveLargestRecordToRecordPool(int chainNumber, byte signature) {
    Bucket bucket = file.get(chainNumber);
    List<Long> largestRecords = bucket.getLargestRecords(signature);
    recordPool.addAll(largestRecords);
    size -= largestRecords.size();
  }

  private int findNextPageInChain(int bucketNumber, int naturalOrderedKey, int chainDisplacement) {
    byte groupNumber = calculateGroupNumber(bucketNumber, currentLevel(naturalOrderedKey));
    int startingPage = groupOverflowTable.getPageForGroup(groupNumber);
    if (startingPage == -1) {
//            System.out.println("groupNumber " + groupNumber);
//            System.out.println(groupOverflowTable);
      return -1;
    }
    return startingPage + chainDisplacement;
  }

  private boolean allocateNewPageAndStore(int bucketNumber, int pageToStore, long key, boolean mainChain) {
    //todo review this level very carefully
    int groupSize = calculateGroupSize(level);
    byte groupNumber = calculateGroupNumber(bucketNumber, currentLevel(HashCalculator.calculateNaturalOrderedHash(key, level)));

    int[] pos = groupOverflowTable.searchForGroupOrCreate(groupNumber, groupSize);

    int pageToUse = pageIndicator.getFirstEmptyPage(pos[0], pos[1]);

    int actualStartingPage = pos[0];

    if (pageToUse == -1) {
      if (pos[1] == MAX_GROUP_SIZE) {
        throw new GroupOverflowException(
            "There is no empty page for group size " + groupSize + " because pages " + pageIndicator.toString() + " are already allocated." +
                "Starting page is " + pos[0]
        );
      } else {
        groupSize = pos[1] * 2;
        int newStartingPage = groupOverflowTable.enlargeGroupSize(groupNumber, groupSize);
        moveGroupToNewPosition(pos[0], newStartingPage, pos[1]);
        pageToUse = pageIndicator.getFirstEmptyPage(newStartingPage, groupSize);
        actualStartingPage = newStartingPage;
      }
    }

    //update displacement of existing index element
    if (mainChain) {
//            System.out.println((pageToUse - pos[0]) + " but " + (byte) (pageToUse - pos[0]));
      primaryIndex.updateDisplacement(pageToStore, (byte) (pageToUse - actualStartingPage));
    } else {
//      System.out.println((pageToUse - pos[0]) + " but " + (byte) (pageToUse - pos[0]));
      int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(actualStartingPage + pageToStore - pos[0]);
      secondaryIndex.updateDisplacement(realPosInSecondaryIndex, (byte) (pageToUse - actualStartingPage));
    }

    pageIndicator.set(pageToUse);
    int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
    secondaryIndex.addNewPosition(realPosInSecondaryIndex);
    Bucket bucket = new Bucket();
    file.set(pageToUse, bucket);


    return storeRecordInOverflowBucket(pageToUse, key);
  }

  private boolean storeRecordInMainBucket(final int bucketNumber, long key) {

    Bucket bucket = file.get(bucketNumber);

    for (int i = 0; i < bucket.size; i++){
      if (bucket.keys[i] == key) {
        return false;
      }
    }


    bucket.keys[bucket.size] = key;
    bucket.size++;

    int displacement = primaryIndex.incrementChainDisplacement(bucketNumber, bucket.size);
    if (bucket.size == Bucket.BUCKET_MAX_SIZE && displacement > 253) {
      throw new RuntimeException("this can't be true");
    }
    if (displacement <= 253) {
      primaryIndex.updateSignature(bucketNumber, bucket.keys, bucket.size);
    }
    return true;
  }

  private boolean storeRecordInOverflowBucket(int page, long key) {
    Bucket bucket = file.get(page);


    for (int i = 0; i < bucket.size; i++){
      if (bucket.keys[i] == key) {
        return false;
      }
    }

    bucket.keys[bucket.size] = key;
    bucket.size++;

    int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(page);
    int displacement = secondaryIndex.incrementChainDisplacement(realPosInSecondaryIndex, bucket.size);
    if (bucket.size == Bucket.BUCKET_MAX_SIZE && displacement > 253) {
      throw new RuntimeException("this can't be true");
    }
    if (displacement <= 253) {
      secondaryIndex.updateSignature(realPosInSecondaryIndex, bucket.keys, bucket.size);
    }
    return true;
  }

  private byte calculateGroupNumber(int bucketNumber, int currentLevel) {
    final int groupSize;

    groupSize = calculateGroupSize(currentLevel);

    int x = (int) (CHAIN_NUMBER * Math.pow(2, currentLevel) + bucketNumber - 1);
    int y = x / groupSize;
    return (byte) (y % 31);
  }

  private int calculateGroupSize(final int iLevel) {
    int divisor = 0;
    byte no = -1;
    double nogps;
    do {
      if (divisor < 128) {
        no++;
      }
      divisor = (int) Math.pow(2, 2 + no);
      nogps = (CHAIN_NUMBER * Math.pow(2, iLevel)) / divisor;

    } while (!((nogps <= 31) || (divisor == 128)));

    return divisor;
  }

  public boolean contains(long key) {
    final int bucketNumber = calculateHash(key);

    int keySignature = LinearHashingTableHelper.calculateSignature(key) & 0xFF;
    int signature = primaryIndex.getChainSignature(bucketNumber);
    int pageNumberToUse = bucketNumber;

    int chainDisplacement = primaryIndex.getChainDisplacement(bucketNumber);
    byte groupNumber = calculateGroupNumber(bucketNumber, currentLevel(HashCalculator.calculateNaturalOrderedHash(key, level)));
    int pageNumber = groupOverflowTable.getPageForGroup(groupNumber);

    while (true) {
      if (keySignature > signature) {
        if (chainDisplacement >= 253) {
          return false;
        } else {
          pageNumberToUse = pageNumber + chainDisplacement;
          int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
          signature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
          chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
        }
      } else {
        Bucket bucket = file.get(pageNumberToUse);
        return bucket != null && bucket.searchForItem(key);
      }

    }
  }

  public boolean delete(long key) {
    final int bucketNumber = calculateHash(key);

    int keySignature = LinearHashingTableHelper.calculateSignature(key) & 0xFF;
    int signature = primaryIndex.getChainSignature(bucketNumber);
    int pageNumberToUse = bucketNumber;

    int chainDisplacement = primaryIndex.getChainDisplacement(bucketNumber);
    byte groupNumber = calculateGroupNumber(bucketNumber, currentLevel(HashCalculator.calculateNaturalOrderedHash(key, level)));
    int pageNumber = groupOverflowTable.getPageForGroup(groupNumber);

    int prevPage = bucketNumber;
    while (true) {
      if (keySignature > signature) {
        if (chainDisplacement >= 253) {
          return false;
        } else {
          prevPage = pageNumberToUse;
          pageNumberToUse = pageNumber + chainDisplacement;
          int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
          signature = secondaryIndex.getChainSignature(realPosInSecondaryIndex);
          chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
        }
      } else {
        Bucket bucket = file.get(pageNumberToUse);
        int position = bucket.deleteKey(key);
        if (position >= 0) {
          //move record from successor to current bucket
          while (chainDisplacement < 253) {
            prevPage = pageNumberToUse;
            pageNumberToUse = pageNumber + chainDisplacement;
            int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
            chainDisplacement = secondaryIndex.getChainDisplacement(realPosInSecondaryIndex);
            Bucket secondBucket = file.get(pageNumberToUse);
            List<Long> smallestRecords = secondBucket.getSmallestRecords(Bucket.BUCKET_MAX_SIZE - bucket.size);
            if (smallestRecords.isEmpty()) {
              //do nothing!
            } else {
              //move this records to predecessor
              bucket.add(smallestRecords);

              for (Long smallestRecord : smallestRecords){
                if (secondBucket.deleteKey(smallestRecord) < 0) {
                  throw new IllegalStateException("error while deleting record to move it to predecessor bucket");
                }
              }
            }

            //update signatures after removing some records from buckets
            if (prevPage == bucketNumber) {
              if (primaryIndex.getChainDisplacement(bucketNumber) > 253) {
                primaryIndex.updateSignature(bucketNumber, 255);
              } else {
                primaryIndex.updateSignature(bucketNumber, bucket.keys, bucket.size);
              }
            } else {
              int indexPosition = pageIndicator.getRealPosInSecondaryIndex(prevPage);
              if (primaryIndex.getChainDisplacement(bucketNumber) > 253) {
                secondaryIndex.updateSignature(indexPosition, 255);
              } else {
                secondaryIndex.updateSignature(indexPosition, bucket.keys, bucket.size);
              }
            }
            bucket = secondBucket;
          }

          //update displacement and signature in last bucket
          if (pageNumberToUse == bucketNumber) {
            //main bucket does not have overflow chain
            int displacement = primaryIndex.decrementDisplacement(bucketNumber, bucket.size);
            if (displacement <= 253) {
              primaryIndex.updateSignature(bucketNumber, bucket.keys, bucket.size);
            } else {
              primaryIndex.updateSignature(bucketNumber, 255);
            }
          } else {
            int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
            if (bucket.size == 0) {
              secondaryIndex.remove(realPosInSecondaryIndex);
              pageIndicator.unset(pageNumberToUse);
              //set prev bucket in chain correct displacement
              if (prevPage == bucketNumber) {
                int displacement = primaryIndex.decrementDisplacement(bucketNumber, file.get(bucketNumber).size, true);
                if (displacement > 253) {
                  primaryIndex.updateSignature(bucketNumber, 255);
                }
              } else {
                int prevIndexPosition = pageIndicator.getRealPosInSecondaryIndex(prevPage);
                int displacement = secondaryIndex.decrementDisplacement(
                    prevIndexPosition,
                    file.get(prevPage).size,
                    true
                );
                if (displacement > 253) {
                  secondaryIndex.updateSignature(prevIndexPosition, 255);
                }
              }
            } else {
              int displacement = secondaryIndex.decrementDisplacement(realPosInSecondaryIndex, bucket.size);
              if (displacement <= 253) {
                secondaryIndex.updateSignature(realPosInSecondaryIndex, bucket.keys, bucket.size);
              } else {
                secondaryIndex.updateSignature(realPosInSecondaryIndex, 255);
              }
            }
          }
        } else {
          //return false because nothing was found
          return false;
        }
        size--;
//        mergeBucketIfNeeded();
        return true;
      }
    }
  }
}
