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
    int hash = calculateHash(key);

    final boolean result = tryInsertIntoChain(hash, key);
    if (result) {
      size++;
    }
    splitBucketsIfNeeded();

    return result;
  }

  private int calculateHash(long key) {
    int internalHash = HashCalculator.calculateInternalHash(key, level);
    if (internalHash < next || (internalHash == next && isSplitting)) {
      internalHash = calculateNextHash(key);
    }
    return internalHash;
  }

  private int calculateNextHash(long key) {
    return HashCalculator.calculateInternalHash(key, level + 1);
  }

  private boolean tryInsertIntoChain(final int keyHash, long key) {
    int bucketNumber = HashCalculator.calculateBucketNumber(keyHash, calculateActualLevel(keyHash));
    int chainDisplacement = primaryIndex.getChainDisplacement(keyHash);

    //try to store record in main bucket
    int pageToStore;
    final int keySignature;

    if (chainDisplacement > 253) {
//      System.out.println("chainDisplacement " + chainDisplacement);
      return storeRecordInMainBucket(keyHash, key);
    } else {
      int chainSignature = primaryIndex.getChainSignature(keyHash);
      keySignature = LinearHashingTableHelper.calculateSignature(key);
      if (keySignature < chainSignature) {
        moveLargestRecordToRecordPool(bucketNumber, (byte) chainSignature);
        final boolean result = storeRecordInMainBucket(keyHash, key);
        storeRecordFromRecordPool();
        return result;
      } else if (keySignature == chainSignature) {
        recordPool.add(key);
        size--;
        moveLargestRecordToRecordPool(bucketNumber, (byte) chainSignature);
        Bucket bucket = file.get(bucketNumber);

        primaryIndex.updateSignature(keyHash, bucket.keys, bucket.size);

        storeRecordFromRecordPool();
        return true;
      } else {
        if (chainDisplacement == 253) {
          //allocate new page

          return allocateNewPageAndStore(keyHash, keyHash, key, true);

        } else {
          pageToStore = findNextPageInChain(keyHash, chainDisplacement);
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
            return allocateNewPageAndStore(keyHash, realPosInSecondaryIndex, key, false);
          } else {
            pageToStore = findNextPageInChain(keyHash, chainDisplacement);
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
      loadChainInPool(next);
      int pageToStore = next + (int) Math.pow(2, level);
      int groupSize = calculateGroupSize(level + 1);
      boolean needMove = false;
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
    System.out.println("merge " + capacity);
    if (capacity < minCapacity) {
      System.out.println("in");
      //TODO make this durable by inventing cool record pool
      int chainNumber = primaryIndex.bucketCount() - 1;
      loadChainInPool(chainNumber);
      primaryIndex.remove(chainNumber);
      //todo remove secondary index info

      //todo remove pageIndicator info
      file.set(chainNumber, null);

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

      int hash = calculateNextHash(key);

      final boolean result = tryInsertIntoChain(hash, key);
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
  }

  private void loadChainInPool(final int keyHash) {
    int bucketNumber = HashCalculator.calculateBucketNumber(keyHash, calculateActualLevel(keyHash));
    Collection<? extends Long> content = file.get(bucketNumber).getContent();
    size -= content.size();
    chainPool.addAll(content);
    file.get(bucketNumber).emptyBucket();

    int displacement = primaryIndex.getChainDisplacement(keyHash);
    int pageToUse;

    while (displacement < 253) {
      pageToUse = findNextPageInChain(keyHash, displacement);

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
    primaryIndex.clearChainInfo(keyHash);
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

  private int findNextPageInChain(int keyHash, int chainDisplacement) {
    byte groupNumber = calculateGroupNumber(keyHash);
    int startingPage = groupOverflowTable.getPageForGroup(groupNumber);
    if (startingPage == -1) {
//            System.out.println("groupNumber " + groupNumber);
//            System.out.println(groupOverflowTable);
      return -1;
    }
    return startingPage + chainDisplacement;
  }

  private boolean allocateNewPageAndStore(int keyHash, int positionInIndex, long key, boolean mainChain) {
    int groupSize = calculateGroupSize(calculateActualLevel(keyHash));
    byte groupNumber = calculateGroupNumber(keyHash);

    int[] pos = groupOverflowTable.searchForGroupOrCreate(groupNumber, groupSize);

    int pageToUse = pageIndicator.getFirstEmptyPage(pos[0], pos[1]);

    if (pageToUse == -1) {
      throw new GroupOverflowException(
          "There is no empty page for group size " + groupSize + " because pages " + pageIndicator.toString() + " are already allocated." +
              "Starting page is " + pos[0]
      );
    }

    if (mainChain) {
//            System.out.println((pageToUse - pos[0]) + " but " + (byte) (pageToUse - pos[0]));
      primaryIndex.updateDisplacement(positionInIndex, (byte) (pageToUse - pos[0]));
    } else {
//      System.out.println((pageToUse - pos[0]) + " but " + (byte) (pageToUse - pos[0]));
      secondaryIndex.updateDisplacement(positionInIndex, (byte) (pageToUse - pos[0]));
    }

    //TODO right order in secondary index
    pageIndicator.set(pageToUse);
    int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageToUse);
    secondaryIndex.addNewPosition(realPosInSecondaryIndex);
    Bucket bucket = new Bucket();
    file.set(pageToUse, bucket);


    return storeRecordInOverflowBucket(pageToUse, key);
  }

  private int calculateActualLevel(int keyHash) {
    return ((keyHash < next || isSplitting) || keyHash >= (CHAIN_NUMBER * Math.pow(2, level))) ? level + 1 : level;
  }

  //TODO fix this
  private boolean storeRecordInMainBucket(final int keyHash, long key) {
    int bucketNumber = HashCalculator.calculateBucketNumber(keyHash, calculateActualLevel(keyHash));

    Bucket bucket = file.get(bucketNumber);

    for (int i = 0; i < bucket.size; i++){
      if (bucket.keys[i] == key) {
        return false;
      }
    }


    bucket.keys[bucket.size] = key;
    bucket.size++;

    int displacement = primaryIndex.incrementChainDisplacement(keyHash, bucket.size);
    if (bucket.size == Bucket.BUCKET_MAX_SIZE && displacement > 253) {
      throw new RuntimeException("this can't be true");
    }
    if (displacement <= 253) {
      primaryIndex.updateSignature(keyHash, bucket.keys, bucket.size);
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

  private byte calculateGroupNumber(int keyHash) {
    final int currentLevel;
    final int groupSize;

    if ((keyHash < next || isSplitting) || keyHash >= (CHAIN_NUMBER * Math.pow(2, level))) {
      currentLevel = level + 1;
    } else {
      currentLevel = level;
    }
    groupSize = calculateGroupSize(currentLevel);

    int x = (int) (CHAIN_NUMBER * Math.pow(2, currentLevel) + keyHash - 1);
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
    final int hash = calculateHash(key);
    final int bucketNumber = HashCalculator.calculateBucketNumber(hash, calculateActualLevel(hash));

    int keySignature = LinearHashingTableHelper.calculateSignature(key) & 0xFF;
    int signature = primaryIndex.getChainSignature(hash);
    int pageNumberToUse = bucketNumber;

    int chainDisplacement = primaryIndex.getChainDisplacement(hash);
    byte groupNumber = calculateGroupNumber(hash);
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
    final int hash = calculateHash(key);

    int keySignature = LinearHashingTableHelper.calculateSignature(key) & 0xFF;
    int signature = primaryIndex.getChainSignature(hash);
    int pageNumberToUse = hash;

    int chainDisplacement = primaryIndex.getChainDisplacement(hash);
    byte groupNumber = calculateGroupNumber(hash);
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
        final Bucket originalBucket = bucket;
        int position = bucket.deleteKey(key);
        if (position >= 0) {
          //move record from successor to current bucket
          int prevPage = pageNumberToUse;
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

              secondaryIndex.updateSignature(realPosInSecondaryIndex, secondBucket.keys, secondBucket.size);
              bucket = secondBucket;
            }
          }

          primaryIndex.updateSignature(hash, originalBucket.keys, originalBucket.size);

          if (pageNumberToUse == hash) {
            //main bucket does not have overflow chain
            primaryIndex.decrementDisplacement(hash, bucket.size);
          } else {
            int realPosInSecondaryIndex = pageIndicator.getRealPosInSecondaryIndex(pageNumberToUse);
            if (bucket.size == 0) {
              secondaryIndex.remove(realPosInSecondaryIndex);
              pageIndicator.unset(pageNumberToUse);
              //set prev bucket in chain correct displacement
              if (prevPage == hash) {
                primaryIndex.decrementDisplacement(hash, file.get(hash).size, true);
              } else {
                secondaryIndex.decrementDisplacement(
                    pageIndicator.getRealPosInSecondaryIndex(prevPage),
                    file.get(prevPage).size,
                    true
                );
              }
            } else {
              secondaryIndex.decrementDisplacement(realPosInSecondaryIndex, bucket.size);
            }
          }
        } else {
          //return false because nothing was found
          return false;
        }
        size--;
        mergeBucketIfNeeded();
        return true;
      }
    }
  }
}
