package lh;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com
 *         Date: 8/28/12
 *         Time: 10:34 AM
 */
public class Index {
  private List<IndexElement> indexContent = new ArrayList<IndexElement>(2);

  public void addNewPosition() {
    indexContent.add(new IndexElement());
  }

  public void addNewPosition(int position) {
    indexContent.add(position, new IndexElement());
  }

  public int getChainDisplacement(int numberOfChain) {
//    System.out.println("chain " + numberOfChain + " size " + indexContent.size());
    return indexContent.get(numberOfChain).displacement & 0xFF;
  }

  public int getChainSignature(int numberOfChain) {
    return indexContent.get(numberOfChain).signature & 0xFF;
  }

  public int incrementChainDisplacement(int chainNumber, int bucketSize) {
    if ((indexContent.get(chainNumber).displacement & 0xFF) == 254 && bucketSize == Bucket.BUCKET_MAX_SIZE) {
      indexContent.get(chainNumber).displacement = (byte) 253;
    } else if ((indexContent.get(chainNumber).displacement & 0xFF) == 255) {
      indexContent.get(chainNumber).displacement = (byte) 254;
    }

    return indexContent.get(chainNumber).displacement & 0xFF;
  }

  public int decrementDisplacement(int chainNumber, int bucketSize, boolean nextIndexWasRemoved) {
    if ((indexContent.get(chainNumber).displacement & 0xFF) < 253 && nextIndexWasRemoved) {
      indexContent.get(chainNumber).displacement = (byte) 253;
    } else if ((indexContent.get(chainNumber).displacement & 0xFF) == 253 && bucketSize < Bucket.BUCKET_MAX_SIZE) {
      indexContent.get(chainNumber).displacement = (byte) 254;
    } else if ((indexContent.get(chainNumber).displacement & 0xFF) == 254 && bucketSize == 0) {
      indexContent.get(chainNumber).displacement = (byte) 255;
    }

    return indexContent.get(chainNumber).displacement & 0xFF;
  }

  public int decrementDisplacement(int chainNumber, int bucketSize) {
    return decrementDisplacement(chainNumber, bucketSize, false);
  }

  public void updateSignature(int chainNumber, long[] keys, int size) {
    int signature = 0;
    for (int i = 0; i < size; i++){
      if (signature < LinearHashingTableHelper.calculateSignature(keys[i])) {
        signature = LinearHashingTableHelper.calculateSignature(keys[i]);
      }
    }
    indexContent.get(chainNumber).signature = (byte) signature;
  }

  public void updateSignature(int bucketNumber, int signature) {
    indexContent.get(bucketNumber).signature = (byte) signature;
  }

  public void updateDisplacement(int chainNumber, byte displacement) {
    indexContent.get(chainNumber).displacement = displacement;
  }

  public void remove(int index) {
    indexContent.remove(index);
  }

  public int bucketCount() {
    return indexContent.size();
  }


  public void clearChainInfo(int chainNumber) {
    indexContent.get(chainNumber).displacement = (byte) 255;
    indexContent.get(chainNumber).signature = (byte) 255;
  }

  public void moveRecord(int oldPositionInIndex, int newPositionInIndex) {
//    System.out.print("index size " + indexContent.size());
    IndexElement indexElement = indexContent.get(oldPositionInIndex);
    indexContent.remove(indexElement);
    indexContent.add(newPositionInIndex, indexElement);
//    System.out.println(" index size after move " + indexContent.size());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0, indexContentSize = indexContent.size(); i < indexContentSize; i++){
      IndexElement indexElement = indexContent.get(i);
      builder.append("|\t\t").append(i).append("\t\t|\t\t").append(indexElement.displacement & 0xFF).append("\t\t|\t\t").append(indexElement.signature & 0xFF).append("\t\t|\n");
    }
    return builder.toString();
  }
}
