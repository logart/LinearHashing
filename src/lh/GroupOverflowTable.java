package lh;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com
 *         Date: 8/28/12
 *         Time: 10:25 AM
 */
public class GroupOverflowTable {
  List<GroupOverflowInfo> overflowInfo = new LinkedList<GroupOverflowInfo>();
  private static final byte DUMMY_GROUP_NUMBER = -1;

  public GroupOverflowTable() {
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, 2));
  }

  public int[] searchForGroupOrCreate(byte groupNumber, int groupSize) {
//    System.out.println("searchForGroupOrCreate");
    int dummyGroup = -1;
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++){
      GroupOverflowInfo groupOverflowInfo = overflowInfo.get(i);

      if (groupOverflowInfo.group == groupNumber) {
        return new int[]{groupOverflowInfo.startingPage, overflowInfo.get(i + 1).startingPage - groupOverflowInfo.startingPage};
      }

      if (dummyGroup == -1 && groupOverflowInfo.group == DUMMY_GROUP_NUMBER) {
        dummyGroup = i;
      }
    }

    if (dummyGroup == -1) {
      dummyGroup = overflowInfo.size() - 1;
    }

    //search is not successful so create new group on place of first dummy group
//    assert overflowInfo.get(dummyGroup).group == DUMMY_GROUP_NUMBER;
    overflowInfo.get(dummyGroup).group = groupNumber;

    createDummyGroupIfNeeded(groupSize);

//    System.out.println(this.toString());

    assert overflowInfo.get(dummyGroup).startingPage <= overflowInfo.get(overflowInfo.size() - 1).startingPage;
    return new int[]{overflowInfo.get(dummyGroup).startingPage, groupSize};
  }

  public int getPageForGroup(byte groupNumber) {
//    System.out.println("getPageForGroup");
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo){
      if (groupOverflowInfo.group == groupNumber) {
//        System.out.println(this.toString());
        return groupOverflowInfo.startingPage;
      }
    }
//    System.out.println(this.toString());
    return -1;
  }

  public GroupOverflowInfo findDummyGroup() {
    return findDummyGroup(0);
  }

  public int move(byte groupNumber, int groupSize) {
    removeGroup(groupNumber);
    GroupOverflowInfo dummyGroup = findDummyGroup(groupSize);
    dummyGroup.group = groupNumber;
    createDummyGroupIfNeeded(groupSize);
    return dummyGroup.startingPage;
  }

  private GroupOverflowInfo findDummyGroup(int minGroupSize) {
    //    System.out.println("findDummyGroup");
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++){
      if (
          (overflowInfo.get(i).group == DUMMY_GROUP_NUMBER) &&
              ((overflowInfo.get(i + 1).startingPage - overflowInfo.get(i).startingPage) >= minGroupSize)
          ) {
//        System.out.println(this.toString());
        return overflowInfo.get(i);
      }
    }

    if (overflowInfo.get(overflowInfo.size() - 1).group == DUMMY_GROUP_NUMBER) {
//      assert overflowInfo.get(0).group == DUMMY_GROUP_NUMBER;
      return overflowInfo.get(overflowInfo.size() - 1);
    }
//    System.out.println(this.toString());
    return null;

  }

  private void removeGroup(byte groupNumber) {
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo){
      if (groupOverflowInfo.group == groupNumber) {
        overflowInfo.remove(groupOverflowInfo);
        break;
      }
    }
  }

  private void createDummyGroupIfNeeded(int groupSize) {
    if (findDummyGroup() == null) {
      createDummyGroup(groupSize);
    }
    if (overflowInfo.get(overflowInfo.size()-1).group != DUMMY_GROUP_NUMBER) {
      createDummyGroup(groupSize);
    }
  }

  private void createDummyGroup(int groupSize) {
//    System.out.println("createDummyGroup");
    int startingPage = overflowInfo.get(overflowInfo.size() - 1).startingPage;
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, startingPage + groupSize));

//    System.out.println(this.toString());
  }

  public void moveDummyGroup(final int groupSize) {
//    System.out.println("moveDummyGroup");
    if (isSecondDummyGroupExists()) {
      removeGroup(DUMMY_GROUP_NUMBER);
    } else {
      findDummyGroup().startingPage = overflowInfo.get(overflowInfo.size() - 1).startingPage + groupSize;
    }
//    System.out.println(this.toString());
  }

  public void moveDummyGroupIfNeeded(int lastPage, int groupSize) {
//    System.out.println("moveDummyGroupIfNeeded");
//    System.out.println("lastPage " + lastPage + " groupSize " + groupSize);
    if (findDummyGroup().startingPage <= lastPage) {
      moveDummyGroup(groupSize);
      collapseDummyGroups();
    }

//    System.out.println(this.toString());
  }

  public void removeUnusedGroups(PageIndicator pageIndicator) {
//    System.out.println("removeUnusedGroups");
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++){
      for (int j = overflowInfo.get(i).startingPage; j < overflowInfo.get(i + 1).startingPage; ++j){
        if (pageIndicator.get(j)) {
          break;
        } else if (j == overflowInfo.get(i + 1).startingPage - 1) {
//          System.out.println("sp = " + overflowInfo.get(i).startingPage + " " + pageIndicator);
//          System.out.println("replace group " + overflowInfo.get(i).group + " with dummy");
          overflowInfo.get(i).group = DUMMY_GROUP_NUMBER;
        }
      }
    }

    collapseDummyGroups();
//    System.out.println(this.toString());
  }

  private void collapseDummyGroups() {
//    System.out.println("collapseDummyGroups");
    for (int i = overflowInfo.size() - 1; i > 0; --i){
      if (overflowInfo.get(i).group == DUMMY_GROUP_NUMBER && overflowInfo.get(i - 1).group == DUMMY_GROUP_NUMBER) {
        GroupOverflowInfo groupOverflowInfo = overflowInfo.get(i);
        overflowInfo.remove(groupOverflowInfo);
      }
    }
//    System.out.println(this.toString());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo){
      builder.append("|\t").append(groupOverflowInfo.group).append("\t|\t").append(groupOverflowInfo.startingPage).append("\t|\n");
    }
    return builder.toString();
  }

  public List<GroupOverflowInfo> getOverflowGroupsInfoToMove(int page) {
//    System.out.println("getOverflowGroupsInfoToMove");
    List<GroupOverflowInfo> result = new ArrayList<GroupOverflowInfo>(overflowInfo.size());
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo){
      if (groupOverflowInfo.startingPage <= page) {
        result.add(groupOverflowInfo);
      }
    }
//    System.out.println(this.toString());
    return result;
  }

  public boolean isSecondDummyGroupExists() {
    int counter = 0;
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo){
      if (groupOverflowInfo.group == DUMMY_GROUP_NUMBER) {
        counter++;
      }
    }
    return counter > 1;
  }

  public int getSizeForGroup(byte groupNumber) {
    for (int i = 0, overflowInfoSize = overflowInfo.size() - 1; i < overflowInfoSize; i++){
      GroupOverflowInfo groupOverflowInfo = overflowInfo.get(i);
      if (groupOverflowInfo.group == groupNumber) {
        return overflowInfo.get(i + 1).startingPage - groupOverflowInfo.startingPage;
      }
    }
    return -1;
  }

  public int enlargeGroupSize(final byte groupNumber, final int newGroupSize) {
    for (GroupOverflowInfo groupOverflowInfo : overflowInfo){
      if (groupNumber == groupOverflowInfo.group) {
        groupOverflowInfo.group = DUMMY_GROUP_NUMBER;
        break;
      }
    }
    assert overflowInfo.get(overflowInfo.size() - 1).group == DUMMY_GROUP_NUMBER;
    int newStartingPage = overflowInfo.get(overflowInfo.size() - 1).startingPage;
    overflowInfo.set(overflowInfo.size() - 1, new GroupOverflowInfo(groupNumber, newStartingPage));
    overflowInfo.add(new GroupOverflowInfo(DUMMY_GROUP_NUMBER, newStartingPage + newGroupSize));

    return newStartingPage;
  }
}
