package lh;

/**
 * @author Artem Loginov (logart) logart2007@gmail.com
 *         Date: 8/28/12
 *         Time: 10:28 AM
 */
public class GroupOverflowInfo {
    byte group;
    int startingPage;

    public GroupOverflowInfo(byte group, int startingPage) {
        this.group = group;
        this.startingPage = startingPage;
    }
}
