import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MyComparator implements Comparator<SortBean> {

    public int compare(SortBean o1, SortBean o2) {
        if(o1.name.equals(o2.name)==true){
            return o1.score-o2.score;
        }
        return o1.name.compareTo(o2.name);
    }

    //测试如下
    public static void main(String[] args) {
        MyComparator comparator = new MyComparator();
        List<SortBean> list = new ArrayList<SortBean>();
        list.add(new SortBean("hadoop",100));
        list.add(new SortBean("spark",200));
        list.add(new SortBean("spark",300));
        list.add(new SortBean("hadoop",200));

        Collections.sort(list);
        for (SortBean bean : list){
            System.out.println(bean);
        }
    }
}
