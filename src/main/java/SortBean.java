public class SortBean implements Comparable<SortBean>{

    String name;
    int score;

    public SortBean(String name,int score) {
        this.name = name;
        this.score = score;
    }

    public int compareTo(SortBean o) {
        //字符内容相同，按照整形比较大小
        if(this.name.equals(o.name)){
            return this.score-o.score;
        }
        //字符内容不同,按照字符内容比较
        return this.name.compareTo(o.name);
    }

    @Override
    public String toString() {
        return name+"\t"+score;
    }
}
