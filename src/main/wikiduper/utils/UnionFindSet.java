package wikiduper.utils;

public class UnionFindSet {
    public DocSentence data;
    UnionFindSet parent;
    
    public UnionFindSet(DocSentence d){
        this.data = d;
        this.parent = this;
    }
    
    public static UnionFindSet find(UnionFindSet s){
        UnionFindSet testparent = s;
        while(testparent.parent != testparent){
            testparent = testparent.parent;
        }
        return testparent;
    }
    /*
    public static UnionFindSet merge(UnionFindSet s1, UnionFindSet s2){
        if(s1 == s2) return s1;
        UnionFindSet head1 = UnionFindSet.find(s1);
        UnionFindSet head2 = UnionFindSet.find(s2);
        head2.parent = head1;
        return head1;
    }
    */
    public static UnionFindSet merge(UnionFindSet s1, UnionFindSet s2){
        if(s1 == s2) return s1;
        UnionFindSet testparent = s2;
        UnionFindSet tmp;
        UnionFindSet head1 = UnionFindSet.find(s1);
        while(testparent.parent != testparent){
            tmp = testparent.parent;
            testparent.parent = head1;
            testparent = tmp;
        }

        testparent.parent = head1;
        return head1;
    }

    @Override
    public int hashCode(){
        return this.data.hashCode();
    }
    
    @Override
    public boolean equals(Object o){
        if(!(o instanceof UnionFindSet)){
            return false;
        }
        UnionFindSet s = (UnionFindSet) o;
        return s.data.equals(this.data);
    }
      
}
