package wikiduper.clir.minhashwiki;

public class UnionFindSet {
    DocSentence data;
    UnionFindSet parent;
    
    public UnionFindSet(DocSentence d){
        this.data = d;
        this.parent = this;
    }
    
    
    public static UnionFindSet find(UnionFindSet s){
        if(s.parent == s) return s;
        return UnionFindSet.find(s.parent);
    }
    
    public static UnionFindSet merge(UnionFindSet s1, UnionFindSet s2){
        if(s1 == s2) return s1;
        UnionFindSet head1 = UnionFindSet.find(s1);
        UnionFindSet head2 = UnionFindSet.find(s2);
        head2.parent = head1;
        return head1;
    }
    
}
