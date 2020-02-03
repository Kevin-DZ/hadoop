package com.day6.TopSogou;

import com.day6.javabean.OrderBean;
import org.apache.hadoop.io.WritableComparator;

public class SogouGroup extends WritableComparator {

    public SogouGroup(){
        super(SogouBean.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        SogouBean o1 = (SogouBean) a;
        SogouBean o2 = (SogouBean) b;

        return o1.getUid().compareTo(o2.getUid());
    }
}
