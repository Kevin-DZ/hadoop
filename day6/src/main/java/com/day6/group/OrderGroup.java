package com.day6.group;

import com.day6.javabean.OrderBean;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroup extends WritableComparator {

    public OrderGroup(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;

        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
