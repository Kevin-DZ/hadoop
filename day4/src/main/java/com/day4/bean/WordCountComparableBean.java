package com.day4.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    自定义数据类型,加了比较
 */
public class WordCountComparableBean implements WritableComparable<WordCountComparableBean> {
    private String word;
    private int length;

    public WordCountComparableBean() {
    }

    public void setCountBean(String word, int length) {
        this.setWord(word);
        this.setLength(length);
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.word);
        dataOutput.writeInt(this.length);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.length = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.word + "\t" + this.length;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    //用于shuffle阶段的排序和分组
    @Override
    public int compareTo(WordCountComparableBean o) {
        int comp = getWord().compareTo(o.getWord());
        if (comp == 0) {
            return Integer.valueOf(getLength()).compareTo(Integer.valueOf(o.getLength()));
        }
        return comp;
    }

}
