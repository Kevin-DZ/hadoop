package com.day5.mysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public  class DBReader implements Writable,DBWritable{
	
	private int number;
	private String word ;
	
	public DBReader(){
		
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setString(1, word);
		statement.setInt(2,number);

	}

	public void readFields(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		this.word = resultSet.getString(1);
		this.number = resultSet.getInt(2);

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(word);
		out.writeInt(number);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.word = in.readUTF();
		this.number = in.readInt();
	}

	@Override
	public String toString() {
		return  word + "\t" + number;
	}

}

