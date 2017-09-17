package com.bigdatum.data;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class jsonData{
	public String getData(){
		List<String> date = Arrays.asList("2017-01-02-11:22:33","2017-01-03-11:22:33","2017-01-04-11:22:33",
				"2017-01-05-11:22:33","2017-01-06-11:22:33","2017-01-07-11:22:33");
		List<String> stock = Arrays.asList("stock01","stock02","stock03","stock04","stock05","stock06","stock07","stock08",
				"stock09","stock10","stock11","stock12");
		List<String> buyer = Arrays.asList("bAAA","bBBB","bCCC","bDDD","bEEE","bFFF","bGGG");
		List<String> seller = Arrays.asList("sAAA","sBBB","sCCC","sDDD","sEEE","sFFF","sGGG");
		
		Random rand = new Random();
		return (new json(date.get(rand.nextInt(5)),stock.get(rand.nextInt(11)),
				buyer.get(rand.nextInt(6)),seller.get(rand.nextInt(6)),rand.nextInt(1000),rand.nextInt(100000))).toString();

	}
}

class json{
	public long Transaction_id = 0 ;
	public String Stock = ""; 
	public String Seller = "";
	public String Buyer = "";
	public String Date = "";
	public double Price = 0;
	public double Volume = 0;
	
	public json(String date, String stock, String seller, String buyer, double price, double volume){
		Random random = new Random();
		this.Transaction_id = (long)(random.nextDouble()*10000000000L);
		this.Date = date;
		this.Stock = stock;
		this.Seller = seller;
		this.Buyer = buyer;
		this.Price = price;
		this.Volume = volume;
	}

	public String toString(){
		return "{'records':[{'value':{ 'Transaction-id' :" + this.Transaction_id + 
				", 'Time': '"+ this.Date + 
			    "', 'Stock':'" + this.Stock + "', 'Seller':'" + this.Seller + "' ,'Buyer':'" + this.Buyer + 
			    "', 'Price': " + this.Price + ",'Volume':" + this.Volume + "}}]}" ;
	}
}

