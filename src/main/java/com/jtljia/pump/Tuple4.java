package com.jtljia.pump;

public class Tuple4<F,T,H, O> {
	private F _1;
	private T _2;
	private H _3;
	private O _4;
	
	public Tuple4(F e1, T e2, H e3, O e4){
		this._1 = e1;
		this._2 = e2;
		this._3 = e3;
		this._4 = e4;
	}
	
	public F e1() {
		return _1;
	}

	public T e2() {
		return _2;
	}

	public H e3(){
		return _3;
	};
	
	public O e4(){
		return _4;
	};
}
