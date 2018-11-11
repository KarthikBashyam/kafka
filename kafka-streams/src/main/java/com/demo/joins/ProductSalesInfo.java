package com.demo.joins;

public class ProductSalesInfo {

	private Long id;

	private String name;

	private String customer;

	private Long count;

	public ProductSalesInfo(Long id, String name, String customer, Long count) {
		super();
		this.id = id;
		this.name = name;
		this.customer = customer;
		this.count = count;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCustomer() {
		return customer;
	}

	public void setCustomer(String customer) {
		this.customer = customer;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}
