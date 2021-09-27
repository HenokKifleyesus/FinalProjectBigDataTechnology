package cs523.FinalProject2;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class MercedesCars implements Serializable {


	private String model;
	  private String year;
	  private float price;
	  private String transmission;
	  private double milleage;
	  private String fuelType;
	  private double tax;
	  private float mpg;
	  private float engineSize;
	public String getModel() {
		return model;
	}
	public void setModel(String model) {
		this.model = model;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}
	public String getTransmission() {
		return transmission;
	}
	public void setTransmission(String transmission) {
		this.transmission = transmission;
	}
	public double getMilleage() {
		return milleage;
	}
	public void setMilleage(double milleage) {
		this.milleage = milleage;
	}
	public String getFuelType() {
		return fuelType;
	}
	public void setFuelType(String fuelType) {
		this.fuelType = fuelType;
	}
	public double getTax() {
		return tax;
	}
	public void setTax(double tax) {
		this.tax = tax;
	}
	public float getMpg() {
		return mpg;
	}
	public void setMpg(float mpg) {
		this.mpg = mpg;
	}
	public float getEngineSize() {
		return engineSize;
	}
	public void setEngineSize(float engineSize) {
		this.engineSize = engineSize;
	}
	  

	
	  
}
