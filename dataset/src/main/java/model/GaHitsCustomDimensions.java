package model;

import java.util.ArrayList;

public class GaHitsCustomDimensions {
	
	public static class CustomDimensions{
		public String index;
		public String value;
		public String getIndex() {
			return index;
		}
		public void setIndex(String index) {
			this.index = index;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
	}
	
	private String fullVisitorId;
    private String visitId;
    private Totals totals;
    private ArrayList<Hits> hits;
    
	public String getFullVisitorId() {
		return fullVisitorId;
	}
	public void setFullVisitorId(String fullVisitorId) {
		this.fullVisitorId = fullVisitorId;
	}
	public String getVisitId() {
		return visitId;
	}
	public void setVisitId(String visitId) {
		this.visitId = visitId;
	}
	public Totals getTotals() {
		return totals;
	}
	public void setTotals(Totals totals) {
		this.totals = totals;
	}
	
	public ArrayList<Hits> getHits() {
		return hits;
	}
	public void setHits(ArrayList<Hits> hits) {
		this.hits = hits;
	}


	//
	public class Totals {
		public String pageviews;
		public String screenviews;
	
		public String getPageviews() {
			return (pageviews==null)? "0": pageviews;
		}
		public void setPageviews(String pageviews) {
			this.pageviews = pageviews;
		}
		public String getScreenviews() {
			return (screenviews==null)? "0": screenviews;
		}
		public void setScreenviews(String screenviews) {
			this.screenviews = screenviews;
		}
		
    }
	
	//
	public class Hits {
		public String hitNumber;
		public ArrayList<CustomDimensions> customDimensions;
		public ArrayList<Product> product;
		
		public String getHitNumber() {
			return (hitNumber==null)? "0": hitNumber;
		}
		public void setHitNumber(String hitNumber) {
			this.hitNumber = hitNumber;
		}
		public ArrayList<CustomDimensions> getCustomDimensions() {
			return customDimensions;
		}
		public void setCustomDimensions(ArrayList<CustomDimensions> customDimensions) {
			this.customDimensions = customDimensions;
		}
		public ArrayList<Product> getProduct() {
			return product;
		}
		public void setProduct(ArrayList<Product> product) {
			this.product = product;
		}


		public class Product{
			public ArrayList<CustomDimensions> customDimensions;

			public ArrayList<CustomDimensions> getCustomDimensions() {
				return customDimensions;
			}

			public void setCustomDimensions(ArrayList<CustomDimensions> customDimensions) {
				this.customDimensions = customDimensions;
			}
		}
		
		
    }
	
	
	@Override
	public String toString() {
		String result = "";
		String id = null;
		try{
			if(Integer.parseInt(totals.getPageviews())>0) {
				id = "pc_" + fullVisitorId + "_" + visitId;
			}else if(Integer.parseInt(totals.getScreenviews())>0){
				id = "app_" + fullVisitorId + "_" + visitId;
			}else {
				id = "other_" + fullVisitorId + "_" + visitId;
			}
			
			//
			for(int i=0;i<hits.size();i++){
				ArrayList<CustomDimensions> customDimensions = hits.get(i).getCustomDimensions();
				
				if(hits.get(i).getProduct()!=null && !hits.get(i).getProduct().isEmpty() ) {
					customDimensions.addAll(hits.get(i).getProduct().get(0).getCustomDimensions());
				}	
				
				for(int j=0;j<customDimensions.size();j++) {

					result += id + "\001" + fullVisitorId + "\001";
				
					if(hits.get(i)!=null) {
						result += hits.get(i).getHitNumber() + "\001";
					}else {
						result += ""+ "\001";
					}
				
			
						
				
				result +=customDimensions.get(j).getIndex() + "\001"+customDimensions.get(j).getValue();
				
				if(j!=customDimensions.size()-1 ) {
					result += "\n";
				}
				
				}
				
            }

		}
		catch(Exception e) {
			//log.error("GaSession error msg is {}", e.getMessage(), e);
		}
		
		return result;
	}
    
}
