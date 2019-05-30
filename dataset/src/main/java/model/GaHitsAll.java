package model;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class GaHitsAll {
	
	private String fullVisitorId;
    private String visitId;
    private String visitStartTime;
    private String date;
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
	public String getVisitStartTime() {
		return visitStartTime;
	}
	public void setVisitStartTime(String visitStartTime) {
		this.visitStartTime = visitStartTime;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
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
		public String type;
		public String hour;
		public String minute;
		public String time;
		public String referer;
		public String isEntrance;
		public String isExit;
		public String isInteraction;
		public String customVariables;
		public String customDimensions;
		public String customMetrics;
		public ECommerceAction eCommerceAction;
		public ArrayList<Product> product;
		public Page page;
		public AppInfo appInfo;
		public ExceptionInfo exceptionInfo;
		public EventInfo eventInfo;
		
		
		public String getHitNumber() {
			return (hitNumber==null)? "0": hitNumber;
		}
		public void setHitNumber(String hitNumber) {
			this.hitNumber = hitNumber;
		}
		public String getType() {
			return (type==null)? "": type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public String getHour() {
			return hour;
		}
		public void setHour(String hour) {
			this.hour = hour;
		}
		public String getMinute() {
			return minute;
		}
		public void setMinute(String minute) {
			this.minute = minute;
		}
		public String getTime() {
			return time;
		}
		public void setTime(String time) {
			this.time = time;
		}
		public String getReferer() {
			return (referer==null)? "": referer;
		}
		public void setReferer(String referer) {
			this.referer = referer;
		}
		public String getIsEntrance() {
			return (isEntrance==null)? "": isEntrance;
		}
		public void setIsEntrance(String isEntrance) {
			this.isEntrance = isEntrance;
		}
		public String getIsExit() {
			return (isExit==null)? "": isExit;
		}
		public void setIsExit(String isExit) {
			this.isExit = isExit;
		}
		public String getIsInteraction() {
			return (isInteraction==null)? "": isInteraction;
		}
		public void setIsInteraction(String isInteraction) {
			this.isInteraction = isInteraction;
		}
		public String getCustomVariables() {
			return customVariables;
		}
		public void setCustomVariables(String customVariables) {
			this.customVariables = customVariables;
		}
		public String getCustomDimensions() {
			return customDimensions;
		}
		public void setCustomDimensions(String customDimensions) {
			this.customDimensions = customDimensions;
		}
		public String getCustomMetrics() {
			return customMetrics;
		}
		public void setCustomMetrics(String customMetrics) {
			this.customMetrics = customMetrics;
		}
		public ECommerceAction geteCommerceAction() {
			return eCommerceAction;
		}
		public void seteCommerceAction(ECommerceAction eCommerceAction) {
			this.eCommerceAction = eCommerceAction;
		}
		public ArrayList<Product> getProduct() {
			return product;
		}
		public void setProduct(ArrayList<Product> product) {
			this.product = product;
		}
		public Page getPage() {
			return page;
		}
		public void setPage(Page page) {
			this.page = page;
		}
		public AppInfo getAppInfo() {
			return appInfo;
		}
		public void setAppInfo(AppInfo appInfo) {
			this.appInfo = appInfo;
		}
		public ExceptionInfo getExceptionInfo() {
			return exceptionInfo;
		}
		public void setExceptionInfo(ExceptionInfo exceptionInfo) {
			this.exceptionInfo = exceptionInfo;
		}
		public EventInfo getEventInfo() {
			return eventInfo;
		}
		public void setEventInfo(EventInfo eventInfo) {
			this.eventInfo = eventInfo;
		}


		public class ECommerceAction {
			public String action_type;
			public String step;
			public String getAction_type() {
				return (action_type==null)? "": action_type;
			}
			public void setAction_type(String action_type) {
				this.action_type = action_type;
			}
			public String getStep() {
				return (step==null)? "": step;
			}
			public void setStep(String step) {
				this.step = step;
			}
			
		}
		
		public class Product{
			public String productSKU;
			public String v2ProductCategory;
			public String v2ProductName;
			public String customDimensions;
			public String getProductSKU() {
				return (productSKU==null)? "": productSKU;
			}
			public void setProductSKU(String productSKU) {
				this.productSKU = productSKU;
			}
			public String getV2ProductCategory() {
				return (v2ProductCategory==null)? "": v2ProductCategory;
			}
			public void setV2ProductCategory(String v2ProductCategory) {
				this.v2ProductCategory = v2ProductCategory;
			}
			public String getV2ProductName() {
				return (v2ProductName==null)? "": v2ProductName;
			}
			public void setV2ProductName(String v2ProductName) {
				this.v2ProductName = v2ProductName;
			}
			public String getCustomDimensions() {
				return (customDimensions==null)? "": customDimensions;
			}
			public void setCustomDimensions(String customDimensions) {
				this.customDimensions = customDimensions;
			}
		}
		
		public class Page{
			public String pagePath;
			public String hostname;
			public String pageTitle;
			public String searchKeyword;
			public String searchCategory;
			public String getPagePath() {
				return (pagePath==null)? "": pagePath;
			}
			public void setPagePath(String pagePath) {
				this.pagePath = pagePath;
			}
			public String getHostname() {
				return (hostname==null)? "": hostname;
			}
			public void setHostname(String hostname) {
				this.hostname = hostname;
			}
			public String getPageTitle() {
				return (pageTitle==null)? "": pageTitle;
			}
			public void setPageTitle(String pageTitle) {
				this.pageTitle = pageTitle;
			}
			public String getSearchKeyword() {
				return (searchKeyword==null)? "": searchKeyword;
			}
			public void setSearchKeyword(String searchKeyword) {
				this.searchKeyword = searchKeyword;
			}
			public String getSearchCategory() {
				return (searchCategory==null)? "": searchCategory;
			}
			public void setSearchCategory(String searchCategory) {
				this.searchCategory = searchCategory;
			}
			
		}
		
		public class AppInfo{
			public String name;
			public String version;
			public String id;
			public String installerId;
			public String appInstallerId;
			public String appName;
			public String appVersion;
			public String appId;
			public String screenName;
			public String landingScreenName;
			public String exitScreenName;
			public String screenDepth;
			public String getName() {
				return (name==null)? "": name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public String getVersion() {
				return (version==null)? "": version;
			}
			public void setVersion(String version) {
				this.version = version;
			}
			public String getId() {
				return (id==null)? "": id;
			}
			public void setId(String id) {
				this.id = id;
			}
			public String getInstallerId() {
				return (installerId==null)? "": installerId;
			}
			public void setInstallerId(String installerId) {
				this.installerId = installerId;
			}
			public String getAppInstallerId() {
				return (appInstallerId==null)? "": appInstallerId;
			}
			public void setAppInstallerId(String appInstallerId) {
				this.appInstallerId = appInstallerId;
			}
			public String getAppName() {
				return (appName==null)? "": appName;
			}
			public void setAppName(String appName) {
				this.appName = appName;
			}
			public String getAppVersion() {
				return (appVersion==null)? "": appVersion;
			}
			public void setAppVersion(String appVersion) {
				this.appVersion = appVersion;
			}
			public String getAppId() {
				return (appId==null)? "": appId;
			}
			public void setAppId(String appId) {
				this.appId = appId;
			}
			public String getScreenName() {
				return (screenName==null)? "": screenName;
			}
			public void setScreenName(String screenName) {
				this.screenName = screenName;
			}
			public String getLandingScreenName() {
				return (landingScreenName==null)? "": landingScreenName;
			}
			public void setLandingScreenName(String landingScreenName) {
				this.landingScreenName = landingScreenName;
			}
			public String getExitScreenName() {
				return (exitScreenName==null)? "": exitScreenName;
			}
			public void setExitScreenName(String exitScreenName) {
				this.exitScreenName = exitScreenName;
			}
			public String getScreenDepth() {
				return (screenDepth==null)? "": screenDepth;
			}
			public void setScreenDepth(String screenDepth) {
				this.screenDepth = screenDepth;
			}
		}
		
		public class ExceptionInfo{
			public String description;
			public String isFatal;
			public String getDescription() {
				return (description==null)? "": description;
			}
			public void setDescription(String description) {
				this.description = description;
			}
			public String getIsFatal() {
				return (isFatal==null)? "": isFatal;
			}
			public void setIsFatal(String isFatal) {
				this.isFatal = isFatal;
			}
		}
		
		public class EventInfo{
			public String eventCategory;
			public String eventAction;
			public String eventLabel;
			public String getEventCategory() {
				return (eventCategory==null)? "": eventCategory;
			}
			public void setEventCategory(String eventCategory) {
				this.eventCategory = eventCategory;
			}
			public String getEventAction() {
				return (eventAction==null)? "": eventAction;
			}
			public void setEventAction(String eventAction) {
				this.eventAction = eventAction;
			}
			public String getEventLabel() {
				return (eventLabel==null)? "": eventLabel;
			}
			public void setEventLabel(String eventLabel) {
				this.eventLabel = eventLabel;
			}
		}
		
    }
	
	
	@Override
	public String toString() {
		String time = null;
		String result = "";
		String id = null;
		try{
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if(visitStartTime!=null) {
				time = simpleDateFormat.format(new Date(new Long(visitStartTime+"000")));
			}
			
			if(Integer.parseInt(totals.getPageviews())>0) {
				id = "pc_" + fullVisitorId + "_" + visitId;
			}else if(Integer.parseInt(totals.getScreenviews())>0){
				id = "app_" + fullVisitorId + "_" + visitId;
			}else {
				id = "other_" + fullVisitorId + "_" + visitId;
			}
			
			//
			for(int i=0;i<hits.size();i++){
				result += id + "\001" +
						fullVisitorId + "\001" +
						time + "\001" +
						date + "\001" ;
				
				if(hits.get(i)!=null) {
					result += hits.get(i).getHitNumber() + "\001" +
						hits.get(i).getType() + "\001" +
						hits.get(i).getHour() + "\001" +
						hits.get(i).getMinute() + "\001" +
						hits.get(i).getTime() + "\001" +
						hits.get(i).getReferer() + "\001" +
						hits.get(i).getIsEntrance() + "\001" +
						hits.get(i).getIsExit() + "\001" +
						hits.get(i).getIsInteraction() + "\001";
				}else {
					result += ""+ "\001" +""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001";
				}
				
				if(hits.get(i).geteCommerceAction()!=null) {
					result += hits.get(i).geteCommerceAction().getAction_type() + "\001" +
							hits.get(i).geteCommerceAction().getStep() + "\001";
				}else {
					result += ""+ "\001" +""+ "\001" ;
				}
						
				if(hits.get(i).getProduct()!=null && !hits.get(i).getProduct().isEmpty() ) {	
					result += hits.get(i).getProduct().get(0).getProductSKU() + "\001" +
						hits.get(i).getProduct().get(0).getV2ProductCategory() + "\001" +
						hits.get(i).getProduct().get(0).getV2ProductName() + "\001" +
						hits.get(i).getProduct().get(0).getCustomDimensions() + "\001";
				}else {
					result += ""+ "\001" +""+ "\001"+""+ "\001"+""+ "\001";
				}
				if(hits.get(i).getPage()!=null) {
					result += hits.get(i).getPage().getPagePath() + "\001" +
							hits.get(i).getPage().getHostname() + "\001" +
							hits.get(i).getPage().getPageTitle() + "\001" +
							hits.get(i).getPage().getSearchKeyword() + "\001" +
							hits.get(i).getPage().getSearchCategory() + "\001";
				}else {
					result += ""+ "\001" +""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001" ;
				}
	
						
				if(hits.get(i).getAppInfo()!=null) {
					result += hits.get(i).getAppInfo().getName() + "\001" +
							hits.get(i).getAppInfo().getVersion() + "\001" +
							hits.get(i).getAppInfo().getId() + "\001" +
							hits.get(i).getAppInfo().getInstallerId() + "\001" +
							hits.get(i).getAppInfo().getAppInstallerId() + "\001" +
							hits.get(i).getAppInfo().getAppName() + "\001" +
							hits.get(i).getAppInfo().getAppId() + "\001" +
							hits.get(i).getAppInfo().getScreenName() + "\001" +
							hits.get(i).getAppInfo().getLandingScreenName() + "\001" +
							hits.get(i).getAppInfo().getExitScreenName() + "\001" +
							hits.get(i).getAppInfo().getScreenDepth() + "\001";
				}else {
					result += ""+ "\001" +""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001"+""+ "\001" ;
				}
						
				
				if(hits.get(i).getExceptionInfo()!=null) {
					result += hits.get(i).getExceptionInfo().getDescription() + "\001" +
							hits.get(i).getExceptionInfo().getIsFatal() + "\001";
				}else {
					result += ""+ "\001" +""+ "\001" ;
				}
						
				if(hits.get(i).getEventInfo()!=null) {
					result += hits.get(i).getEventInfo().getEventCategory() + "\001" +
							hits.get(i).getEventInfo().getEventAction() + "\001" +
							hits.get(i).getEventInfo().getEventLabel() + "\001" ;
				}else {
					result += ""+ "\001" +""+ "\001" +""+ "\001" ;
				}
				result += hits.get(i).getCustomVariables() + "\001" +
						hits.get(i).getCustomDimensions() + "\001" +
						hits.get(i).getCustomMetrics();
				
				if(i!=hits.size()-1) {
					result += "\n";
				}
				
            }

		}
		catch(Exception e) {
			//log.error("GaSession error msg is {}", e.getMessage(), e);
		}
		
		return result;
	}
    
}
