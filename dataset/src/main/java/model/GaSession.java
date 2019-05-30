package model;

import java.text.SimpleDateFormat;
import java.util.Date;



public class GaSession {
	
	private String fullVisitorId;
    private String visitId;
    private String visitNumber;
    private String visitStartTime;
    private String date;
    private String channelGrouping;
    private Totals totals;
    private TrafficSource trafficSource;
    private Device device;
    private GeoNetwork geoNetwork;
    
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
	public String getVisitNumber() {
		return visitNumber;
	}
	public void setVisitNumber(String visitNumber) {
		this.visitNumber = visitNumber;
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
	public String getChannelGrouping() {
		return channelGrouping;
	}
	public void setChannelGrouping(String channelGrouping) {
		this.channelGrouping = channelGrouping;
	}
	public Totals getTotals() {
		return totals;
	}
	public void setTotals(Totals totals) {
		this.totals = totals;
	}
	public TrafficSource getTrafficSource() {
		return trafficSource;
	}
	public void setTrafficSource(TrafficSource trafficSource) {
		this.trafficSource = trafficSource;
	}
	public Device getDevice() {
		return device;
	}
	public void setDevice(Device device) {
		this.device = device;
	}
	public GeoNetwork getGeoNetwork() {
		return geoNetwork;
	}
	public void setGeoNetwork(GeoNetwork geoNetwork) {
		this.geoNetwork = geoNetwork;
	}

	//
	public class Totals {
		public String bounces;
		public String hits;
		public String newVisits;
		public String pageviews;
		public String screenviews;
		public String timeOnScreen;
		public String timeOnSite;
		public String UniqueScreenViews;
		public String visits;
		public String getBounces() {
			return (bounces==null)? "0": bounces;
		}
		public void setBounces(String bounces) {
			this.bounces = bounces;
		}
		public String getHits() {
			return (hits==null)? "0": hits;
		}
		public void setHits(String hits) {
			this.hits = hits;
		}
		public String getNewVisits() {
			return (newVisits==null)? "0": newVisits;
		}
		public void setNewVisits(String newVisits) {
			this.newVisits = newVisits;
		}
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
		public String getTimeOnScreen() {
			return (timeOnScreen==null)? "0": timeOnScreen;
		}
		public void setTimeOnScreen(String timeOnScreen) {
			this.timeOnScreen = timeOnScreen;
		}
		public String getTimeOnSite() {
			return (timeOnSite==null)? "0": timeOnSite;
		}
		public void setTimeOnSite(String timeOnSite) {
			this.timeOnSite = timeOnSite;
		}
		public String getUniqueScreenViews() {
			return (UniqueScreenViews==null)? "0": UniqueScreenViews;
		}
		public void setUniqueScreenViews(String uniqueScreenViews) {
			UniqueScreenViews = uniqueScreenViews;
		}
		public String getVisits() {
			return (visits==null)? "0": visits;
		}
		public void setVisits(String visits) {
			this.visits = visits;
		}
		
    }
	
	public class TrafficSource {
		public String adContent;
		public String keyword;
		public String medium;
		public String referralPath;
		public String source;
		public String getAdContent() {
			return (adContent==null)? "": adContent;
		}
		public void setAdContent(String adContent) {
			this.adContent = adContent;
		}
		public String getKeyword() {
			return (keyword==null)? "": keyword;
		}
		public void setKeyword(String keyword) {
			this.keyword = keyword;
		}
		public String getMedium() {
			return (medium==null)? "": medium;
		}
		public void setMedium(String medium) {
			this.medium = medium;
		}
		public String getReferralPath() {
			return (referralPath==null)? "": referralPath;
		}
		public void setReferralPath(String referralPath) {
			this.referralPath = referralPath;
		}
		public String getSource() {
			return (source==null)? "": source;
		}
		public void setSource(String source) {
			this.source = source;
		}
	}
	
	public class Device {
		public String browser;
		public String browserVersion;
		public String deviceCategory;
		public String operatingSystem;
		public String operatingSystemVersion;
		public String mobileDeviceBranding;
		public String flashVersion;
		public String javaEnabled;
		public String language;
		public String screenColors;
		public String screenResolution;
		public String getBrowser() {
			return (browser==null)? "": browser;
		}
		public void setBrowser(String browser) {
			this.browser = browser;
		}
		public String getBrowserVersion() {
			return (browserVersion==null)? "": browserVersion;
		}
		public void setBrowserVersion(String browserVersion) {
			this.browserVersion = browserVersion;
		}
		public String getDeviceCategory() {
			return (deviceCategory==null)? "": deviceCategory;
		}
		public void setDeviceCategory(String deviceCategory) {
			this.deviceCategory = deviceCategory;
		}
		public String getOperatingSystem() {
			return (operatingSystem==null)? "": operatingSystem;
		}
		public void setOperatingSystem(String operatingSystem) {
			this.operatingSystem = operatingSystem;
		}
		public String getOperatingSystemVersion() {
			return (operatingSystemVersion==null)? "": operatingSystemVersion;
		}
		public void setOperatingSystemVersion(String operatingSystemVersion) {
			this.operatingSystemVersion = operatingSystemVersion;
		}
		public String getMobileDeviceBranding() {
			return (mobileDeviceBranding==null)? "": mobileDeviceBranding;
		}
		public void setMobileDeviceBranding(String mobileDeviceBranding) {
			this.mobileDeviceBranding = mobileDeviceBranding;
		}
		public String getFlashVersion() {
			return (flashVersion==null)? "": flashVersion;
		}
		public void setFlashVersion(String flashVersion) {
			this.flashVersion = flashVersion;
		}
		public String getJavaEnabled() {
			return (javaEnabled==null)? "": javaEnabled;
		}
		public void setJavaEnabled(String javaEnabled) {
			this.javaEnabled = javaEnabled;
		}
		public String getLanguage() {
			return (language==null)? "": language;
		}
		public void setLanguage(String language) {
			this.language = language;
		}
		public String getScreenColors() {
			return (screenColors==null)? "": screenColors;
		}
		public void setScreenColors(String screenColors) {
			this.screenColors = screenColors;
		}
		public String getScreenResolution() {
			return (screenResolution==null)? "": screenResolution;
		}
		public void setScreenResolution(String screenResolution) {
			this.screenResolution = screenResolution;
		}
	}
	
	public class GeoNetwork {
		private String continent;
		private String subContinent;
		private String country;
		private String region;
		private String metro;
		private String networkLocation;
		public String getContinent() {
			return continent;
		}
		public void setContinent(String continent) {
			this.continent = continent;
		}
		public String getSubContinent() {
			return subContinent;
		}
		public void setSubContinent(String subContinent) {
			this.subContinent = subContinent;
		}
		public String getCountry() {
			return country;
		}
		public void setCountry(String country) {
			this.country = country;
		}
		public String getRegion() {
			return region;
		}
		public void setRegion(String region) {
			this.region = region;
		}
		public String getMetro() {
			return metro;
		}
		public void setMetro(String metro) {
			this.metro = metro;
		}
		public String getNetworkLocation() {
			return networkLocation;
		}
		public void setNetworkLocation(String networkLocation) {
			this.networkLocation = networkLocation;
		}

	}
	
	@Override
	public String toString() {
		String time = null;
		String result = null;
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
			
			result =id + "\001" +
					fullVisitorId + "\001" +
					visitId + "\001" +
					visitNumber + "\001" +
					time + "\001" +
					date + "\001" +
					channelGrouping + "\001" +
					totals.getBounces() + "\001" +
					totals.getHits() + "\001" +
					totals.getNewVisits() + "\001" +
					totals.getPageviews() + "\001" +
					totals.getScreenviews() + "\001" +
					totals.getTimeOnScreen() + "\001" +
					totals.getTimeOnSite() + "\001" +
					totals.getUniqueScreenViews() + "\001" +
					totals.getVisits() + "\001" +
					trafficSource.getAdContent() + "\001" +
					trafficSource.getKeyword() + "\001" +
					trafficSource.getMedium() + "\001" +
					trafficSource.getReferralPath() + "\001" +
					trafficSource.getSource() + "\001" +
					device.getBrowser() + "\001" +
					device.getBrowserVersion() + "\001" +
					device.getDeviceCategory() + "\001" +
					device.getOperatingSystem() + "\001" +
					device.getOperatingSystemVersion() + "\001" +
					device.getMobileDeviceBranding() + "\001" +
					device.getFlashVersion() + "\001" +
					device.getJavaEnabled() + "\001" +
					device.getLanguage() + "\001" +
					device.getScreenColors() + "\001" +
					device.getScreenResolution() + "\001" +
					geoNetwork.getContinent() + "\001" +
					geoNetwork.getSubContinent() + "\001" +
					geoNetwork.getCountry() + "\001" +
					geoNetwork.getRegion() + "\001" +
					geoNetwork.getMetro() + "\001" +
					geoNetwork.getNetworkLocation();

		}
		catch(Exception e) {
			//log.error("GaSession error msg is {}", e.getMessage(), e);
		}
		
		return result;
	}
    
}
