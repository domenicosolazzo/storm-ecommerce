package storm.analytics.utilities;

import java.io.Serializable;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class NavigationEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    String userId;
    String pageType;
    @SuppressWarnings("rawtypes")
    Map otherData;

    @SuppressWarnings("rawtypes")
    public NavigationEntry(String userId, String pageType, Map otherData) {
        this.userId = userId;
        this.pageType = pageType;
        this.otherData= otherData;
    }

    public String getUserId(){
        return this.userId;
    }

    public void setUserId(String userId){
        this.userId = userId;
    }

    public String getPageType(){
        return this.pageType;
    }

    public void setPageType(String pageType){
        this.pageType = pageType;
    }

    @SuppressWarnings("rawtypes")
    public Map getOtherData(){
        return this.otherData;
    }

    @SuppressWarnings("rawtypes")
    public void setOtherData(Map otherData){
        this.otherData = otherData;
    }

    public String toString(){
        String ret = "User:" + userId + " navigating a " + pageType;
        if(otherData!=null){
            ret += " page with "+otherData.toString();
        }
        return ret;

    }
}
