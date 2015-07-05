package tests.engage.engage_api.rubick.common.util;


//import org.codehaus.jettison.json.JSONException;
//import org.codehaus.jettison.json.JSONObject;
import org.json.JSONException;
import org.json.JSONObject;
import tests.engage.engage_api.rubick.DAO.DeviceServiceDao;
import tests.engage.engage_api.rubick.common.exceptions.DeviceServiceException;

/**
 * Created by keshav.gupta on 01/07/15.
 */
public class DeviceService {

    DeviceServiceDao deviceServiceDao;

    public DeviceService(String deviceServiceUrl) {
        deviceServiceDao=new DeviceServiceDao(deviceServiceUrl);
    }

    public void putUserIdDeviceIdEntryInDeviceService(JSONObject jsonObj) throws DeviceServiceException, JSONException {
        deviceServiceDao.putUserIdDeviceIdEntryInDeviceService(jsonObj);
    }

    public void deleteDeviceIdInfoInDeviceService(String deviceId) {
        deviceServiceDao.deleteDeviceIdInfoInDeviceService(deviceId);
    }
}
