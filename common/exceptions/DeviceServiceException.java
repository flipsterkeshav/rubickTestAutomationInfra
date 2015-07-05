package tests.engage.engage_api.rubick.common.exceptions;

/**
 * Created by nitin.kaushik on 12/06/15.
 */
public class DeviceServiceException extends Throwable {
    public DeviceServiceException() {
    }

    public DeviceServiceException( String message ) {
        super(message);
    }

    public DeviceServiceException( String message, Throwable cause ) {
        super(message, cause);
    }

    public DeviceServiceException( Throwable cause ) {
        super(cause);
    }

//    public DeviceServiceException( String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace ) {
//        super(message, cause, enableSuppression, writableStackTrace);
//    }
}
