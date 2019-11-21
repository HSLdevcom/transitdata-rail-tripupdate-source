package fi.hsl.transitdata.gtfsrtsource;

import com.google.transit.realtime.GtfsRealtime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

class TestUtils {
    static GtfsRealtime.FeedMessage readExample() {
        URL railAlertStream = RailTripUpdateServiceTest.class.getClassLoader().getResource("rail-alert-example.json");
        try (InputStream inputStream = railAlertStream.openStream()) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            byte[] readWindow = new byte[256];
            int numberOfBytesRead;

            while ((numberOfBytesRead = inputStream.read(readWindow)) > 0) {
                byteArrayOutputStream.write(readWindow, 0, numberOfBytesRead);
            }
            return GtfsRealtime.FeedMessage.parseFrom(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}