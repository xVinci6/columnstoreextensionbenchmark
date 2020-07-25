package at.sessa.thesisbenchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Utility {
    public static void execRuntime(String command) {
        Logger logger = LoggerFactory.getLogger(Utility.class);
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(command);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));

            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(proc.getErrorStream()));

            String s = null;
            while ((s = stdInput.readLine()) != null) {
                logger.info(s);
            }

            while ((s = stdError.readLine()) != null) {
                logger.info(s);
            }
        } catch (Exception e) {
            logger.error("Error while executing command", e);
        }
    }
}
