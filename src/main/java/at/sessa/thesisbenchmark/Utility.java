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

// Read the output from the command
            System.out.println("Here is the standard output of the command:\n");
            String s = null;
            while ((s = stdInput.readLine()) != null) {
                logger.info(s);
            }

// Read any errors from the attempted command
            System.out.println("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                logger.info(s);
            }
        } catch (Exception e) {
            logger.error("Error while executing command", e);
        }
    }
}
