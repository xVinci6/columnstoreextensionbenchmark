package at.sessa.thesisbenchmark.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "at.sessa.thesisbenchmark")
public class GenericProperties {
    private String scaleFactor;
    private String dockerTestdataMountPath;

    public String getDockerTestdataMountPath() {
        return dockerTestdataMountPath;
    }

    public void setDockerTestdataMountPath(String dockerTestdataMountPath) {
        this.dockerTestdataMountPath = dockerTestdataMountPath;
    }

    public String getScaleFactor() {
        return scaleFactor;
    }

    public void setScaleFactor(String scaleFactor) {
        this.scaleFactor = scaleFactor;
    }
}
