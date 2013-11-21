package redis.clients.jedis.tests;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class EmbeddedRedis {
    private final Process redisProcess;

    private final String instanceName;

    public EmbeddedRedis(String instanceName, String ...command) throws IOException {
        this.instanceName = instanceName;
        redisProcess = new ProcessBuilder(command).start();
        logIO(redisProcess.getInputStream(), false);
        logIO(redisProcess.getErrorStream(), true);
    }

    private void logIO(final InputStream src, final boolean warn) {
        new Thread(new Runnable() {
            public void run() {
                Scanner sc = new Scanner(src);
                while (sc.hasNextLine()) {
                    if (warn) System.err.println(instanceName + " " + sc.nextLine());
                    else System.out.println(instanceName + " " + sc.nextLine());
                }
            }
        }).start();
    }

    public void die() throws InterruptedException {
        redisProcess.destroy();
        redisProcess.waitFor();
    }
}
