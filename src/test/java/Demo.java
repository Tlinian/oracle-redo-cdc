import com.xdream.redo.RedoClient;
import com.xdream.redo.config.Config;
import com.xdream.redo.deserialize.RedoEvent;

import java.io.IOException;
import java.nio.file.Path;

public class Demo {
    public static void main(String[] args) throws IOException, InterruptedException {
        RedoClient redoClient = new RedoClient(new Config(Path.of("D:\\AI-project\\code2\\oracle-redo-analysis\\src\\main\\resources\\application.properties")));
        redoClient.init();
        redoClient.start();
        while(true){
            RedoEvent redoEvent = redoClient.redoEvent();
            if (redoEvent != null){
                System.out.println(redoEvent);
            }else  {
                Thread.sleep(3000);
                System.out.println("no redo event");
            }
        }
    }
}
