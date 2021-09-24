import java.io.*; // wildcard import for brevity in tutorial
import java.net.*;
import java.util.Random;
import java.util.concurrent.*;

public class TemperatureServer {
    private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final int PORT = 9999;
    private static final String DELIMITER = ":";
    private static final long EVENT_PERIOD_SECONDS = 1;
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException, InterruptedException {
        BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
        SERVER_EXECUTOR.execute(new SteamingServer(eventQueue));
        while (true) {
            eventQueue.put(generateEvent());
            Thread.sleep(TimeUnit.SECONDS.toMillis(EVENT_PERIOD_SECONDS));
        }
    }

    private static String generateEvent() {
        int userNumber = random.nextInt(40);
        // In production use a real schema like JSON or protocol buffers
        return String.format("météo : %s", userNumber);
    }

    private static class SteamingServer implements Runnable {
        private final BlockingQueue<String> eventQueue;

        public SteamingServer(BlockingQueue<String> eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void run() {
            try {
                ServerSocket serverSocket = new ServerSocket(PORT);
                System.out.println("Attente de connection ... ");

                Socket clientSocket = serverSocket.accept();
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                System.out.println("Une connextion vient de s'etablir " + clientSocket.toString());

                while (true) {
                    String event = eventQueue.take();
                    System.out.println(String.format("Envoi de la température \"%s\" sur la socket.", event));
                    out.println(event);
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Erreur sur le server", e);
            }
        }
    }
}