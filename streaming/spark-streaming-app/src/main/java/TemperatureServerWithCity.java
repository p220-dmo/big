import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class TemperatureServerWithCity {
    private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final int PORT = 9910;
    private static final String DELIMITER = ":";
    private static final long EVENT_PERIOD_SECONDS = 1;
    private static final Random random = new Random();
    static List<String> cities = Arrays.asList("Nanterre", "Puteaux", "Paris", "Courbevoie", "Nogent", "Poissy", "Torcy");

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

        String city = cities.get(random.nextInt(cities.size() - 1));
        // In production use a real schema like JSON or protocol buffers
        return String.format("%s ; %s ; %s",new Date(), city, userNumber);
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