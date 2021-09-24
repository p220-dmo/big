import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CashBoxServer {
    private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final int PORT = 9999;
    private static final String DELIMITER = ":";
    private static final long EVENT_PERIOD_MILLISECONDS = 1;
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException, InterruptedException {
        BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
        SERVER_EXECUTOR.execute(new SteamingServer(eventQueue));
        while (true) {
            eventQueue.put(generateEvent());
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(EVENT_PERIOD_MILLISECONDS));
        }
    }

    private static String generateEvent() {
        int userNumber = random.nextInt(40);

        Sale s = new Sale();
        s.setProductId(random.nextInt(1000));
        s.setTimeId(random.nextInt(12345));
        s.setCustomerId(random.nextInt(10000));
        s.setPromotionId(random.nextInt(10));
        s.setStoreCost((1 + random.nextInt(1000)) * random.nextFloat());
        s.setStoreId(random.nextInt(25));
        s.setStoreSales(s.getStoreCost() * (random.nextFloat() + 1));
        s.setUnitSales((1 + random.nextInt(10)) * 1.0f);
        return s.toString();
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
                System.out.println("Une connexion vient de s'etablir " + clientSocket.toString());

                while (true) {
                    String event = eventQueue.take();
                    System.out.println(String.format("Envoi d'une sur la socket.", event));
                    out.println(event);
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Erreur sur le server", e);
            }
        }

        
    }
}