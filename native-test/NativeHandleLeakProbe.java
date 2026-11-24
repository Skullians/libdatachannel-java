package tel.schich.libdatachannel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * WINDOWS ONLY TEST
 */
public final class NativeHandleLeakProbe {
    private static final int WARM_UP_ROUNDS = 3;
    private static final int ROUNDS = 30;
    private static final int MESSAGES = 5;
    private static final int ALLOWED_GROWTH_PER_ROUND = 10;

    private static void round(ExecutorService callbacks) throws InterruptedException {
        PeerConnection offerer = PeerConnection.createPeer(PeerConnectionConfiguration.DEFAULT, callbacks);
        PeerConnection answerer = PeerConnection.createPeer(PeerConnectionConfiguration.DEFAULT, callbacks);
        CountDownLatch received = new CountDownLatch(MESSAGES);
        offerer.onLocalDescription.register((peer, sdp, type) -> answerer.setRemoteDescription(sdp, type));
        answerer.onLocalDescription.register((peer, sdp, type) -> offerer.setRemoteDescription(sdp, type));
        offerer.onLocalCandidate.register((peer, candidate, mid) -> answerer.addRemoteCandidate(candidate, mid));
        answerer.onLocalCandidate.register((peer, candidate, mid) -> offerer.addRemoteCandidate(candidate, mid));
        answerer.onDataChannel.register((peer, channel) -> channel.onMessage.register(
                DataChannelCallback.Message.handleText((dc, text) -> received.countDown())));
        DataChannel channel = offerer.createDataChannel("handle-leak");
        channel.onOpen.register(open -> {
            for (int i = 0; i < MESSAGES; i++) open.sendMessage("message " + i);
        });

        boolean exchanged = received.await(10, TimeUnit.SECONDS);
        boolean closed = offerer.closeAndAwait(Duration.ofSeconds(10)) & answerer.closeAndAwait(Duration.ofSeconds(10));
        if (!exchanged) throw new AssertionError("Peers did not exchange messages");
        if (!closed) throw new AssertionError("Native teardown timed out");
    }

    private static long handleCount() throws IOException, InterruptedException {
        String powershell = System.getenv("SystemRoot") + "\\System32\\WindowsPowerShell\\v1.0\\powershell.exe";
        Process process = new ProcessBuilder(powershell, "-NoProfile", "-Command",
                "(Get-Process -Id " + ProcessHandle.current().pid() + ").HandleCount")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (process.waitFor() != 0) throw new IOException("Could not read the handle count: " + output);
        return Long.parseLong(output);
    }

    public static void main(String[] args) throws Exception {
        Class.forName("tel.schich.libdatachannel.LibDataChannelArchDetect").getMethod("initialize").invoke(null);
        ExecutorService callbacks = Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < WARM_UP_ROUNDS; i++) round(callbacks);
            long before = handleCount();
            for (int i = 0; i < ROUNDS; i++) round(callbacks);
            long growth = handleCount() - before;
            if (growth > (long) ROUNDS * ALLOWED_GROWTH_PER_ROUND) {
                throw new AssertionError("Handle count grew by " + growth + " over " + ROUNDS + " connections");
            }
            System.out.println("handle-leak PASS rounds=" + ROUNDS + " growth=" + growth);
        } finally {
            callbacks.shutdownNow();
        }
    }
}
