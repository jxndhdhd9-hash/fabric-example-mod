package com.example.wssync;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientLifecycleEvents;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.minecraft.client.MinecraftClient;
import net.minecraft.client.network.ClientPlayerEntity;
import net.minecraft.util.math.Vec3d;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Συνδέεται στο ws://localhost:8080, διαβάζει JSON με X/Y/Z και μετακινεί
 * τον τοπικό παίκτη σε αυτές τις συντεταγμένες σε κάθε client tick.
 *
 * Δεκτά formats:
 *   {"x":1.0,"y":64.0,"z":-3.5}
 *   {"position":{"x":1.0,"y":64.0,"z":-3.5}, ...}   (το format του Unity plugin)
 */
public class WsSyncClient implements ClientModInitializer {

    public static final Logger LOGGER = LoggerFactory.getLogger("wssync");

    // ---- Ρυθμίσεις ----
    private static final URI SERVER_URI = URI.create("ws://localhost:8080");
    private static final long RECONNECT_DELAY_MS = 3000;
    /** Πολλαπλασιαστής συντεταγμένων (π.χ. 1 Unity unit = 1 block). */
    private static final double SCALE = 1.0;
    /**
     * false = ο συγχρονισμός δουλεύει μόνο σε singleplayer.
     * Σε πραγματικούς servers η μετακίνηση αυτού του τύπου μοιάζει με teleport hack
     * και μπορεί να οδηγήσει σε rubber-banding ή ban.
     */
    private static final boolean ALLOW_MULTIPLAYER = false;

    private record Pos(double x, double y, double z) {}

    private static volatile Pos latest;
    private static volatile boolean running = true;
    private static volatile WebSocket currentSocket;

    @Override
    public void onInitializeClient() {
        Thread t = new Thread(WsSyncClient::connectionLoop, "wssync-connection");
        t.setDaemon(true);
        t.start();

        // START: ώστε τα movement packets του tick να μεταφέρουν τη νέα θέση.
        // END:   ώστε η φυσική του παίκτη να μη μετατοπίσει την τελική θέση.
        ClientTickEvents.START_CLIENT_TICK.register(WsSyncClient::applyPosition);
        ClientTickEvents.END_CLIENT_TICK.register(WsSyncClient::applyPosition);

        ClientLifecycleEvents.CLIENT_STOPPING.register(client -> {
            running = false;
            WebSocket ws = currentSocket;
            if (ws != null) {
                ws.abort();
            }
        });
    }

    // ------------------------------------------------------------------
    // Εφαρμογή θέσης στον παίκτη (client thread)
    // ------------------------------------------------------------------

    private static void applyPosition(MinecraftClient client) {
        Pos p = latest;
        if (p == null) return;

        ClientPlayerEntity player = client.player;
        if (player == null || client.world == null) return;
        if (!ALLOW_MULTIPLAYER && !client.isInSingleplayer()) return;

        player.setPosition(p.x(), p.y(), p.z());
        player.setVelocity(Vec3d.ZERO);
        player.fallDistance = 0.0f;
    }

    // ------------------------------------------------------------------
    // WebSocket (background thread) με αυτόματο reconnect
    // ------------------------------------------------------------------

    private static void connectionLoop() {
        HttpClient http = HttpClient.newHttpClient();
        boolean failureReported = false;

        while (running) {
            Listener listener = new Listener();
            try {
                WebSocket ws = http.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .buildAsync(SERVER_URI, listener)
                        .join();
                currentSocket = ws;
                failureReported = false;
                listener.closed.join(); // μπλοκάρει μέχρι να κλείσει η σύνδεση
            } catch (Exception e) {
                if (!failureReported) {
                    LOGGER.warn("Δεν ήταν δυνατή η σύνδεση στο {} - νέες προσπάθειες κάθε {} ms",
                            SERVER_URI, RECONNECT_DELAY_MS);
                    failureReported = true;
                }
            }

            currentSocket = null;
            if (!running) break;

            try {
                Thread.sleep(RECONNECT_DELAY_MS);
            } catch (InterruptedException ie) {
                return;
            }
        }
    }

    private static final class Listener implements WebSocket.Listener {
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            LOGGER.info("Συνδέθηκε στο {}", SERVER_URI);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            // Τα μεγάλα μηνύματα μπορεί να έρθουν σε κομμάτια
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                buffer.setLength(0);
                handleMessage(message);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            LOGGER.info("Η σύνδεση έκλεισε ({})", statusCode);
            closed.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            LOGGER.warn("WebSocket error: {}", error.getMessage());
            closed.complete(null);
        }
    }

    private static void handleMessage(String json) {
        try {
            JsonObject root = JsonParser.parseString(json).getAsJsonObject();

            JsonObject src = root;
            JsonElement nested = root.get("position");
            if (nested != null && nested.isJsonObject()) {
                src = nested.getAsJsonObject();
            }

            if (!src.has("x") || !src.has("y") || !src.has("z")) return;

            double x = src.get("x").getAsDouble() * SCALE;
            double y = src.get("y").getAsDouble() * SCALE;
            double z = src.get("z").getAsDouble() * SCALE;

            if (!Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(z)) return;

            latest = new Pos(x, y, z);
        } catch (RuntimeException e) {
            // μη έγκυρο JSON ή απροσδόκητη δομή - αγνοείται
        }
    }
}
