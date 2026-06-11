package it.pagopa.util.cosmos_copy.service;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import it.pagopa.util.cosmos_copy.model.SyncStatus;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class ReceiptSyncService {

    @Autowired
    private CosmosAsyncClient cosmosAsyncClient;

    @Value("${azure.cosmos.database}")
    private String databaseName;

    @Value("${azure.cosmos.source-container}")
    private String sourceContainerName;

    @Value("${azure.cosmos.target-container}")
    private String targetContainerName;

    @Value("${azure.cosmos.lease-container}")
    private String leaseContainerName;

    @Value("${retry.max-attempts}")
    private int retryMaxAttempts;

    @Value("${retry.backoff-seconds}")
    private long retryBackoffSeconds;

    // Stato del processo
    private CosmosAsyncContainer targetContainer;
    private ChangeFeedProcessor changeFeedProcessor;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    // Contatori
    private final AtomicLong processedDocuments = new AtomicLong(0);
    private final AtomicLong targetLimit = new AtomicLong(0); // 0 = nessun limite

    @PostConstruct
    public void init() {
        CosmosAsyncDatabase database = cosmosAsyncClient.getDatabase(databaseName);
        CosmosAsyncContainer sourceContainer = database.getContainer(sourceContainerName);
        this.targetContainer = database.getContainer(targetContainerName);
        CosmosAsyncContainer leaseContainer = ensureLeaseContainer(database);

        ChangeFeedProcessorOptions options = new ChangeFeedProcessorOptions();
        options.setStartFromBeginning(true);
        options.setMaxItemCount(500);

        this.changeFeedProcessor = new ChangeFeedProcessorBuilder()
                .hostName(UUID.randomUUID().toString())
                .options(options)
                .feedContainer(sourceContainer)
                .leaseContainer(leaseContainer)
                .handleChanges(this::handleChangesBatch)
                .buildChangeFeedProcessor();
    }

    // ── API di controllo ───────────────────────────────────────────────────────

    public Mono<Void> startSync(Long limit) {
        if (isRunning.compareAndSet(false, true)) {
            // Se c'è un limite, impostiamo il targetLimit calcolando il valore di arrivo desiderato
            long limitValue = (limit != null && limit > 0) ? processedDocuments.get() + limit : 0;
            targetLimit.set(limitValue);

            log.info("[SYNC] Avvio richiesto. Limite target impostato a: {}", limitValue > 0 ? limitValue : "Nessuno (Infinito)");
            return changeFeedProcessor.start();
        }
        return Mono.empty();
    }

    public Mono<Void> stopSync() {
        if (isRunning.compareAndSet(true, false)) {
            log.info("[SYNC] Stop richiesto. Arresto del Change Feed in corso...");
            return changeFeedProcessor.stop();
        }
        return Mono.empty();
    }

    public SyncStatus getStatus() {
        return new SyncStatus(
                isRunning.get(),
                processedDocuments.get(),
                targetLimit.get() > 0 ? targetLimit.get() : null
        );
    }

    // ── Processing ─────────────────────────────────────────────────────────────

    private void handleChangesBatch(List<JsonNode> documents) {
        try {
            Flux.fromIterable(documents)
                    .filter(doc -> {
                        long limit = targetLimit.get();
                        if (limit > 0 && processedDocuments.get() >= limit) {
                            if (isRunning.get()) {
                                log.info("[SYNC] Limite di {} record raggiunto. Stop automatico.", limit);
                                stopSync().subscribeOn(Schedulers.boundedElastic()).subscribe();
                            }
                            return false;
                        }
                        return true;
                    })
                    // Concorrenza a 20 per non bombardare eccessivamente la destinazione
                    .flatMap(doc -> upsertWithRetry(doc, targetContainer), 20)
                    .doOnNext(doc -> {
                        long current = processedDocuments.incrementAndGet();
                        if (current % 1000 == 0) {
                            log.info("[SYNC] Progress: {} documenti elaborati.", current);
                        }
                    })
                    // .then().block() è FONDAMENTALE qui.
                    // Mette in pausa il thread del Change Feed finché i 500 upsert asincroni
                    // non sono finiti. Se va tutto bene, il codice procede e il lease avanza.
                    .then()
                    .block();

        } catch (Exception e) {
            // Se arriviamo qui, significa che tutti i retry (es. per errore 429) sono falliti.
            log.error("[SYNC] ERRORE CRITICO: Timeout o RU esaurite. Arresto automatico del processo in corso...");

            // 1. Diciamo all'applicativo di fermarsi spegnendo il Change Feed
            if (isRunning.get()) {
                stopSync().subscribeOn(Schedulers.boundedElastic()).subscribe();
            }

            // 2. RILANCIAMO L'ECCEZIONE!
            // Questo dice a Cosmos DB: "Il batch è fallito, NON spostare il segnalibro del lease".
            // Quando riavvierai il processo, ripartirà esattamente dai documenti che non è riuscito a scrivere.
            throw new RuntimeException("Interruzione per fallimento batch (Possibile 429 Too Many Requests)", e);
        }
    }
    // ── Upsert & Utility (Invariati) ───────────────────────────────────────────

    private Mono<JsonNode> upsertWithRetry(JsonNode document, CosmosAsyncContainer container) {
        return container.upsertItem(document)
                // Restituiamo il documento invece di .then() così il Flux principale può contarlo
                .thenReturn(document)
                .retryWhen(Retry.backoff(retryMaxAttempts, Duration.ofSeconds(retryBackoffSeconds))
                        // Max tempo da spendere nei retry prima di arrendersi e far crashare il batch
                        .maxBackoff(Duration.ofSeconds(60))
                        .doBeforeRetry(signal -> log.warn(
                                "[RETRY] 429 o Timeout per id={}. Tentativo {}. Motivo: {}",
                                document.path("id").asText("?"),
                                signal.totalRetries() + 1,
                                signal.failure().getMessage()
                        ))
                );
    }

    private CosmosAsyncContainer ensureLeaseContainer(CosmosAsyncDatabase database) {
        CosmosContainerProperties props = new CosmosContainerProperties(leaseContainerName, "/id");
        database.createContainerIfNotExists(props, ThroughputProperties.createManualThroughput(400)).block(Duration.ofSeconds(30));
        return database.getContainer(leaseContainerName);
    }
}