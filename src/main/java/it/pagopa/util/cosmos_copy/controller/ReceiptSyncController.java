package it.pagopa.util.cosmos_copy.controller;

import it.pagopa.util.cosmos_copy.model.SyncStatus;
import it.pagopa.util.cosmos_copy.service.ReceiptSyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/sync")
public class ReceiptSyncController {

    @Autowired
    private ReceiptSyncService receiptSyncService;

    // POST /api/sync/start
    // POST /api/sync/start?limit=500000
    @PostMapping("/start")
    public Mono<ResponseEntity<String>> start(@RequestParam(required = false) Long limit) {
        if (receiptSyncService.getStatus().running()) {
            return Mono.just(ResponseEntity.badRequest().body("Il processo è già in esecuzione."));
        }
        return receiptSyncService.startSync(limit)
                .thenReturn(ResponseEntity.ok("Sincronizzazione avviata correttamente."));
    }

    // POST /api/sync/stop
    @PostMapping("/stop")
    public Mono<ResponseEntity<String>> stop() {
        if (!receiptSyncService.getStatus().running()) {
            return Mono.just(ResponseEntity.badRequest().body("Il processo non è in esecuzione."));
        }
        return receiptSyncService.stopSync()
                .thenReturn(ResponseEntity.ok("Arresto della sincronizzazione in corso..."));
    }

    // GET /api/sync/status
    @GetMapping("/status")
    public ResponseEntity<SyncStatus> getStatus() {
        return ResponseEntity.ok(receiptSyncService.getStatus());
    }
}