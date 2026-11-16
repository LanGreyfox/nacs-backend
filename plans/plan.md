# Plan: P2P-Reaktionsfähigkeit auf schwachen Geräten

## Ziel

Auch während der Vorbereitung und Übertragung großer Dateien sollen Web-App,
Status-API und P2P-Verbindungen ansprechbar bleiben. Gleichzeitig sollen
unnötige Datei-Lesevorgänge und parallele Prüfsummenberechnungen auf dem
Raspberry Pi reduziert werden.

Vorgeschlagener Branch: `fix/p2p-responsiveness`.

## Ausgangslage

- Nach einer WebDAV-Änderung wird vor der P2P-Ankündigung die gesamte Datei
  für CRC32 gelesen (`src/webdav.rs`).
- Bei `FetchFile` liest der Sender die Datei für `FileStart` erneut vollständig
  und wartet darauf innerhalb der P2P-Ereignisschleife (`src/p2p.rs`,
  `src/sync.rs`).
- Der Login der mitgelieferten Web-App prüft `/api/v1/status`. Diese Anfrage
  wartet ihrerseits auf die P2P-Schleife (`src/api.rs`). Eine lange laufende
  Vorbereitung kann daher die Anmeldung verzögern.
- Auch der Manifest-Aufbau kann Dateien lesen und wird von der P2P-Schleife
  abgewartet (`src/db.rs`). Er ist bei der Analyse mit zu berücksichtigen.
- CPU- oder Speicherengpässe auf dem konkreten Pi und die genaue Ursache des
  Heartbeat-Timeouts sind noch nicht durch Messungen bestätigt.

## Umsetzung

1. **Ausgangszustand erfassen**
   - Mit einer großen Datei auf dem betroffenen Pi Zeitpunkt und Dauer von
     Ankündigung, `FetchFile`, Prüfsummenberechnung und erstem Chunk messen.
   - Parallel Login bzw. `/api/v1/status`, Heartbeats, CPU-Auslastung und
     Speicher-I/O beobachten. Logmeldung und Art des Timeouts festhalten.

2. **P2P-Schleife reaktionsfähig halten**
   - Aufwendige Vorbereitung von `FetchFile` außerhalb der P2P-Schleife
     ausführen. Die Schleife muss währenddessen Swarm-Ereignisse und
     Statusanfragen weiter bearbeiten können; Antworten bleiben dem richtigen
     Peer und Anfragekanal zugeordnet.
   - Den Manifest-Pfad auf dieselbe Warteproblematik prüfen und, soweit für
     das Problem erforderlich, ebenfalls entkoppeln. Nicht nur die Arbeit auf
     einen anderen Thread verschieben und anschließend in der Schleife darauf
     warten.
   - Fehler, abgebrochene Anfragen und Peer-Trennungen ausdrücklich behandeln;
     keine erfolgreiche Antwort mit erfundenen Daten erzeugen.

3. **Arbeit begrenzen und Doppelarbeit vermeiden**
   - Prüfsummen-Jobs über die beteiligten Pfade hinweg begrenzen, damit mehrere
     Anfragen oder WebDAV-Änderungen den Pi nicht gleichzeitig mit beliebig
     vielen vollständigen Datei-Reads belasten. Anstehende Arbeit und
     Überlastung nachvollziehbar behandeln.
   - Vorhandene Prüfsummen nur wiederverwenden, wenn sie nachweislich zur
     aktuell übertragenen Dateiversion gehören. Änderungen, Ersetzung,
     Löschung und externe Dateizugriffe berücksichtigen; Größe und Zeitstempel
     allein nicht als Garant für unveränderten Inhalt behandeln.
   - Falls eine sichere Wiederverwendung nicht möglich ist, erneut berechnen,
     ohne die P2P-Schleife zu blockieren. Reihenfolge von Dateiänderungen,
     Ankündigungen und Datenbankeinträgen erhalten.

4. **Kompatibilität und Integrität erhalten**
   - Das bestehende Wire-Protokoll `/nacs-backend/sync/1`, CRC32-Prüfung,
     Chunk-Transfer und Retry-Verhalten beibehalten.
   - Dateiänderungen während Vorbereitung oder Übertragung dürfen nicht dazu
     führen, dass falsche Inhalte als erfolgreich verifiziert veröffentlicht
     werden. Abbruch bzw. erneuter Versuch muss erkennbar sein.
   - Das bestehende, speicherschonende Transferverhalten erhalten.

## Überprüfung

- Automatisierter Test: Eine gezielt angehaltene Datei- oder Manifest-
  Vorbereitung läuft, während Statusanfragen und P2P-Ereignisse weiterhin
  bearbeitet werden. Erst danach die Vorbereitung freigeben und die korrekte
  Antwort prüfen.
- Tests für begrenzte parallele Arbeit, mehrere Anfragen, Fehler und
  Peer-Abbruch sowie für sichere Wiederverwendung und Invalidierung bei
  Dateiänderungen. Ein Prüfsummenfehler darf weiterhin keine Datei freigeben.
- Bestehende Sync-, P2P-, WebDAV- und API-Tests ausführen; zusätzlich
  Formatierung und Clippy wie in CI prüfen.
- Auf dem betroffenen Pi vor und nach dem Fix mit derselben großen Datei und
  Umgebung vergleichen: Login/Status während der Vorbereitung, Heartbeats,
  Zeit bis zum ersten Chunk, CPU- und I/O-Auslastung. Dateigröße, Speichermedium
  und Ergebnisse dokumentieren. Keine Verbesserung als bestätigt ausgeben,
  bevor der Test dort durchgeführt wurde.

## Nicht Teil dieses Fixes

- P2P-Authentifizierung und allgemeine Pfadhärtung.
- Ein neues Sync-Protokoll oder ein Wechsel des Prüfsummenalgorithmus.
- Timeout-Erhöhungen als Ersatz für die Beseitigung der Blockierung.

## Umsetzungsstand

- Datei- und Manifest-Vorbereitung sind von der P2P-Schleife entkoppelt;
  Prüfsummenberechnungen und anstehende P2P-Arbeit sind begrenzt.
- WebDAV-Ereignisse werden über eine begrenzte Warteschlange geordnet
  verarbeitet. Bei unvollständigen Chunks wird die Übertragung erneut versucht.
- Prüfsummen werden bei Datei-Anfragen weiterhin neu berechnet: Für extern
  veränderbare Dateien ist eine sichere Wiederverwendung nicht gewährleistet.
- Automatische Tests und CI-Lints sind erfolgreich. Messungen vor und nach dem
  Fix auf dem betroffenen Raspberry Pi stehen noch aus.
